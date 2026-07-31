use std::time::Duration;

use anyhow::Result;
use serde::{Deserialize, Serialize};
#[allow(unused_imports)]
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::mpsc;

use genie_common::mode::Mode;

const SOCKET_PATH: &str = "/run/geniepod/governor.sock";
/// Owner/group read-write only — genie-core and genie-ctl run as root on device.
const SOCKET_MODE: u32 = 0o660;
const RUN_DIR_MODE: u32 = 0o750;
/// Idle-read deadline for one control-socket line. Size is already capped
/// (`MAX_CONTROL_LINE_BYTES`); without a time bound a connect-and-stall peer
/// parks `fill_buf` forever and leaks the per-connection task + fd.
const CONTROL_READ_TIMEOUT: Duration = Duration::from_secs(30);

/// Commands that external processes (genie-core, CLI) can send to the governor.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "cmd", rename_all = "snake_case")]
pub enum Command {
    /// Switch to a specific mode.
    SetMode { mode: Mode },
    /// Enter media mode (genie-core sends this when "play Inception" is triggered).
    MediaStart,
    /// Exit media mode (genie-core sends this on "stop playing").
    MediaStop,
    /// Query current state.
    Status,
}

/// Response sent back to the caller.
#[derive(Debug, Serialize)]
pub struct StatusResponse {
    pub mode: Mode,
    /// `None` (JSON `null`) when unknown, so a consumer can tell it from a
    /// genuine zero — `system.rs` falls back to its own read on `null`.
    pub mem_available_mb: Option<u64>,
    pub uptime_secs: u64,
}

/// Spawn the Unix socket listener. Returns a receiver of commands.
pub async fn spawn_listener() -> Result<mpsc::Receiver<(Command, ResponseSender)>> {
    // Clean up stale socket.
    let _ = tokio::fs::remove_file(SOCKET_PATH).await;
    tokio::fs::create_dir_all("/run/geniepod").await?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(
            "/run/geniepod",
            std::fs::Permissions::from_mode(RUN_DIR_MODE),
        )?;
    }

    let listener = UnixListener::bind(SOCKET_PATH)?;

    // Restrict socket to owner/group — blocks unprivileged local users from
    // sending mode commands while root-owned services (genie-core, genie-ctl) retain access.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(SOCKET_PATH, std::fs::Permissions::from_mode(SOCKET_MODE))?;
    }

    let (tx, rx) = mpsc::channel::<(Command, ResponseSender)>(16);

    tokio::spawn(async move {
        tracing::info!(path = SOCKET_PATH, "control socket listening");
        loop {
            match listener.accept().await {
                Ok((stream, _)) => {
                    let tx = tx.clone();
                    tokio::spawn(handle_connection(stream, tx));
                }
                Err(e) => {
                    tracing::warn!(error = %e, "accept failed");
                }
            }
        }
    });

    Ok(rx)
}

/// Channel for sending a JSON response back to the client.
pub type ResponseSender = tokio::sync::oneshot::Sender<String>;

const MAX_CONTROL_LINE_BYTES: usize = 16 * 1024;

async fn read_control_line<R>(reader: &mut R) -> Result<Option<String>, String>
where
    R: tokio::io::AsyncBufRead + Unpin,
{
    use tokio::io::AsyncBufReadExt;

    let mut out = Vec::new();
    loop {
        let available = reader.fill_buf().await.map_err(|e| e.to_string())?;
        if available.is_empty() {
            return if out.is_empty() {
                Ok(None)
            } else {
                Ok(Some(String::from_utf8_lossy(&out).into_owned()))
            };
        }
        if let Some(idx) = available.iter().position(|&b| b == b'\n') {
            let take = idx + 1;
            if out.len() + take > MAX_CONTROL_LINE_BYTES {
                return Err(format!(
                    "control command exceeds {} bytes",
                    MAX_CONTROL_LINE_BYTES
                ));
            }
            out.extend_from_slice(&available[..take]);
            reader.consume(take);
            let mut line = String::from_utf8_lossy(&out).into_owned();
            if line.ends_with('\n') {
                line.pop();
                if line.ends_with('\r') {
                    line.pop();
                }
            }
            return Ok(Some(line));
        }
        let take = available.len();
        if out.len() + take > MAX_CONTROL_LINE_BYTES {
            return Err(format!(
                "control command exceeds {} bytes",
                MAX_CONTROL_LINE_BYTES
            ));
        }
        out.extend_from_slice(available);
        reader.consume(take);
    }
}

async fn handle_connection(stream: UnixStream, tx: mpsc::Sender<(Command, ResponseSender)>) {
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);

    loop {
        let line = match tokio::time::timeout(CONTROL_READ_TIMEOUT, read_control_line(&mut reader))
            .await
        {
            Ok(Ok(Some(line))) => line,
            Ok(Ok(None)) => break,
            Ok(Err(e)) => {
                let err = serde_json::json!({"error": e});
                let mut msg = err.to_string();
                msg.push('\n');
                let _ = writer.write_all(msg.as_bytes()).await;
                break;
            }
            Err(_elapsed) => {
                tracing::warn!(
                    timeout = ?CONTROL_READ_TIMEOUT,
                    "control socket idle read timed out; closing connection"
                );
                break;
            }
        };
        let line = line.trim().to_string();
        if line.is_empty() {
            continue;
        }

        match serde_json::from_str::<Command>(&line) {
            Ok(cmd) => {
                let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();
                if tx.send((cmd, resp_tx)).await.is_err() {
                    break;
                }
                // Wait for the governor to process and respond.
                if let Ok(response) = resp_rx.await {
                    let mut msg = response;
                    msg.push('\n');
                    let _ = writer.write_all(msg.as_bytes()).await;
                }
            }
            Err(e) => {
                let err = serde_json::json!({"error": e.to_string()});
                let mut msg = err.to_string();
                msg.push('\n');
                let _ = writer.write_all(msg.as_bytes()).await;
            }
        }
    }
}

/// CLI helper: send a command to the governor and print the response.
/// Usage: `echo '{"cmd":"status"}' | socat - UNIX-CONNECT:/run/geniepod/governor.sock`
/// Or from Rust: `control::send_command(&cmd).await`
#[allow(dead_code)]
pub async fn send_command(cmd: &Command) -> Result<String> {
    let stream = UnixStream::connect(SOCKET_PATH).await?;
    let (reader, mut writer) = stream.into_split();

    let json = serde_json::to_string(cmd)?;
    writer.write_all(json.as_bytes()).await?;
    writer.write_all(b"\n").await?;

    let mut lines = BufReader::new(reader).lines();
    let response = lines.next_line().await?.unwrap_or_else(|| "{}".to_string());
    Ok(response)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncWriteExt, BufReader};
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn read_control_line_rejects_oversized_input() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let payload = "x".repeat(MAX_CONTROL_LINE_BYTES + 1);
            let _ = stream.write_all(payload.as_bytes()).await;
            let _ = stream.write_all(b"\n").await;
        });

        let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        let mut reader = BufReader::new(stream);
        let error = read_control_line(&mut reader).await.unwrap_err();
        server.abort();

        assert!(
            error.contains(&format!(
                "control command exceeds {MAX_CONTROL_LINE_BYTES} bytes"
            )),
            "expected cap error, got: {error}"
        );
    }

    #[tokio::test]
    async fn control_read_timeout_drops_a_stalled_peer() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        // Accept but never write a newline — previously `fill_buf` hung forever.
        let server = tokio::spawn(async move {
            let (_stream, _) = listener.accept().await.unwrap();
            tokio::time::sleep(Duration::from_secs(60)).await;
        });

        let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        let mut reader = BufReader::new(stream);
        let short = Duration::from_millis(200);
        let started = std::time::Instant::now();
        let result = tokio::time::timeout(short, read_control_line(&mut reader)).await;
        let elapsed = started.elapsed();
        server.abort();

        assert!(
            result.is_err(),
            "stalled peer must hit the idle-read deadline, not complete a line"
        );
        assert!(
            elapsed < Duration::from_secs(2),
            "must fail on the timeout, not wait for the peer: took {elapsed:?}"
        );
    }
}
