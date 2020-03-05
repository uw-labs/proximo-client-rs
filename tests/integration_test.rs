extern crate proximo_client;

use proximo_client::proximo::Message;
use proximo_client::{ProximoError, Sink};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time;

#[tokio::test]
async fn test_all() {
    do_test_all().await.unwrap();
}

async fn do_test_all() -> Result<(), Box<dyn std::error::Error>> {
    let nats = Command::new("nats-streaming-server")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()?;

    let _nats = DropKiller { child: nats };

    let proximo = Command::new("proximo-server")
        .args(&["nats-streaming"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()?;

    let _proximo = DropKiller { child: proximo };

    thread::sleep(time::Duration::from_millis(1000));

    let plumber = Command::new("plumber")
        .args(&[
            "consume-all-raw",
            "--sourceurl",
            "nats-streaming://localhost:4222/topic1?cluster-id=test-cluster&queue-group=1",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()?;

    let _plumber = DropKiller { child: plumber };

    do_publishes().await?;

    Ok(())
}

async fn do_publishes() -> Result<(), ProximoError> {
    let mut s = Sink::new("http://localhost:6868", "topic1").await?;

    for id in 1..10 {
        let id = id.to_string();

        let data = format!("Hello from message id {}", id).into_bytes();

        let m = Message { id, data };

        s.send_message(m).await?;
    }

    Ok(())
}

/// DropKiller tracks child processes and kills them when dropped.
struct DropKiller {
    child: Child,
}

impl std::ops::Drop for DropKiller {
    fn drop(&mut self) {
        self.child.kill().unwrap();
    }
}
