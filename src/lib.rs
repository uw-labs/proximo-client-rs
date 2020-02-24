pub mod proximo {
    tonic::include_proto!("proximo");
}

use proximo::message_sink_client::MessageSinkClient;
use proximo::{Message, PublisherRequest, StartPublishRequest};
use tokio::sync::mpsc;
use tonic::transport::Channel;
use tonic::Request;

pub struct Sink {
    client: MessageSinkClient<Channel>,
    topic: String,
}

impl Sink {
    pub async fn new(
        url: &str,
        topic: &str,
    ) -> Result<Sink, Box<dyn std::error::Error>> {
        let client = MessageSinkClient::connect(url.to_string()).await?;

        Ok(Sink {
            client,
            topic: topic.to_string(),
        })
    }

    pub async fn publish_messages(
        &mut self,
        mut cancel: mpsc::Receiver<()>,
        mut messages: mpsc::Receiver<Message>,
        mut acks: mpsc::Sender<Message>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let topic = self.topic.to_string();

        let (mut toack_tx, mut toack_rx) = mpsc::channel(16);

        let outbound = async_stream::stream! {
            let req = PublisherRequest{
                msg: None,
                start_request: Some(StartPublishRequest{topic}),
            };
            yield req;

            loop {
                // TODO: deal with cancellation.
                match messages.recv().await {
                    None => {
                        panic!("empty message : when does this happen?")
                    }
                    Some(msg) => {
                        let req = PublisherRequest {
                                msg: Some(msg.clone()),
                                start_request:None,
                        };

                        match toack_tx.send(msg).await {
                            Ok(()) => {
                                // do nothing
                            }
                            Err(e) => {
                                panic!("handle this properly")
                            }
                        }

                        yield req;
                    }
                }
            }
        };

        let response = self.client.publish(Request::new(outbound)).await?;

        let mut inbound = response.into_inner();

        loop {
            tokio::select! {

                c = cancel.recv() => {
                    match c {
                        None => panic!(
                            "empty cancel.  when does this happen?"
                        ),
                        Some(()) => {
                            // cancelled, so return
                            println!("cancel recv'd - exiting");
                            return Ok(());
                        }
                    }
                }

                m = inbound.message() => {
                    match m? {
                        None => panic!(
                            "empty message from proximo.  when does this happen?"
                        ),
                        Some(conf) => match toack_rx.recv().await {
                            None => panic!("when does this happen?"),
                            Some(to_ack) => {
                                if to_ack.id != conf.msg_id {
                                    return Err(Box::new(ProximoError::new(
                                        "unexpected ack order",
                                    )));
                                }
                                acks.send(to_ack).await?;
                            }
                        },

                    }
                }
            }
        }
    }
}

use std::fmt;

#[derive(Debug)]
struct ProximoError {
    err: String,
}

impl std::error::Error for ProximoError {}

impl ProximoError {
    fn new(s: &str) -> Self {
        ProximoError { err: s.to_string() }
    }
}

impl fmt::Display for ProximoError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Oh no, something bad went down")
    }
}
