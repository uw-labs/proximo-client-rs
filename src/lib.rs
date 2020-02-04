pub mod proximo {
    tonic::include_proto!("proximo");
}

use futures::executor::block_on;
use std::error::Error;
use tonic::transport::Channel;
use tonic::Request;

use proximo::message_sink_client::MessageSinkClient;
//use proximo::{Confirmation, Message, PublisherRequest, StartPublishRequest};
use proximo::{Message, PublisherRequest, StartPublishRequest};
use tokio::sync::mpsc;

pub struct Sink {
    client: MessageSinkClient<Channel>,
    topic: String,
}

impl Sink {
    pub fn new(
        url: String,
        topic: String,
    ) -> Result<Sink, Box<dyn std::error::Error>> {
        let client = block_on(async { MessageSinkClient::connect(url).await })?;

        return Ok(Sink {
            client: client,
            topic: topic,
        });
    }

    pub async fn publish_messages(
        &mut self,
        cancel: mpsc::Receiver<()>,
        acks: mpsc::Sender<Message>,
        messages: mpsc::Receiver<Message>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        return do_pub_messages(
            &mut self.client,
            cancel,
            acks,
            messages,
            self.topic.to_string(),
        )
        .await;
    }
}

async fn do_pub_messages(
    client: &mut MessageSinkClient<Channel>,
    _cancel: mpsc::Receiver<()>,
    _acks: mpsc::Sender<Message>,
    mut messages: mpsc::Receiver<Message>,
    topic: String,
) -> Result<(), Box<dyn Error>> {
    let outbound = async_stream::stream! {
        let req = PublisherRequest{
            msg: None,
            start_request: Some(StartPublishRequest{topic: topic.to_string()}),
        };
        yield req;

        loop {
            // TODO: deal with cancellation.
            match messages.recv().await {
                None => {
                    panic!("when does this happen?")
                }
                Some(x) => {
                    let req = PublisherRequest {
                            msg: Some(x),
                            start_request:None,
                    };

                    yield req;
                }
            }
        }
    };

    let response = client.publish(Request::new(outbound)).await?;

    let mut inbound = response.into_inner();

    while let Some(m) = inbound.message().await? {
        println!("NOTE = {:?}", m);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
