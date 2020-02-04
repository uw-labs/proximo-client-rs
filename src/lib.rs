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

        Ok(Sink { client, topic })
    }

    pub async fn publish_messages(
        &mut self,
        _cancel: mpsc::Receiver<()>, // TODO: deal with cancellation
        _acks: mpsc::Sender<Message>,
        mut messages: mpsc::Receiver<Message>,
    ) -> Result<(), Box<dyn std::error::Error>> {

        let topic = self.topic.to_string();

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

        let response = self.client.publish(Request::new(outbound)).await?;

        let mut inbound = response.into_inner();

        while let Some(m) = inbound.message().await? {
            println!("NOTE = {:?}", m);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
