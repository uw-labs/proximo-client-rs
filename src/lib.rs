pub mod proximo {
    tonic::include_proto!("proximo");
}

use proximo::message_sink_client::MessageSinkClient;
use proximo::{Message, PublisherRequest, StartPublishRequest};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tonic::Request;

pub struct Sink {
    //    client: MessageSinkClient<Channel>,
    reqs: mpsc::Sender<MessageRequest>,
}

struct MessageRequest {
    m: Message,
    done: mpsc::Sender<Result<(), ProximoError>>,
}

impl Sink {
    pub async fn new(url: &str, topic: &str) -> Result<Sink, ProximoError> {
        let mut client = MessageSinkClient::connect(url.to_string()).await?;

        let (mut toack_tx, mut toack_rx) = mpsc::channel(16);
        let (reqs_tx, mut reqs_rx): (
            mpsc::Sender<MessageRequest>,
            mpsc::Receiver<MessageRequest>,
        ) = mpsc::channel(16);

        let topic1 = topic.to_owned();

        let outbound = async_stream::stream! {

            yield PublisherRequest{msg: None, start_request: Some(StartPublishRequest{topic:topic1})};

            // This loop exits when None is recieved, indicating that the sender end has been
            // dropped.
            while let Some(req) = reqs_rx.recv().await {
                let pubReq = PublisherRequest {
                        msg: Some(req.m),
                        start_request:None,
                };
                match toack_tx.send(req.done).await {
                    Ok(()) => {
                        yield pubReq;
                    }
                    Err(e) => {
                        // this indicates the recieving handle has closed, so we have nothing left to do.
                        break;
                    }
                }
            }

        };

        let _jh: JoinHandle<Result<(), ProximoError>> = tokio::spawn(
            async move {
                let response = client.publish(Request::new(outbound)).await?;

                let mut inbound = response.into_inner();

                loop {
                    match inbound.message().await? {
                        None => panic!(
                        "empty message from proximo server.  when does this happen?"
                    ),
                        Some(_conf) => match toack_rx.recv().await {
                            None => {
                                // This indicates the sending end is dropped, so we have nothing else to do.
                                return Ok(());
                            }
                            Some(mut to_ack) => {
                                /*
                                TODO: check the id is the one expected?
                                if to_ack.id != conf.msg_id {
                                    return Err(Box::new(ProximoError::new(
                                        "unexpected ack order",
                                    )));
                                }
                                */
                                to_ack.send(Ok(())).await?
                            }
                        },
                    }
                }
            },
        );

        Ok(Sink {
            // client,
            reqs: reqs_tx,
        })
    }

    pub async fn send_message(
        &mut self,
        m: Message,
    ) -> Result<(), ProximoError> {
        let (done_tx, mut done_rx) =
            mpsc::channel::<Result<(), ProximoError>>(16);

        let req = MessageRequest { m, done: done_tx };

        self.reqs.send(req).await?;

        match done_rx.recv().await {
            None => {
                panic!("no recv. what?");
            }
            Some(recv_res) => Ok(recv_res?),
        }
    }
}

use std::fmt;

#[derive(Debug)]
pub struct ProximoError {
    err: String,
}

impl std::error::Error for ProximoError {}

impl fmt::Display for ProximoError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Oh no, something bad went down")
    }
}

impl From<tonic::Status> for ProximoError {
    fn from(err: tonic::Status) -> ProximoError {
        ProximoError { err: format!("{}", err) }
    }
}

impl From<tonic::transport::Error> for ProximoError {
    fn from(err: tonic::transport::Error) -> ProximoError {
        ProximoError { err: format!("{}", err) }
    }
}

impl From<mpsc::error::SendError<MessageRequest>> for ProximoError {
    fn from(err: mpsc::error::SendError<MessageRequest>) -> ProximoError {
        ProximoError { err: format!("{}", err) }
    }
}

impl From<mpsc::error::SendError<Result<(), ProximoError>>> for ProximoError {
    fn from(
        err: mpsc::error::SendError<Result<(), ProximoError>>,
    ) -> ProximoError {
        ProximoError { err: format!("{}", err) }
    }
}
