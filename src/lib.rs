pub mod proximo {
    tonic::include_proto!("proximo");
}

use std::fmt;

use futures::{channel::mpsc, SinkExt, StreamExt};
use proximo::message_sink_client::MessageSinkClient;
use proximo::{Message, PublisherRequest, StartPublishRequest};
use tokio::task::JoinHandle;
use tonic::Request;

pub struct Sink {
    //    client: MessageSinkClient<Channel>,
    reqs: mpsc::Sender<MessageRequest>,
}

struct MessageRequest {
    m: Message,
    done: mpsc::Sender<Result<(), Error>>,
}

impl Sink {
    pub async fn new(url: &str, topic: &str) -> Result<Sink, Error> {
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
            while let Some(req) = reqs_rx.next().await {
                let pub_req = PublisherRequest {
                        msg: Some(req.m),
                        start_request:None,
                };
                match toack_tx.send(req.done).await {
                    Ok(()) => {
                        yield pub_req;
                    }
                    Err(_e) => {
                        // this indicates the recieving handle has closed, so we have nothing left to do.
                        break;
                    }
                }
            }

        };

        let _jh: JoinHandle<Result<(), Error>> = tokio::spawn(async move {
            let response = client.publish(Request::new(outbound)).await?;

            let mut inbound = response.into_inner();

            loop {
                match inbound.message().await? {
                        None => panic!(
                        "empty message from proximo server.  when does this happen?"
                    ),
                        Some(_conf) => match toack_rx.next().await {
                            None => {
                                // This indicates the sending end is dropped, so we have nothing else to do.
                                return Ok(());
                            }
                            Some(mut to_ack) => {
                                /*
                                TODO: check the id is the one expected?
                                if to_ack.id != conf.msg_id {
                                    return Err(Box::new(Error::new(
                                        "unexpected ack order",
                                    )));
                                }
                                */
                                to_ack.send(Ok(())).await?
                            }
                        },
                    }
            }
        });

        Ok(Sink {
            // client,
            reqs: reqs_tx,
        })
    }

    pub async fn send_message(&mut self, m: Message) -> Result<AckResponse, Error> {
        let (done_tx, done_rx) = mpsc::channel::<Result<(), Error>>(16);

        let req = MessageRequest { m, done: done_tx };

        self.reqs.send(req).await?;

        Ok(AckResponse{res:done_rx})
    }
}

pub struct AckResponse {
    res : mpsc::Receiver<Result<(), Error>>,
}

impl AckResponse {
   pub async fn response(&mut self ) -> Result<(), Error> {
        match self.res.next().await {
            Some(res) => {
                res
            },
            None => {
                Err(Error::Ack("closed before ack".to_string()))
            }
        }
    }
}

#[derive(Debug)]
pub enum Error {
    TonicStatus(tonic::Status),
    TonicTransport(tonic::transport::Error),
    Send(mpsc::SendError),
    Ack(String),
}

impl std::error::Error for Error {}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::TonicStatus(err) => err.fmt(f),
            Error::TonicTransport( err) => err.fmt(f),
            Error::Send( err) => err.fmt(f),
            Error::Ack( err) => err.fmt(f),
        }
    }
}

impl From<tonic::Status> for Error {
    fn from(err: tonic::Status) -> Self {
        Self::TonicStatus(err)
    }
}

impl From<tonic::transport::Error> for Error {
    fn from(err: tonic::transport::Error) -> Self {
        Self::TonicTransport(err)
    }
}

impl From<mpsc::SendError> for Error {
    fn from(err: mpsc::SendError) -> Self {
        Self::Send(err)
    }
}
