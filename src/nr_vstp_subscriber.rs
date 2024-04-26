use async_trait::async_trait;
use crate::subscriber::Subscriber;
use crate::error::Error;
use serde::Deserialize;
use tokio::task::JoinHandle;

use tokio_stomp::client;
use tokio_stomp::client::ClientTransport;
use tokio_stomp::FromServer;
use tokio_stomp::ToServer;

use futures::SinkExt;
use futures::StreamExt;
use futures::stream::SplitSink;
use futures::stream::SplitStream;

use tokio::io::AsyncBufRead;
use tokio::io::BufReader;
use tokio::time::Duration;

use std::fmt;
use std::io::Cursor;

pub struct NrVstpSubscriber {
    config: NrVstpSubscriberConfig,
    stream: Option<SplitStream<ClientTransport>>,
    keepalive: Option<JoinHandle<Result<(), Error>>>,
}

#[derive(Deserialize)]
pub struct NrVstpSubscriberConfig {
    username: String,
    password: String,
}

impl NrVstpSubscriber {
    pub fn new(config: NrVstpSubscriberConfig) -> Self {
        Self {
            config,
            stream: None,
            keepalive: None,
        }
    }
}

#[derive(Debug)]
pub struct NrVstpError {
    what: String,
}

impl fmt::Display for NrVstpError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Error reading from VSTP STOMP stream: {}", self.what)
    }
}

async fn keep_alive(mut sink: SplitSink<ClientTransport, tokio_stomp::Message<ToServer>>) -> Result<(), Error> {
    // horrible hacky workaround for tokio_stomp's lack of heartbeat support. I'm truly sorry.
    loop {
        tokio::time::sleep(Duration::from_secs(15)).await;
        sink.send(ToServer::Begin { transaction: "foo".to_string() }.into()).await?;
        tokio::time::sleep(Duration::from_secs(15)).await;
        sink.send(ToServer::Abort { transaction: "foo".to_string() }.into()).await?;
    }
}

#[async_trait]
impl Subscriber for NrVstpSubscriber {
    async fn subscribe(&mut self) -> Result<(), Error> {
        println!("Subscribing to VSTP data from Network Rail");
        let (mut sink, stream) = client::connect(
            "publicdatafeeds.networkrail.co.uk:61618",
            "/".to_string(),
            Some(self.config.username.clone()),
            Some(self.config.password.clone()),
        ).await?.split();
        self.stream = Some(stream);

        sink.send(client::subscribe("/topic/VSTP_ALL", "1")).await?;

        self.keepalive = Some(tokio::spawn(async move {
            return keep_alive(sink).await;
        }));

        Ok(())
    }

    async fn receive(&mut self) -> Result<Box<dyn AsyncBufRead + Unpin + Send>, Error> {
        let msg = match &mut self.stream {
            Some(x) => x.next().await.transpose()?,
            None => return Err(Error::NrVstpError(NrVstpError { what: "Subscribe not yet called".to_string() })),
        };
        println!("Received VSTP data from Network Rail");
        let msg = match msg {
            Some(x) => x,
            None => return Err(Error::NrVstpError(NrVstpError { what: "Received empty message".to_string() })),
        };

        match msg.content {
            FromServer::Message { body, .. } => Ok(Box::new(BufReader::new(Cursor::new(match body {
                Some(x) => x,
                None => return Err(Error::NrVstpError(NrVstpError { what: "No body".to_string() })),
            })))),
            FromServer::Receipt { .. } => Err(Error::NrVstpError(NrVstpError { what: "Received Receipt".to_string() })),
            FromServer::Error { message, .. } => Err(Error::NrVstpError(NrVstpError { what: message.unwrap() })),
            _ => Err(Error::NrVstpError(NrVstpError { what: "Received unknown message".to_string() })),
        }
    }
}
