use crate::error::Error;
use crate::subscriber::Subscriber;
use async_trait::async_trait;
use serde::Deserialize;

use async_stomp::FromServer;
use async_stomp::client::{ClientTransport, Connector, Subscriber as StompSubscriber};

use futures::SinkExt;
use futures::StreamExt;

use std::fmt;

pub struct NrVstpSubscriber {
    config: NrVstpSubscriberConfig,
    stream: Option<ClientTransport>,
}

#[derive(Clone, Deserialize)]
pub struct NrVstpSubscriberConfig {
    username: String,
    password: String,
}

impl NrVstpSubscriber {
    pub fn new(config: NrVstpSubscriberConfig) -> Self {
        Self {
            config,
            stream: None,
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

#[async_trait]
impl Subscriber for NrVstpSubscriber {
    async fn subscribe(&mut self) -> Result<(), Error> {
        println!("[gbnr] Subscribing to VSTP data from Network Rail");
        let mut connection = Connector::builder()
            .server("publicdatafeeds.networkrail.co.uk:61618")
            .virtualhost("/")
            .login(self.config.username.clone())
            .passcode(self.config.password.clone())
            .heartbeat(15_000, 60_000)
            .connect()
            .await?;

        let subscriber = StompSubscriber::builder()
            .destination("/topic/VSTP_ALL")
            .id("1")
            .subscribe();

        connection.send(subscriber).await?;

        self.stream = Some(connection);

        Ok(())
    }

    async fn receive(&mut self) -> Result<Vec<u8>, Error> {
        let msg = match &mut self.stream {
            Some(x) => x.next().await.transpose()?,
            None => {
                return Err(Error::NrVstpError(NrVstpError {
                    what: "Subscribe not yet called".to_string(),
                }))
            }
        };
        println!("[gbnr] Received VSTP data from Network Rail");
        let msg = match msg {
            Some(x) => x,
            None => {
                return Err(Error::NrVstpError(NrVstpError {
                    what: "Received empty message".to_string(),
                }))
            }
        };

        match msg.content {
            FromServer::Message { body, .. } => Ok(match body {
                Some(x) => x,
                None => {
                    return Err(Error::NrVstpError(NrVstpError {
                        what: "No body".to_string(),
                    }))
                }
            }),
            FromServer::Receipt { .. } => Err(Error::NrVstpError(NrVstpError {
                what: "Received Receipt".to_string(),
            })),
            FromServer::Error { message, .. } => Err(Error::NrVstpError(NrVstpError {
                what: message.unwrap(),
            })),
            _ => Err(Error::NrVstpError(NrVstpError {
                what: "Received unknown message".to_string(),
            })),
        }
    }
}
