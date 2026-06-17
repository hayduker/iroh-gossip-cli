use std::{collections::HashMap, fmt, str::FromStr};

use anyhow::Result;
use clap::Parser;
use futures_lite::StreamExt;
use iroh::{protocol::Router, Endpoint, EndpointAddr, EndpointId, endpoint::presets};
use iroh_gossip::{
    api::{GossipReceiver, Event},
    net::Gossip,
    proto::TopicId,
};
use iroh_services::{API_SECRET_ENV_VAR_NAME, ApiSecret, CLIENT_HOST_ALPN, Client, ClientHost, caps::NetDiagnosticsCap};
use serde::{Deserialize, Serialize};

/// Chat over iroh-gossip
///
/// This broadcasts unsigned messages over iroh-gossip.
///
/// By default a new endpoint id is created when starting the example.
///
/// By default, we use the default n0 address lookup services to dial by `EndpointId`.
#[derive(Parser, Debug)]
struct Args {
    /// Set your nickname.
    #[clap(short, long)]
    name: Option<String>,
    /// Set the bind port for our socket. By default, a random port will be used.
    #[clap(short, long, default_value = "0")]
    bind_port: u16,
    #[clap(subcommand)]
    command: Command,
}

#[derive(Parser, Debug)]
enum Command {
    /// Open a chat room for a topic and print a ticket for others to join.
    Open,
    /// Join a chat room from a ticket.
    Join {
        /// The ticket, as base32 string.
        ticket: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // parse the cli command
    let (topic, endpoints) = match &args.command {
        Command::Open => {
            let topic = TopicId::from_bytes(rand::random());
            println!("> opening chat room for topic {topic}");
            (topic, vec![])
        }
        Command::Join { ticket } => {
            let Ticket { topic, endpoints } = Ticket::from_str(ticket)?;
            println!("> joining chat room for topic {topic}");
            (topic, endpoints)
        }
    };

    let endpoint = Endpoint::bind(presets::N0).await?;

    let secret = ApiSecret::from_env_var(API_SECRET_ENV_VAR_NAME)?;
    let remote_id = secret.addr().id;

    let client = Client::builder(&endpoint)
        .api_secret_from_env()?
        .name("gossip-chat-endpoint")?
        .build()
        .await?;

    let client2 = client.clone();
    tokio::spawn(async move {
        client2
            .grant_capability(remote_id, vec![NetDiagnosticsCap::GetAny])
            .await
            .unwrap();
    });

    let host = ClientHost::new(&endpoint);

    println!("> our endpoint id: {}", endpoint.id());
    let gossip = Gossip::builder().spawn(endpoint.clone());

    let router = Router::builder(endpoint.clone())
        .accept(CLIENT_HOST_ALPN, host)
        .accept(iroh_gossip::ALPN, gossip.clone())
        .spawn();

    // in our main file, after we create a topic `id`:
    // print a ticket that includes our own endpoint id and endpoint addresses
    let ticket = {
        // Get our address information, includes our
        // `EndpointId`, our `RelayUrl`, and any direct
        // addresses.
        let me = endpoint.addr();
        let endpoints = vec![me];
        Ticket { topic, endpoints }
    };
    println!("> ticket to join us: {ticket}");

    // join the gossip topic by connecting to known endpoints, if any
    let endpoint_ids = endpoints.iter().map(|p| p.id).collect();
    if endpoints.is_empty() {
        println!("> waiting for endpoints to join us...");
    } else {
        println!("> trying to connect to {} endpoints...", endpoints.len());
    };
    let (sender, receiver) = gossip.subscribe_and_join(topic, endpoint_ids).await?.split();
    println!("> connected!");

    // broadcast our name, if set
    if let Some(name) = args.name {
        let message = Message::new(MessageBody::AboutMe {
            from: endpoint.id(),
            name,
        });
        sender.broadcast(message.to_vec().into()).await?;
    }

    // subscribe and print loop
    tokio::spawn(subscribe_loop(receiver));

    // spawn an input thread that reads stdin
    // create a multi-provider, single-consumer channel
    let (line_tx, mut line_rx) = tokio::sync::mpsc::channel(1);
    // and pass the `sender` portion to the `input_loop`
    std::thread::spawn(move || input_loop(line_tx));

    // broadcast each line we type
    println!("> type a message and hit enter to broadcast...");
    // listen for lines that we have typed to be sent from `stdin`
    while let Some(text) = line_rx.recv().await {
        // create a message from the text
        let message = Message::new(MessageBody::Message {
            from: endpoint.id(),
            text: text.clone(),
        });
        // broadcast the encoded message
        sender.broadcast(message.to_vec().into()).await?;
        // print to ourselves the text that we sent
        println!("> sent: {text}");
    }
    router.shutdown().await?;

    Ok(())
}

#[derive(Debug, Serialize, Deserialize)]
struct Message {
    body: MessageBody,
    nonce: [u8; 16],
}

#[derive(Debug, Serialize, Deserialize)]
enum MessageBody {
    AboutMe { from: EndpointId, name: String },
    Message { from: EndpointId, text: String },
}

impl Message {
    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        serde_json::from_slice(bytes).map_err(Into::into)
    }

    pub fn new(body: MessageBody) -> Self {
        Self {
            body,
            nonce: rand::random(),
        }
    }

    pub fn to_vec(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("serde_json::to_vec is infallible")
    }
}

// Handle incoming events
async fn subscribe_loop(mut receiver: GossipReceiver) -> Result<()> {
    // keep track of the mapping between `EndpointId`s and names
    let mut names = HashMap::new();
    // iterate over all events
    while let Some(event) = receiver.try_next().await? {
        // if the Event is a `GossipEvent::Received`, let's deserialize the message:
        if let Event::Received(msg) = event {
            // deserialize the message and match on the
            // message type:
            match Message::from_bytes(&msg.content)?.body {
                MessageBody::AboutMe { from, name } => {
                    // if it's an `AboutMe` message
                    // add an entry into the map
                    // and print the name
                    names.insert(from, name.clone());
                    println!("> {} is now known as {}", from.fmt_short(), name);
                }
                MessageBody::Message { from, text } => {
                    // if it's a `Message` message,
                    // get the name from the map
                    // and print the message
                    let name = names
                        .get(&from)
                        .map_or_else(|| from.fmt_short().to_string(), String::to_string);
                    println!("{}: {}", name, text);
                }
            }
        }
    }
    Ok(())
}

fn input_loop(line_tx: tokio::sync::mpsc::Sender<String>) -> Result<()> {
    let mut buffer = String::new();
    let stdin = std::io::stdin(); // We get `Stdin` here.
    loop {
        stdin.read_line(&mut buffer)?;
        line_tx.blocking_send(buffer.clone())?;
        buffer.clear();
    }
}

// add the `Ticket` code to the bottom of the main file
#[derive(Debug, Serialize, Deserialize)]
struct Ticket {
    topic: TopicId,
    endpoints: Vec<EndpointAddr>,
}

impl Ticket {
    /// Deserialize from a slice of bytes to a Ticket.
    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        serde_json::from_slice(bytes).map_err(Into::into)
    }

    /// Serialize from a `Ticket` to a `Vec` of bytes.
    pub fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("serde_json::to_vec is infallible")
    }
}

// The `Display` trait allows us to use the `to_string`
// method on `Ticket`.
impl fmt::Display for Ticket {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut text = data_encoding::BASE32_NOPAD.encode(&self.to_bytes()[..]);
        text.make_ascii_lowercase();
        write!(f, "{}", text)
    }
}

// The `FromStr` trait allows us to turn a `str` into
// a `Ticket`
impl FromStr for Ticket {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let bytes = data_encoding::BASE32_NOPAD.decode(s.to_ascii_uppercase().as_bytes())?;
        Self::from_bytes(&bytes)
    }
}