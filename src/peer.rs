use crate::{Rx, Tx};
use rand::random;
use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, stdin};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio::sync::mpsc;

pub struct Peer {
    v: Arc<Mutex<f64>>,
    id: String,
    address: String,
    sender: Tx,
    receiver: Arc<Mutex<Rx>>,
    peers: Arc<Mutex<HashMap<String, String>>>,
}

impl Peer {
    pub fn new(id: &str, address: &str) -> Self {
        let (sender, receiver) = mpsc::unbounded_channel();
        Self {
            v: Arc::new(Mutex::new(random::<f64>())),
            id: id.to_string(),
            address: address.to_string(),
            sender,
            receiver: Arc::new(Mutex::new(receiver)),
            peers: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn start(&self) -> Result<(), Box<dyn Error>> {
        let listener = TcpListener::bind(&self.address).await?;
        let peers_ = self.peers.clone();
        let id_ = self.id.clone();
        let sender_ = self.sender.clone();
        let v_ = self.v.clone();
        tokio::spawn(async move {
            let _ = Self::accept_conns(listener, peers_, id_, sender_, v_).await;
        });

        let v_ = self.v.clone();
        let peers_ = self.peers.clone();
        let id_ = self.id.clone();
        let addr_ = self.address.clone();
        tokio::spawn(async move {
            Self::anti_entropy_loop(v_, peers_, &id_, &addr_).await;
        });

        self.handle_communications().await?;
        Ok(())
    }

    async fn anti_entropy_loop(
        v: Arc<Mutex<f64>>,
        peers: Arc<Mutex<HashMap<String, String>>>,
        id: &str,
        _addr: &str,
    ) {
        println!("mock for anti_entropy_loop");
    }

    pub async fn accept_conns(
        listener: TcpListener,
        peers: Arc<Mutex<HashMap<String, String>>>,
        _id: String,
        sender: Tx,
        v: Arc<Mutex<f64>>,
    ) -> Result<(), Box<dyn Error>> {
        loop {
            match listener.accept().await {
                Ok((socket, addr)) => {
                    println!("incoming connection from {}", addr);
                    let peers_ = peers.clone();
                    let sender_ = sender.clone();
                    let v_ = v.clone();
                    tokio::spawn(async move {
                        if let Err(e) = Self::handle_peer(socket, peers_, v_, sender_).await {
                            eprintln!("peer handler error: {}", e);
                        }
                    });
                }
                Err(e) => eprintln!("Failed to accept connection: {}", e),
            }
        }
    }

    async fn handle_peer(
        socket: TcpStream,
        peers: Arc<Mutex<HashMap<String, String>>>,
        v: Arc<Mutex<f64>>,
        sender: Tx,
    ) -> Result<(), Box<dyn Error>> {
        let (read_half, mut writer) = socket.into_split();
        let mut reader = BufReader::new(read_half);
        let mut line = String::new();

        loop {
            line.clear();
            let n = reader.read_line(&mut line).await?;
            if n == 0 {
                break;
            }
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }

            let mut parts = trimmed.split('/');
            let Some(cmd) = parts.next() else {
                continue;
            };

            match cmd {
                "LINK" => {
                    if let (Some(address), Some(id)) = (parts.next(), parts.next()) {
                        peers
                            .lock()
                            .await
                            .insert(id.to_string(), address.to_string());
                        let _ = sender.send(format!("Linked: {} at {}", id, address));
                        let _ = writer.write_all(b"LINK/ACK\n").await;
                    }
                }
                "SYNC" => {
                    if let (Some(peer_id), Some(v_str)) = (parts.next(), parts.next()) {
                        if let Ok(peer_v) = v_str.parse::<f64>() {
                            let mut current_v = v.lock().await;
                            *current_v = (*current_v + peer_v) / 2.0;
                            let _ = writer
                                .write_all(format!("SYNC/ACK/{:.12}\n", *current_v).as_bytes())
                                .await;
                            let _ = sender
                                .send(format!("Synced with {} -> v={:.6}", peer_id, *current_v));
                        }
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }

    async fn handle_communications(&self) -> Result<(), Box<dyn Error>> {
        let mut stdin = BufReader::new(stdin()).lines();
        let receiver = self.receiver.clone();

        loop {
            tokio::select! {
                msg = async {
                    let mut rx = receiver.lock().await;
                    rx.recv().await
                } => {
                    if let Some(msg) = msg { println!("{}", msg); }
                }
                line = stdin.next_line() => {
                    let Ok(Some(line)) = line else { continue; };
                    let input = line.trim();
                    if input.is_empty() { continue; }

                    match input {
                        "net" => {
                            let p_list = self.peers.lock().await;
                            println!("{} connected to {:?}", self.id, p_list.keys().cloned().collect::<Vec<_>>());
                        }
                        "value" => {
                            let v = *self.v.lock().await;
                            println!("current value: {}", v);
                        }
                        "sync" => {
                            let snapshot = { let p = self.peers.lock().await; p.clone() };
                            let my_v = *self.v.lock().await;
                            for (peer_id, addr) in snapshot {
                                if let Err(e) = Self::sync_with_peer(&addr, &self.id, my_v, self.v.clone()).await {
                                    eprintln!("sync to {}@{} failed: {}", peer_id, addr, e);
                                }
                            }
                        }
                        _ if input.starts_with("LINK/") => {
                            //LINK/{address}/{id}
                            let mut it = input.split('/');
                            let _ = it.next();
                            if let (Some(address), Some(id)) = (it.next(), it.next()) {
                                println!("linking with {} at {}", id, address);

                                let addr_for_spawn = address.to_string();
                                let self_addr = self.address.clone();
                                let self_id = self.id.clone();
                                let peers = self.peers.clone();
                                let id_ = id.to_string();
                                let address_ = address.to_string();

                                tokio::spawn(async move {
                                    match TcpStream::connect(&addr_for_spawn).await {
                                        Ok(mut stream) => {
                                            let line = format!("LINK/{}/{}\n", self_addr, self_id);
                                            if stream.write_all(line.as_bytes()).await.is_ok() && stream.flush().await.is_ok() {
                                                let (r, _) = stream.into_split();
                                                let mut r = BufReader::new(r);
                                                let mut tmp = String::new();
                                                if r.read_line(&mut tmp).await.is_ok() && tmp.trim() == "LINK/ACK" {
                                                    peers.lock().await.insert(id_.clone(), address_.clone());
                                                    println!("linked to peer {} at {}", id_, address_);
                                                } else {
                                                    eprintln!("failed to receive LINK/ACK from {} at {}", id_, address_);
                                                }
                                            } else {
                                                eprintln!("failed to send LINK to {} at {}", id_, address_);
                                            }
                                        }
                                        Err(e) => eprintln!("failed to connect to {} at {}: {}", id_, address_, e),
                                    }
                                });
                            } else {
                                println!("usage -> LINK/{{address}}/{{id}}");
                            }
                        }
                        _ => {
                            println!("commands -> net  value  sync  LINK/{{address}}/{{id}}");
                        }
                    }
                }
            }
        }
    }

    async fn sync_with_peer(
        peer_addr: &str,
        my_id: &str,
        current_v: f64,
        v: Arc<Mutex<f64>>,
    ) -> Result<(), Box<dyn Error>> {
        println!("mock for sync_with_peer");
        Ok(())
    }
}
