use crate::{Rx, Tx};
use rand::random;
use rand::rngs::mock;
use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, stdin};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio::sync::mpsc;
use tokio::time::{Duration, sleep};

pub struct Monkey {
    banana: Arc<Mutex<f64>>,
    id: String,
    address: String,
    sender: Tx,
    receiver: Arc<Mutex<Rx>>,
    monkeys: Arc<Mutex<HashMap<String, String>>>,
}

impl Monkey {
    pub fn new_monkey(id: &str, address: &str) -> Self {
        let (sender, receiver) = mpsc::unbounded_channel();
        Self {
            banana: Arc::new(Mutex::new(random::<f64>())),
            id: id.to_string(),
            address: address.to_string(),
            sender,
            receiver: Arc::new(Mutex::new(receiver)),
            monkeys: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn initiate_monkey_buisness(&self) -> Result<(), Box<dyn Error>> {
        let listener = TcpListener::bind(&self.address).await?;
        let monkeys_ = self.monkeys.clone();
        let id_ = self.id.clone();
        let sender_ = self.sender.clone();
        let banana_ = self.banana.clone();
        tokio::spawn(async move {
            let _ = Self::handle_incomming_monkeys(listener, monkeys_, id_, sender_, banana_).await;
        });

        let banana_ = self.banana.clone();
        let monkeys_ = self.monkeys.clone();
        let id_ = self.id.clone();
        let addr_ = self.address.clone();
        let sender_ = self.sender.clone();
        tokio::spawn(async move {
            Self::interact_with_random_monkey(banana_, monkeys_, &id_, &addr_, sender_).await;
        });

        self.handle_monkey_interactions().await?;
        Ok(())
    }

    async fn interact_with_random_monkey(
        banana: Arc<Mutex<f64>>,
        monkeys: Arc<Mutex<HashMap<String, String>>>,
        id: &str,
        _addr: &str,
        sender: Tx,
    ) {
        println!("mock for interact with random monkey")
    }

    //handle connections
    pub async fn handle_incomming_monkeys(
        listener: TcpListener,
        monkeys: Arc<Mutex<HashMap<String, String>>>,
        id: String,
        sender: Tx,
        banana: Arc<Mutex<f64>>,
    ) -> Result<(), Box<dyn Error>> {
        let my_addr = listener.local_addr()?.to_string();
        loop {
            match listener.accept().await {
                Ok((socket, addr)) => {
                    println!("incoming connection from {}", addr);
                    let monkeys_ = monkeys.clone();
                    let sender_ = sender.clone();
                    let banana_ = banana.clone();
                    let id_ = id.clone();
                    let my_addr_ = my_addr.clone();
                    tokio::spawn(async move {
                        if let Err(e) = Self::keep_monkey_in_check(
                            socket, monkeys_, banana_, sender_, id_, my_addr_,
                        )
                        .await
                        {
                            eprintln!("keep_monkey_in_check error: {}", e);
                        }
                    });
                }
                Err(e) => eprintln!("Monkey failed to accept incoming monkeys: {}", e),
            }
        }
    }

    //handle peer
    async fn keep_monkey_in_check(
        socket: TcpStream,
        monkeys: Arc<Mutex<HashMap<String, String>>>,
        v: Arc<Mutex<f64>>,
        sender: Tx,
        my_id: String,
        my_addr: String,
    ) -> Result<(), Box<dyn Error>> {
        let (read_half, mut writer) = socket.into_split();
        let mut reader = BufReader::new(read_half);
        let mut line = String::new();

        // Handle single request then close
        reader.read_line(&mut line).await?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            return Ok(());
        }

        let mut parts = trimmed.split('/');
        let Some(cmd) = parts.next() else {
            return Ok(());
        };

        match cmd {
            "LINK" => {
                if let (Some(address), Some(id)) = (parts.next(), parts.next()) {
                    monkeys
                        .lock()
                        .await
                        .insert(id.to_string(), address.to_string());
                    let _ = sender.send(format!("connected with {} at {}", id, address));
                    let response = format!("LINK/ACK/{}/{}\n", my_addr, my_id);
                    let _ = writer.write_all(response.as_bytes()).await;
                }
            }
            "LEVEL" => {
                if let (Some(monkey_id), Some(v_str)) = (parts.next(), parts.next()) {
                    if let Ok(monkey_banana) = v_str.parse::<f64>() {
                        let mut current_banana = v.lock().await;
                        *current_banana = (*current_banana + monkey_banana) / 2.0;
                        let _ = writer
                            .write_all(format!("LEVEL/ACK/{:.12}\n", *current_banana).as_bytes())
                            .await;
                        let _ = sender.send(format!(
                            "Leveled banana with {} -> v={:.6}",
                            monkey_id, *current_banana
                        ));
                    }
                }
            }
            _ => {}
        }
        Ok(())
    }

    //handle communications
    async fn handle_monkey_interactions(&self) -> Result<(), Box<dyn Error>> {
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
                        "monkeys" => {
                            let m_list = self.monkeys.lock().await;
                            println!("{} connected to {:?}", self.id, m_list.keys().cloned().collect::<Vec<_>>());
                        }
                        "banana" => {
                            let banana = *self.banana.lock().await;
                            println!("current banana: {}", banana);
                        }
                        "level" => {
                            let snapshot = { let p = self.monkeys.lock().await; p.clone() };
                            let my_banana = *self.banana.lock().await;
                            for (monkey_id, addr) in snapshot {
                                if let Err(e) = Self::level_bananas(&addr, &self.id, my_banana, self.banana.clone()).await {
                                    eprintln!("attempt to level banana with {}@{} failed: {}", monkey_id, addr, e);
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
                                let monkeys = self.monkeys.clone();
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
                                                if r.read_line(&mut tmp).await.is_ok() {
                                                    let parts: Vec<&str> = tmp.trim().split('/').collect();
                                                    // LINK/ACK/addr/id
                                                    if parts.len() == 4 && parts[0] == "LINK" && parts[1] == "ACK" {
                                                        let monkey_addr = parts[2];
                                                        let monkey_id = parts[3];
                                                        monkeys.lock().await.insert(monkey_id.to_string(), monkey_addr.to_string());
                                                        println!("linked to monkey {} at {}", monkey_id, monkey_addr);
                                                    } else {
                                                        eprintln!("invalid LINK response from {}", address_);
                                                    }
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
                            println!("commands -> monkeys  banana  level  LINK/{{address}}/{{id}}");
                        }
                    }
                }
            }
        }
    }

    async fn level_bananas(
        monkey_addr: &str,
        my_id: &str,
        current_banana: f64,
        banana: Arc<Mutex<f64>>,
    ) -> Result<(), Box<dyn Error>> {
        let mut stream = TcpStream::connect(monkey_addr).await?;

        // Send LEVEL request
        let request = format!("LEVEL/{}/{:.12}\n", my_id, current_banana);
        stream.write_all(request.as_bytes()).await?;
        stream.flush().await?;

        // Read response
        let (r, _) = stream.into_split();
        let mut reader = BufReader::new(r);
        let mut response = String::new();
        reader.read_line(&mut response).await?;

        // Parse LEVEL/ACK/new_value
        let parts: Vec<&str> = response.trim().split('/').collect();
        if parts.len() == 3 && parts[0] == "LEVEL" && parts[1] == "ACK" {
            let new_value = parts[2].parse::<f64>()?;
            *banana.lock().await = new_value;
            return Ok(());
        }

        Err("Invalid LEVEL response".into())
    }
}
