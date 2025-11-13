use crate::{BLUE, GREEN, ORANGE, PURPLE, RED, RESET, Rx, Tx, YELLOW};
use rand::Rng;
use rand::SeedableRng;
use rand::rngs::StdRng;
use rand::seq::IteratorRandom;
use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, stdin};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::sync::{Mutex, RwLock};
use tokio::time::sleep;

pub struct Monkey {
    banana: Arc<Mutex<f64>>,
    id: String,
    address: String,
    sender: Tx,
    receiver: Arc<Mutex<Rx>>,
    monkeys: Arc<RwLock<HashMap<String, String>>>,
}

impl Monkey {
    pub fn new_monkey(id: &str, address: &str, monkeys: HashMap<String, String>) -> Self {
        let (sender, receiver) = mpsc::unbounded_channel();
        let mut rng = StdRng::from_entropy();
        let r: f64 = rng.gen_range(f64::EPSILON..1.0);
        Self {
            banana: Arc::new(Mutex::new(r)),
            id: id.to_string(),
            address: address.to_string(),
            sender,
            receiver: Arc::new(Mutex::new(receiver)),
            monkeys: Arc::new(RwLock::new(monkeys)),
        }
    }

    #[inline]
    pub fn id(&self) -> &str {
        &self.id
    }

    #[inline]
    pub fn address(&self) -> &str {
        &self.address
    }

    #[inline]
    pub async fn get_banana(&self) -> f64 {
        *self.banana.lock().await
    }

    #[inline]
    pub async fn set_banana(&self, value: f64) {
        *self.banana.lock().await = value;
    }

    pub async fn get_known_monkeys(&self) -> Vec<(String, String)> {
        let monkeys = self.monkeys.read().await;
        monkeys
            .iter()
            .map(|(id, addr)| (id.clone(), addr.clone()))
            .collect()
    }

    pub async fn initiate_monkey_business(&self) -> Result<(), Box<dyn Error>> {
        println!("{}{:.6}{} \n", YELLOW, self.banana.lock().await, RESET);

        let listener = TcpListener::bind(&self.address).await?;

        let monkeys = Arc::clone(&self.monkeys);
        let id = self.id.clone();
        let sender = self.sender.clone();
        let banana = Arc::clone(&self.banana);

        tokio::spawn(async move {
            let _ = Self::handle_incoming_monkeys(listener, monkeys, id, sender, banana).await;
        });

        let banana = Arc::clone(&self.banana);
        let monkeys = Arc::clone(&self.monkeys);
        let id = self.id.clone();
        let addr = self.address.clone();
        let sender = self.sender.clone();
        if monkeys.read().await.len() != 0 {
            tokio::spawn(async move {
                Self::interact_with_random_monkey(banana, monkeys, &id, &addr, sender).await;
            });
        }

        self.handle_monkey_interactions().await?;
        Ok(())
    }

    async fn interact_with_random_monkey(
        banana: Arc<Mutex<f64>>,
        monkeys: Arc<RwLock<HashMap<String, String>>>,
        id: &str,
        _addr: &str,
        _sender: Tx,
    ) {
        let lambda = 2.0 / 60.0;
        let mut rng = StdRng::from_entropy();

        loop {
            let u: f64 = rng.gen_range(std::f64::EPSILON..1.0);
            let wait_secs = -u.ln() / lambda;

            if let Some((monkey_id, addr)) = Self::pick_random_monkey(&monkeys, &mut rng).await {
                let my_banana = *banana.lock().await;
                if let Err(e) = Self::level_bananas(&addr, id, my_banana, &banana).await {
                    eprintln!(
                        "{}attempt to level banana with {}@{} failed: {}{}",
                        RED, monkey_id, addr, e, RESET
                    );
                }
            } else {
                println!("No monkeys available to contact.");
            }
            println!("      random monkey (next in ~{wait_secs:.1}s)");
            sleep(Duration::from_secs_f64(wait_secs)).await;
        }
    }

    #[inline]
    pub async fn pick_random_monkey(
        monkeys: &Arc<RwLock<HashMap<String, String>>>,
        rng: &mut StdRng,
    ) -> Option<(String, String)> {
        let map = monkeys.read().await;
        map.iter()
            .choose(rng)
            .map(|(id, addr)| (id.clone(), addr.clone()))
    }

    pub async fn handle_incoming_monkeys(
        listener: TcpListener,
        monkeys: Arc<RwLock<HashMap<String, String>>>,
        id: String,
        sender: Tx,
        banana: Arc<Mutex<f64>>,
    ) -> Result<(), Box<dyn Error>> {
        let my_addr = listener.local_addr()?.to_string();
        loop {
            match listener.accept().await {
                Ok((socket, _addr)) => {
                    let monkeys = Arc::clone(&monkeys);
                    let sender = sender.clone();
                    let banana = Arc::clone(&banana);
                    let id = id.clone();
                    let my_addr = my_addr.clone();

                    tokio::spawn(async move {
                        if let Err(e) = Self::keep_monkey_in_check(
                            socket, &monkeys, &banana, &sender, &id, &my_addr,
                        )
                        .await
                        {
                            eprintln!("{}keep_monkey_in_check error: {}{}", RED, e, RESET);
                        }
                    });
                }
                Err(e) => eprintln!(
                    "{}monkey failed to register incoming monkeys: {}{}",
                    RED, e, RESET
                ),
            }
        }
    }

    async fn keep_monkey_in_check(
        socket: TcpStream,
        monkeys: &Arc<RwLock<HashMap<String, String>>>,
        v: &Arc<Mutex<f64>>,
        sender: &Tx,
        my_id: &str,
        my_addr: &str,
    ) -> Result<(), Box<dyn Error>> {
        let (read_half, mut writer) = socket.into_split();
        let mut reader = BufReader::new(read_half);
        let mut line = String::new();

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
            "REG" => {
                if let (Some(address), Some(id)) = (parts.next(), parts.next()) {
                    let needs_update = {
                        let map = monkeys.read().await;
                        match map.get(id) {
                            None => true,
                            Some(old_addr) => old_addr != address,
                        }
                    };

                    if needs_update {
                        let mut map = monkeys.write().await;
                        map.insert(id.to_string(), address.to_string());
                        drop(map);

                        println!("{}registered {} at {}{}", GREEN, id, address, RESET);
                        let _ = sender.send(format!("registered {} at {}", id, address));
                    }

                    let response = format!("REG/ACK/{}/{}\n", my_addr, my_id);
                    writer.write_all(response.as_bytes()).await?;
                }
            }
            "LEVEL" => {
                if let (Some(monkey_id), Some(v_str)) = (parts.next(), parts.next()) {
                    if let Ok(monkey_banana) = v_str.parse::<f64>() {
                        let new_value = {
                            let mut current_banana = v.lock().await;
                            *current_banana = (*current_banana + monkey_banana) / 2.0;
                            *current_banana
                        };

                        writer
                            .write_all(format!("LEVEL/ACK/{:.12}\n", new_value).as_bytes())
                            .await?;
                        let _ = sender.send(format!(
                            "[IN]  {}leveled banana with {} -> {:.6}{}",
                            YELLOW, monkey_id, new_value, RESET
                        ));
                    }
                }
            }
            _ => {}
        }
        Ok(())
    }

    async fn handle_monkey_interactions(&self) -> Result<(), Box<dyn Error>> {
        let mut stdin = BufReader::new(stdin()).lines();
        let receiver = Arc::clone(&self.receiver);

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
                            let known: Vec<String> = {
                                let m_list = self.monkeys.read().await;
                                m_list.keys().cloned().collect()
                            };
                            println!("{}{} knows {:?}{}", BLUE, self.id, known, RESET);
                        }
                        "banana" => {
                            let banana = self.get_banana().await;
                            println!("{}current banana: {:.6}{}", YELLOW, banana, RESET);
                        }
                        _ if input.starts_with("register/") => {
                            let mut it = input.split('/');
                            let _ = it.next();
                            if let (Some(address), Some(id)) = (it.next(), it.next()) {
                                println!("{}attempting to register {} at {}{}", PURPLE, id, address, RESET);

                                let addr_for_spawn = address.to_string();
                                let self_addr = self.address.clone();
                                let self_id = self.id.clone();
                                let monkeys = Arc::clone(&self.monkeys);
                                let id_ = id.to_string();
                                let address_ = address.to_string();

                                tokio::spawn(async move {
                                    match TcpStream::connect(&addr_for_spawn).await {
                                        Ok(mut stream) => {
                                            let line = format!("REG/{}/{}\n", self_addr, self_id);
                                            if stream.write_all(line.as_bytes()).await.is_ok() && stream.flush().await.is_ok() {
                                                let (r, _) = stream.into_split();
                                                let mut r = BufReader::new(r);
                                                let mut tmp = String::new();
                                                if r.read_line(&mut tmp).await.is_ok() {
                                                    let parts: Vec<&str> = tmp.trim().split('/').collect();
                                                    if parts.len() == 4 && parts[0] == "REG" && parts[1] == "ACK" {
                                                        let monkey_addr = parts[2];
                                                        let monkey_id = parts[3];
                                                        {
                                                            let mut map = monkeys.write().await;
                                                            map.insert(monkey_id.to_string(), monkey_addr.to_string());
                                                        }
                                                        println!("{}registered {} at {}{}", GREEN, monkey_id, monkey_addr, RESET);
                                                    } else {
                                                        eprintln!("{}invalid register response from {}{}", RED, address_, RESET);
                                                    }
                                                } else {
                                                    eprintln!("{}failed to receive REG/ACK from {} at {}{}", RED, id_, address_, RESET);
                                                }
                                            } else {
                                                eprintln!("{}failed to send REG to {} at {}{}", RED, id_, address_, RESET);
                                            }
                                        }
                                        Err(e) => eprintln!("{}failed to register {} at {}: {}{}", RED, id_, address_, e, RESET),
                                    }
                                });
                            } else {
                                println!("{}usage -> register/{{address}}/{{id}}{}", ORANGE, RESET);
                            }
                        }
                        _ => {
                            println!("{}commands -> monkeys  banana  register/{{address}}/{{id}}{}", ORANGE, RESET);
                        }
                    }
                }
            }
        }
    }

    pub async fn level_bananas(
        monkey_addr: &str,
        my_id: &str,
        current_banana: f64,
        banana: &Arc<Mutex<f64>>,
    ) -> Result<(), Box<dyn Error>> {
        let mut stream = TcpStream::connect(monkey_addr).await?;

        let request = format!("LEVEL/{}/{:.12}\n", my_id, current_banana);
        stream.write_all(request.as_bytes()).await?;
        stream.flush().await?;

        let (r, _) = stream.into_split();
        let mut reader = BufReader::new(r);
        let mut response = String::new();
        reader.read_line(&mut response).await?;

        let parts: Vec<&str> = response.trim().split('/').collect();
        if parts.len() == 3 && parts[0] == "LEVEL" && parts[1] == "ACK" {
            let new_value = parts[2].parse::<f64>()?;
            *banana.lock().await = new_value;
            println!(
                "[OUT] {}leveled banana with {} -> {:.6}{}",
                YELLOW, monkey_addr, new_value, RESET
            );
            return Ok(());
        }

        Err("invalid LEVEL response".into())
    }
}
