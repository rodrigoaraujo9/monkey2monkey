use crate::{BLUE, GREEN, ORANGE, PURPLE, RED, RESET, Rx, Tx, YELLOW};
use rand::rngs::SmallRng;
use rand::seq::IteratorRandom;
use rand::{Rng, RngCore, SeedableRng};
use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, stdin};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::sync::{Mutex, RwLock};
use tokio::time::sleep;

pub struct Peer {
    state: Arc<Mutex<f64>>, //value v reffered in the sheet (converges across network via gossip)
    id: String,
    addr: String,                                //IP:port
    tx: Tx,                                      //sender
    rx: Arc<Mutex<Rx>>,                          //receiver
    peers: Arc<RwLock<HashMap<String, String>>>, //known peers ----> (id -> address mapping)
}

impl Peer {
    pub fn new(id: &str, addr: &str, peers: HashMap<String, String>) -> Self {
        let (tx, rx) = mpsc::unbounded_channel(); //can run out of mem
        let mut rng = rand::rng();
        let r: f64 = rng.random_range(f64::EPSILON..1.0); //random initial state -> 0 < state <= 1
        Self {
            state: Arc::new(Mutex::new(r)),
            id: id.to_string(),
            addr: addr.to_string(),
            tx,
            rx: Arc::new(Mutex::new(rx)),
            peers: Arc::new(RwLock::new(peers)),
        }
    }

    #[inline]
    pub fn id(&self) -> &str {
        &self.id
    }

    #[inline]
    pub fn addr(&self) -> &str {
        &self.addr
    }

    #[inline]
    pub async fn get_state(&self) -> f64 {
        *self.state.lock().await
    }

    #[inline]
    pub async fn set_state(&self, val: f64) {
        *self.state.lock().await = val;
    }

    pub async fn get_peers(&self) -> Vec<(String, String)> {
        let peers = self.peers.read().await;
        peers
            .iter()
            .map(|(id, addr)| (id.clone(), addr.clone()))
            .collect()
    }

    pub async fn start(&self) -> Result<(), Box<dyn Error>> {
        // display initial state
        println!("{}{:.6}{} \n", YELLOW, self.state.lock().await, RESET);

        // bind TCP listener to peer's network address
        // creates the server socket for accepting incoming connections
        let listener = TcpListener::bind(&self.addr).await?;

        // clone Arc pointers for listener task ----> cheap
        // each spawned task needs ownership of these values (static)
        let peers = Arc::clone(&self.peers);
        let id = self.id.clone();
        let tx = self.tx.clone();
        let state = Arc::clone(&self.state);

        // spawn background task to accept incoming peer connections
        // this task runs indefinitely, handling all incoming RPCs ----> REG and SYNC
        tokio::spawn(async move {
            let _ = Self::listen(listener, peers, id, tx, state).await;
        });

        // clone Arc pointers for gossip task
        let state = Arc::clone(&self.state);
        let peers = Arc::clone(&self.peers);
        let id = self.id.clone();

        let mut seed: [u8; 32] = [0u8; 32];
        rand::rng().fill_bytes(&mut seed);

        // if there are known peers ----> start gossip protocol
        // gossip task periodically initiates sync with random peers
        if peers.read().await.len() != 0 {
            tokio::spawn(async move {
                Self::gossip(state, peers, &id, &mut seed).await;
            });
        }

        // enter main event loop ----> blocking
        // handles user input and displays async messages from other tasks
        self.handle_input().await?;
        Ok(())
    }

    async fn gossip(
        state: Arc<Mutex<f64>>,
        peers: Arc<RwLock<HashMap<String, String>>>,
        id: &str,
        seed: &[u8; 32],
    ) {
        let lambda = 2.0 / 60.0; // 2 events per minute
        let mut rng = SmallRng::from_seed(*seed);

        loop {
            let u: f64 = rng.random_range(std::f64::EPSILON..1.0);

            //generate exponentially distributed wait time
            //formula is -ln(U) / λ where U ~ Uniform(0,1)
            let wait = -u.ln() / lambda;

            //select random known peer from set
            if let Some((peer_id, peer_addr)) = Self::pick_peer(&peers, &mut rng).await {
                let my_state = *state.lock().await;

                //init sync with selected peer
                if let Err(e) = Self::sync(&peer_addr, id, my_state, &state).await {
                    eprintln!(
                        "{}sync with {}@{} failed: {}{}",
                        RED, peer_id, peer_addr, e, RESET
                    );
                }
            } else {
                println!("No peers available.");
            }
            println!("      gossip (next in ~{wait:.1}s)");

            //sleep until next gossip round ----> exponentially distributed interval
            sleep(Duration::from_secs_f64(wait)).await;
        }
    }

    #[inline]
    pub async fn pick_peer(
        peers: &Arc<RwLock<HashMap<String, String>>>,
        rng: &mut SmallRng,
    ) -> Option<(String, String)> {
        let map = peers.read().await;
        map.iter()
            .choose(rng)
            .map(|(id, addr)| (id.clone(), addr.clone()))
    }

    pub async fn listen(
        listener: TcpListener,
        peers: Arc<RwLock<HashMap<String, String>>>,
        id: String,
        tx: Tx,
        state: Arc<Mutex<f64>>,
    ) -> Result<(), Box<dyn Error>> {
        //get local address for logging and RPC responses
        let my_addr = listener.local_addr()?.to_string();
        loop {
            //block until incoming connection arrives
            match listener.accept().await {
                Ok((socket, _addr)) => {
                    //same clone logic
                    let peers = Arc::clone(&peers);
                    let tx = tx.clone();
                    let state = Arc::clone(&state);
                    let id = id.clone();
                    let my_addr = my_addr.clone();

                    //spawn a task to handle the connection caught independently ----> runs concurrently with accept loop
                    tokio::spawn(async move {
                        if let Err(e) =
                            Self::handle_conn(socket, &peers, &state, &tx, &id, &my_addr).await
                        {
                            eprintln!("{}conn error: {}{}", RED, e, RESET);
                        }
                    });
                }
                Err(e) => eprintln!("{}accept failed: {}{}", RED, e, RESET),
            }
        }
    }

    async fn handle_conn(
        socket: TcpStream,
        peers: &Arc<RwLock<HashMap<String, String>>>,
        state: &Arc<Mutex<f64>>,
        tx: &Tx,
        my_id: &str,
        my_addr: &str,
    ) -> Result<(), Box<dyn Error>> {
        //split communication into read and write halves ----> independant
        let (r, mut w) = socket.into_split();
        let mut reader = BufReader::new(r);
        let mut line = String::new();

        //read request (line) ----> blocks until newline or EOF
        reader.read_line(&mut line).await?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            return Ok(());
        }

        //ignore empty requests ----> filter out noise
        let mut parts = trimmed.split('/');
        let Some(cmd) = parts.next() else {
            return Ok(());
        };

        //dispatch based on command
        match cmd {
            "REG" => {
                //REG/<peer_addr>/<peer_id>
                if let (Some(addr), Some(id)) = (parts.next(), parts.next()) {
                    //check if peer (in [peers]) needs reg or update addr
                    let needs_update = {
                        //read lock
                        let map = peers.read().await;
                        match map.get(id) {
                            None => true,             //new peer ----> register
                            Some(old) => old != addr, // address changed ----> update
                        }
                    };
                    //read unlock ----> end of closure

                    if needs_update {
                        // write lock
                        let mut map = peers.write().await;
                        map.insert(id.to_string(), addr.to_string());
                        drop(map); // unlock ----> explicit

                        //notify
                        println!("{}registered {} at {}{}", GREEN, id, addr, RESET);
                        let _ = tx.send(format!("registered {} at {}", id, addr));
                    }

                    // Send ACK response with our contact info ----> for him to register this peer
                    let resp = format!("REG/ACK/{}/{}\n", my_addr, my_id);
                    w.write_all(resp.as_bytes()).await?;
                }
            }
            "SYNC" => {
                //SYNC/<peer_id>/<peer_state>
                if let (Some(peer_id), Some(v_str)) = (parts.next(), parts.next()) {
                    if let Ok(peer_state) = v_str.parse::<f64>() {
                        //compute and apply state average (gossip convergence)
                        let new_val = {
                            //aquire mutex to read and update state ----> atomically
                            let mut s = state.lock().await;

                            //average the two
                            *s = (*s + peer_state) / 2.0;
                            *s
                        };

                        //send new state back to synced peer ----> bidirectional propagation
                        w.write_all(format!("SYNC/ACK/{:.12}\n", new_val).as_bytes())
                            .await?;

                        //notify
                        let _ = tx.send(format!(
                            "[IN]  {}synced with {} -> {:.6}{}",
                            YELLOW, peer_id, new_val, RESET
                        ));
                    }
                }
            }
            _ => {} //base case ----> ignore unknown commands
        }
        Ok(())
    }

    async fn handle_input(&self) -> Result<(), Box<dyn Error>> {
        //async line for stdin
        let mut stdin = BufReader::new(stdin()).lines();

        //same clone logic ----> to be moved into select loop
        let rx = Arc::clone(&self.rx);

        loop {
            // multiplex between two async event sources
            tokio::select! {
                //event 1 ----> message from background task
                msg = async {
                    let mut receiver = rx.lock().await;
                    receiver.recv().await
                } => {
                    //display message if available ----> None means channel is closed
                    if let Some(msg) = msg { println!("{}", msg); }
                }

                //event 2 ----> user input from stdin
                line = stdin.next_line() => {
                    //read errors or EOF
                    let Ok(Some(line)) = line else { continue; };
                    let input = line.trim();

                    //ignore empty lines ----> noise
                    if input.is_empty() { continue; }

                    //dispatch command
                    match input {
                        "peers" => {
                            //display known peers ----> snapshot
                            let known: Vec<String> = {
                                let map = self.peers.read().await;
                                map.keys().cloned().collect()
                            };
                            println!("{}{} knows {:?}{}", BLUE, self.id, known, RESET);
                        }
                        "state" => {
                            //display current state value ----> snapshot
                            let s = self.get_state().await;
                            println!("{}current state: {:.6}{}", YELLOW, s, RESET);
                        }
                        _ if input.starts_with("register/") => {
                            //register/<addr>/<id>
                            let mut it = input.split('/');
                            let _ = it.next(); //skip prefix ----> "register"
                            if let (Some(addr), Some(id)) = (it.next(), it.next()) {
                                println!("{}registering {} at {}{}", PURPLE, id, addr, RESET);

                                //same clone logic
                                let target_addr = addr.to_string();
                                let self_addr = self.addr.clone();
                                let self_id = self.id.clone();
                                let peers = Arc::clone(&self.peers);
                                let target_id = id.to_string();
                                let target_addr2 = addr.to_string();


                                //spawn task to perform registration handshake
                                //non-blocking ---->  allows user to continue issuing commands
                                tokio::spawn(async move {
                                    match TcpStream::connect(&target_addr).await {
                                        Ok(mut stream) => {
                                            let req = format!("REG/{}/{}\n", self_addr, self_id);
                                            if stream.write_all(req.as_bytes()).await.is_ok() && stream.flush().await.is_ok() {
                                                let (r, _) = stream.into_split();
                                                let mut reader = BufReader::new(r);
                                                let mut buf = String::new();
                                                if reader.read_line(&mut buf).await.is_ok() {
                                                    let parts: Vec<&str> = buf.trim().split('/').collect();
                                                    if parts.len() == 4 && parts[0] == "REG" && parts[1] == "ACK" {
                                                        let peer_addr = parts[2];
                                                        let peer_id = parts[3];
                                                        {
                                                            let mut map = peers.write().await;
                                                            map.insert(peer_id.to_string(), peer_addr.to_string());
                                                        }
                                                        println!("{}registered {} at {}{}", GREEN, peer_id, peer_addr, RESET);
                                                    } else {
                                                        eprintln!("{}invalid response from {}{}", RED, target_addr2, RESET);
                                                    }
                                                } else {
                                                    eprintln!("{}no ACK from {} at {}{}", RED, target_id, target_addr2, RESET);
                                                }
                                            } else {
                                                eprintln!("{}failed to send REG to {} at {}{}", RED, target_id, target_addr2, RESET);
                                            }
                                        }
                                        Err(e) => eprintln!("{}failed to register {} at {}: {}{}", RED, target_id, target_addr2, e, RESET),
                                    }
                                });
                            } else {
                                println!("{}usage -> register/{{address}}/{{id}}{}", ORANGE, RESET);
                            }
                        }
                        _ => {
                            println!("{}commands -> peers  state  register/{{address}}/{{id}}{}", ORANGE, RESET);
                        }
                    }
                }
            }
        }
    }

    pub async fn sync(
        peer_addr: &str,
        my_id: &str,
        my_state: f64,
        state: &Arc<Mutex<f64>>,
    ) -> Result<(), Box<dyn Error>> {
        let mut stream = TcpStream::connect(peer_addr).await?;

        let req = format!("SYNC/{}/{:.12}\n", my_id, my_state);
        stream.write_all(req.as_bytes()).await?;
        stream.flush().await?;

        let (r, _) = stream.into_split();
        let mut reader = BufReader::new(r);
        let mut resp = String::new();
        reader.read_line(&mut resp).await?;

        let parts: Vec<&str> = resp.trim().split('/').collect();
        if parts.len() == 3 && parts[0] == "SYNC" && parts[1] == "ACK" {
            let new_val = parts[2].parse::<f64>()?;
            *state.lock().await = new_val;
            println!(
                "[OUT] {}synced with {} -> {:.6}{}",
                YELLOW, peer_addr, new_val, RESET
            );
            return Ok(());
        }

        Err("invalid SYNC response".into())
    }
}
