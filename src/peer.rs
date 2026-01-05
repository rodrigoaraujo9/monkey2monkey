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
    state: Arc<Mutex<f64>>, //value v reffered in the sheet (converges across network via anti entropy)
    addr: String,           //IP:port
    tx: Tx,                 //sender
    rx: Arc<Mutex<Rx>>,     //receiver
    peers: Arc<RwLock<HashMap<String, String>>>, //known peers ----> (address -> address mapping)
}

impl Peer {
    /// Creates peer with optional initial state
    /// State defaults to random value in (ε, 1.0) if not provided
    pub fn new(addr: &str, peers: HashMap<String, String>, state: Option<f64>) -> Self {
        let (tx, rx) = mpsc::unbounded_channel(); //can run out of mem

        let init_state = state.unwrap_or_else(|| {
            let mut rng = rand::rng();
            rng.random_range(f64::EPSILON..1.0) //random initial state -> 0 < state <= 1
        });
        Self {
            state: Arc::new(Mutex::new(init_state)),
            addr: addr.to_string(),
            tx,
            rx: Arc::new(Mutex::new(rx)),
            peers: Arc::new(RwLock::new(peers)),
        }
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

    pub async fn get_peers(&self) -> Vec<String> {
        let peers = self.peers.read().await;
        peers.keys().cloned().collect()
    }

    /// Initializes peer:
    /// - Binds server socket
    /// - Spawns listener for incoming connections (REG/SYNC RPCs)
    /// - Spawns anti entropy loop for periodic state sync
    /// - Enters event loop for user input and async messages
    pub async fn start(&self) -> Result<(), Box<dyn Error>> {
        // display initial state
        println!("{}{:.6}{} \n", YELLOW, self.state.lock().await, RESET);

        // bind TCP listener to peer's network address
        // creates the server socket for accepting incoming connections
        let listener = TcpListener::bind(&self.addr).await?;

        // clone Arc pointers for listener task ----> cheap
        // each spawned task needs ownership of these values (static)
        let peers = Arc::clone(&self.peers);
        let tx = self.tx.clone();
        let state = Arc::clone(&self.state);

        // spawn background task to accept incoming peer connections
        // this task runs indefinitely, handling all incoming RPCs ----> REG and SYNC
        tokio::spawn(async move {
            let _ = Self::listen(listener, peers, tx, state).await;
        });

        // clone Arc pointers for anti entropy task
        let state = Arc::clone(&self.state);
        let peers = Arc::clone(&self.peers);

        let mut seed: [u8; 32] = [0u8; 32];
        rand::rng().fill_bytes(&mut seed);

        // if there are known peers ----> start anti entropy protocol
        // anti entropy task periodically initiates sync with random peers
        if peers.read().await.len() != 0 {
            tokio::spawn(async move {
                Self::anti_entropy(state, peers, &mut seed).await;
            });
        }

        // enter main event loop ----> blocking
        // handles user input and displays async messages from other tasks
        self.handle_input().await?;
        Ok(())
    }

    /// Anti entropy loop - push-based epidemic broadcast.
    /// Uses exponential distribution (λ=2/60) for delays, averages ~30s between rounds.
    /// Selects random peer, syncs state, repeats forever.
    async fn anti_entropy(
        state: Arc<Mutex<f64>>,
        peers: Arc<RwLock<HashMap<String, String>>>,
        seed: &[u8; 32],
    ) {
        let lambda = 60.0 / 60.0; // 2 events per minute
        let mut rng = SmallRng::from_seed(*seed);

        loop {
            let u: f64 = rng.random_range(std::f64::EPSILON..1.0);

            //generate exponentially distributed wait time
            //formula is -ln(U) / λ where U ~ Uniform(0,1)
            let wait = -u.ln() / lambda;

            //select random known peer from set
            if let Some(peer_addr) = Self::pick_peer(&peers, &mut rng).await {
                let my_state = *state.lock().await;

                //init sync with selected peer
                if let Err(e) = Self::sync(&peer_addr, my_state, &state).await {
                    eprintln!("{}sync with {} failed: {}{}", RED, peer_addr, e, RESET);
                }
            } else {
                println!("No peers available.");
            }
            println!("      anti entropy (next in ~{wait:.1}s)");

            //sleep until next anti entropy round ----> exponentially distributed interval
            sleep(Duration::from_secs_f64(wait)).await;
        }
    }

    /// Picks random peer from [peers] (uniform distribution).
    #[inline]
    pub async fn pick_peer(
        peers: &Arc<RwLock<HashMap<String, String>>>,
        rng: &mut SmallRng,
    ) -> Option<String> {
        let map = peers.read().await;
        map.keys().choose(rng).cloned()
    }

    /// Accepts incoming connections, spawns a handler per connection.
    /// Handles REG and SYNC RPCs
    pub async fn listen(
        listener: TcpListener,
        peers: Arc<RwLock<HashMap<String, String>>>,
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
                    let my_addr = my_addr.clone();

                    //spawn a task to handle the connection caught independently ----> runs concurrently with accept loop
                    tokio::spawn(async move {
                        if let Err(e) =
                            Self::handle_conn(socket, &peers, &state, &tx, &my_addr).await
                        {
                            eprintln!("{}conn error: {}{}", RED, e, RESET);
                        }
                    });
                }
                Err(e) => eprintln!("{}accept failed: {}{}", RED, e, RESET),
            }
        }
    }

    /// Handles single connection - processes REG and SYNC RPCs
    ///
    /// REG: REG/<peer_addr> -> REG/ACK/<my_addr>
    ///      adds/updates peer in registry
    ///
    /// SYNC: SYNC/<peer_state> -> SYNC/ACK/<new_state>
    ///       averages states: (my_state + peer_state) / 2
    ///       both peers converge to same value
    async fn handle_conn(
        socket: TcpStream,
        peers: &Arc<RwLock<HashMap<String, String>>>,
        state: &Arc<Mutex<f64>>,
        tx: &Tx,
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
                if let Some(addr) = parts.next() {
                    let mut map = peers.write().await;
                    let is_new = !map.contains_key(addr);
                    if is_new {
                        map.insert(addr.to_string(), addr.to_string());
                    }
                    drop(map);

                    if is_new {
                        println!("{}registered {}{}", GREEN, addr, RESET);
                        let _ = tx.send(format!("registered {}", addr));
                    }

                    let resp = format!("REG/ACK/{}\n", my_addr);
                    w.write_all(resp.as_bytes()).await?;
                }
            }
            //push-pull logic
            "SYNC" => {
                //SYNC/<peer_state>
                if let Some(v_str) = parts.next() {
                    if let Ok(peer_state) = v_str.parse::<f64>() {
                        //compute and apply state average (anti entropy convergence)
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
                            "[IN]  {}synchronized -> {:.6}{}",
                            YELLOW, new_val, RESET
                        ));
                    }
                }
            }
            _ => {} //base case ----> ignore unknown commands
        }
        Ok(())
    }

    /// Event loop - multiplexes user input and async messages
    /// Commands: peers, state, register/<addr>
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
                            println!("{}{} knows {:?}{}", BLUE, self.addr, known, RESET);
                        }
                        "state" => {
                            //display current state value ----> snapshot
                            let s = self.get_state().await;
                            println!("{}current state: {:.6}{}", YELLOW, s, RESET);
                        }
                        _ if input.starts_with("register/") => {
                            //register/<addr>
                            let mut it = input.split('/');
                            let _ = it.next(); //skip prefix ----> "register"
                            if let Some(addr) = it.next() {
                                println!("{}registering {}{}", PURPLE, addr, RESET);

                                //same clone logic
                                let target_addr = addr.to_string();
                                let self_addr = self.addr.clone();
                                let peers = Arc::clone(&self.peers);
                                let target_addr2 = addr.to_string();

                                //spawn task to perform registration handshake
                                //non-blocking ---->  allows user to continue issuing commands
                                tokio::spawn(async move {
                                    // attempt to connect to target peer ----> TCP
                                    match TcpStream::connect(&target_addr).await {
                                        Ok(mut stream) => {
                                            //send REG/<addr>
                                            let req = format!("REG/{}\n", self_addr);
                                            if stream.write_all(req.as_bytes()).await.is_ok() && stream.flush().await.is_ok() {
                                                let (r, _) = stream.into_split();
                                                let mut reader = BufReader::new(r);
                                                let mut buf = String::new();
                                                if reader.read_line(&mut buf).await.is_ok() {
                                                    //REG/ACK/<peer_addr>
                                                    let parts: Vec<&str> = buf.trim().split('/').collect();
                                                    if parts.len() == 3 && parts[0] == "REG" && parts[1] == "ACK" {
                                                        let peer_addr = parts[2];
                                                        // update peer registry with confirmed info
                                                        {
                                                            let mut map = peers.write().await;
                                                            map.insert(peer_addr.to_string(), peer_addr.to_string());
                                                        }
                                                        println!("{}registered {}{}", GREEN, peer_addr, RESET);
                                                    } else {
                                                        eprintln!("{}invalid response from {}{}", RED, target_addr2, RESET);
                                                    }
                                                } else {
                                                    eprintln!("{}no ACK from {}{}", RED, target_addr2, RESET);
                                                }
                                            } else {
                                                eprintln!("{}failed to send REG to {}{}", RED, target_addr2, RESET);
                                            }
                                        }
                                        Err(e) => eprintln!("{}failed to register {}: {}{}", RED, target_addr2, e, RESET),
                                    }
                                });
                            } else {
                                println!("{}usage -> register/{{address}}{}", ORANGE, RESET);
                            }
                        }
                        _ => {
                            //display help ----> unknown command
                            println!("{}commands -> peers  state  register/{{address}}{}", ORANGE, RESET);
                        }
                    }
                }
            }
        }
    }

    /// Client-side Push-Pull: sends state, receives averaged result, updates local state
    /// Both peers converge to (state1 + state2) / 2
    pub async fn sync(
        peer_addr: &str,
        my_state: f64,
        state: &Arc<Mutex<f64>>,
    ) -> Result<(), Box<dyn Error>> {
        // establish a TCP connection to peer
        let mut stream = TcpStream::connect(peer_addr).await?;

        //sync request SYNC/<state>
        let req = format!("SYNC/{:.12}\n", my_state);
        stream.write_all(req.as_bytes()).await?;
        stream.flush().await?;

        //read response
        let (r, _) = stream.into_split();
        let mut reader = BufReader::new(r);
        let mut resp = String::new();
        reader.read_line(&mut resp).await?;

        //SYNC/ACK/<new_state>
        let parts: Vec<&str> = resp.trim().split('/').collect();
        if parts.len() == 3 && parts[0] == "SYNC" && parts[1] == "ACK" {
            //retrieve new state value
            let new_val = parts[2].parse::<f64>()?;

            //update state with new value
            *state.lock().await = new_val;
            println!(
                "[OUT] {}synchronized with {} -> {:.6}{}",
                YELLOW, peer_addr, new_val, RESET
            );
            return Ok(());
        }

        Err("invalid SYNC response".into())
    }
}
