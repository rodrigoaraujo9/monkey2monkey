use p2p_rust::peer::Peer;
use std::collections::HashMap;
#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    // main.rs <address> [peer_address ...] <init state|None>
    if args.len() < 3 {
        eprintln!(
            "usage -> {} <address> [peer_address ...] <init state|None>",
            args[0]
        );
        eprintln!(
            "example -> {} 127.0.0.1:8001 127.0.0.1:8002 127.0.0.1:8003 None",
            args[0]
        );
        eprintln!();
        eprintln!(
            "the peer will automatically attempt to register with the provided peers on startup."
        );
        return;
    }
    let addr = &args[1];
    let init_state = &args[args.len() - 1];
    let mut init_peers = HashMap::new();
    for i in 2..args.len() - 1 {
        let peer_addr = args[i].to_string();
        init_peers.insert(peer_addr.clone(), peer_addr);
    }
    let init_state: Option<f64> = if init_state.eq_ignore_ascii_case("none") {
        None
    } else {
        let v: f64 = match init_state.parse() {
            Ok(v) => v,
            Err(_) => {
                eprintln!(
                    "invalid initial state '{}', expected a float in (0, 1] or 'None'",
                    init_state
                );
                std::process::exit(1);
            }
        };
        assert!(
            v > 0.0 && v <= 1.0,
            "initial state must be in (0, 1], got {}",
            v
        );
        Some(v)
    };
    let peer = Peer::new(addr, init_peers.clone(), init_state);
    println!("\n\n");
    print!("\x1B[2J\x1B[1;1H");
    println!(r"  /~\");
    println!(r" C oo");
    println!(r" _( ^)");
    println!(r"/   ~\");
    println!("{}", addr);
    if let Err(e) = peer.start().await {
        eprintln!("Error: {}", e);
    }
}
