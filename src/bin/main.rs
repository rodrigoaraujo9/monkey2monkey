use p2p_rust::peer::Peer;
use std::collections::HashMap;

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    // Usage: program <id> <address> [peer_id:peer_address ...]
    if args.len() < 3 {
        eprintln!(
            "usage -> {} <id> <address> [peer_id:peer_address ...]",
            args[0]
        );
        eprintln!(
            "example -> {} peer1 127.0.0.1:8001 peer2:127.0.0.1:8002 peer3:127.0.0.1:8003",
            args[0]
        );
        eprintln!("");
        eprintln!(
            "the peer will automatically attempt to register with the provided peers on startup."
        );
        return;
    }

    let id = &args[1];
    let addr = &args[2];
    let mut init_peers = HashMap::new();

    for i in 3..args.len() {
        let parts: Vec<&str> = args[i].split(':').collect();
        if parts.len() >= 2 {
            let peer_id = parts[0].to_string();
            let peer_addr = parts[1..].join(":");
            init_peers.insert(peer_id, peer_addr);
        } else {
            eprintln!("invalid peer format '{}', expected 'id:address'", args[i]);
        }
    }

    let peer = Peer::new(id, addr, init_peers.clone());

    println!("\n\n");
    print!("\x1B[2J\x1B[1;1H");
    println!(r"  /~\");
    println!(r" C oo");
    println!(r" _( ^)");
    println!(r"/   ~\");
    println!("{}", id);

    if let Err(e) = peer.start().await {
        eprintln!("Error: {}", e);
    }
}
