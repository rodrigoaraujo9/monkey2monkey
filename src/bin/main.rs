use p2p_rust::peer::Peer;

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();

    if args.len() != 3 {
        eprintln!("Usage: {} <id> <address>", args[0]);
        eprintln!("Example: {} peer1 127.0.0.1:8001", args[0]);
        return;
    }

    let id = &args[1];
    let address = &args[2];

    let peer = Peer::new(id, address);

    println!("Starting peer {} on {}", id, address);
    println!("Commands: 'net', 'value', 'LINK:address:id'");

    if let Err(e) = peer.start().await {
        eprintln!("Error: {}", e);
    }
}
