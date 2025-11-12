use p2p_rust::peer::Monkey;

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

    let peer = Monkey::new_monkey(id, address);

    println!("Starting peer {} on {}", id, address);
    println!("Commands: 'net', 'value', 'LINK/address/id'");

    if let Err(e) = peer.initiate_monkey_buisness().await {
        eprintln!("Error: {}", e);
    }
}
