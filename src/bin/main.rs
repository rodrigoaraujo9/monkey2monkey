use p2p_rust::monkey::Monkey;
use std::collections::HashMap;

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();

    // Usage: program <id> <address> [monkey_id:monkey_address ...]
    if args.len() < 3 {
        eprintln!(
            "usage -> {} <id> <address> [monkey_id:monkey_address ...]",
            args[0]
        );
        eprintln!(
            "example -> {} monkey1 127.0.0.1:8001 monkey2:127.0.0.1:8002 monkey3:127.0.0.1:8003",
            args[0]
        );
        eprintln!("");
        eprintln!(
            "the peer will automatically attempt to register with the provided peers on startup."
        );
        return;
    }

    let id = &args[1];
    let address = &args[2];

    let mut init_monkeys = HashMap::new();
    for i in 3..args.len() {
        let parts: Vec<&str> = args[i].split(':').collect();
        if parts.len() >= 2 {
            let monkey_id = parts[0].to_string();
            let monkey_addr = parts[1..].join(":");
            init_monkeys.insert(monkey_id, monkey_addr);
        } else {
            eprintln!("invalid monkey format '{}', expected 'id:address'", args[i]);
        }
    }

    let monkey = Monkey::new_monkey(id, address, init_monkeys.clone());

    println!("\n\n");
    print!("\x1B[2J\x1B[1;1H");
    println!(r"  /~\");
    println!(r" C oo");
    println!(r" _( ^)");
    println!(r"/   ~\");
    println!("{}", id);

    if let Err(e) = monkey.initiate_monkey_business().await {
        eprintln!("Error: {}", e);
    }
}
