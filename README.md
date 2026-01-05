# Anti-Entropy Gossip Protocol

Distributed state synchronization using push-pull gossip with exponential anti-entropy.

## Running

Start peers manually:

```bash
cargo run --release -- 127.0.0.1:8001 127.0.0.1:8002 None
cargo run --release -- 127.0.0.1:8002 127.0.0.1:8001 127.0.0.1:8003 127.0.0.1:8004 None
cargo run --release -- 127.0.0.1:8003 127.0.0.1:8002 127.0.0.1:8004 None
cargo run --release -- 127.0.0.1:8004 127.0.0.1:8002 127.0.0.1:8003 127.0.0.1:8005 127.0.0.1:8006 None
cargo run --release -- 127.0.0.1:8005 127.0.0.1:8004 None
cargo run --release -- 127.0.0.1:8006 127.0.0.1:8004 None
```

Format: `my_address peer1 peer2 ... peerN initial_state`

Use `None` for random initial state.

## Automated Testing

Generate and run network:

```bash
python run.py 10
docker compose up
```

Or in one command:

```bash
python run.py 10 start
```

Run convergence simulation:

```bash
python simulate.py
```

Peers gossip state using exponential intervals (λ=2/60, ~30s average). States converge to network average via push-pull synchronization.

## Commands

- `peers` - list known peers
- `state` - show current state value
- `register/<address>` - manually register new peer
