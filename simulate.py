import random
import re
import subprocess
import time

import matplotlib.pyplot as plt


def generate_topology(n):
    """random connected topology"""
    edges = []
    for i in range(1, n):
        edges.append((random.randint(0, i-1), i))
    for i in range(n):
        for j in range(i+1, n):
            if random.random() < 0.3:
                edges.append((i, j))
    return edges

def run_simulation(n, max_duration=180):
    """run gossip with N peers until convergence"""
    print(f"\n{'='*60}")
    print(f"testing N={n} peers")
    print(f"target convergence: 1/{n} = {1.0/n:.6f}")
    print(f"{'='*60}\n")

    topology = generate_topology(n)
    peers = {i: [] for i in range(n)}
    for a, b in topology:
        peers[a].append(b)
        peers[b].append(a)

    compose = "services:\n"
    for i in range(n):
        port = 8000 + i
        neighbors = " ".join([f'"peer{j}:{8000+j}",' for j in peers[i]])
        state = '"1.0"' if i == 0 else f'"{1e-9}"'
        compose += f"""  peer{i}:
    build: .
    command: ["0.0.0.0:{port}", {neighbors} {state}]
    networks:
      - net
    ports:
      - "{port}:{port}"

"""
    compose += "networks:\n  net:\n    driver: bridge\n"

    with open("docker-compose.yml", "w") as f:
        f.write(compose)

    subprocess.run(["docker", "compose", "up", "-d"],
                  stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    process = subprocess.Popen(
        ["docker", "compose", "logs", "-f"],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        universal_newlines=True
    )

    start = time.time()
    target = 1.0 / n
    tolerance = 0.001
    recent_values = []
    converged = False

    try:
        while time.time() - start < max_duration:
            line = process.stdout.readline()
            if line:
                print(line, end='')

                # Extract state values from sync messages
                match = re.search(r'-> ([\d.]+)', line)
                if match:
                    value = float(match.group(1))
                    recent_values.append(value)

                    # Keep only last 20 values
                    if len(recent_values) > 20:
                        recent_values.pop(0)

                    # Check convergence: all recent values close to target
                    if len(recent_values) >= 10:
                        if all(abs(v - target) < tolerance for v in recent_values[-10:]):
                            converged = True
                            break

            time.sleep(0.01)
    except KeyboardInterrupt:
        pass
    finally:
        process.terminate()
        process.wait()

    elapsed = time.time() - start

    print(f"\n\nstopping containers...")
    subprocess.run(["docker", "compose", "down", "-v"],
                  stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    if converged:
        print(f"converged in {elapsed:.1f}s\n")
        return elapsed
    else:
        print(f"timeout after {elapsed:.1f}s\n")
        return None

# Run simulations
results = []
for n in [3, 5, 7, 10, 15]:
    conv_time = run_simulation(n, max_duration=180)
    if conv_time:
        results.append((n, conv_time))
    time.sleep(2)

# Plot results
if results:
    print(f"\n{'='*60}")
    print("generating plot...")
    print(f"{'='*60}\n")

    plt.figure(figsize=(10, 6))
    plt.plot([r[0] for r in results], [r[1] for r in results], 'o-', linewidth=2, markersize=8)
    plt.xlabel("Number of peers (N)", fontsize=12)
    plt.ylabel("Convergence time (seconds)", fontsize=12)
    plt.title("Gossip Protocol: Convergence Time vs Network Size", fontsize=14)
    plt.grid(True, alpha=0.3)
    plt.savefig("convergence.png", dpi=150, bbox_inches='tight')
    print("plot saved to convergence.png")
    print(f"results: {results}")
else:
    print("no convergence detected")
