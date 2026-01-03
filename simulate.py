import re
import subprocess
import time

import matplotlib.pyplot as plt

from run import make_network


def run_simulation(n, max_duration=180):
    print(f"\n{'='*60}")
    print(f"testing N={n} peers")
    print(f"target: 1/{n} = {1.0/n:.6f}")
    print(f"{'='*60}\n")

    make_network(n)

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
    recent = []
    converged = False

    try:
        while time.time() - start < max_duration:
            line = process.stdout.readline()
            if line:
                print(line, end='')

                match = re.search(r'-> ([\d.]+)', line)
                if match:
                    value = float(match.group(1))
                    recent.append(value)

                    if len(recent) > 20:
                        recent.pop(0)

                    if len(recent) >= 10:
                        if all(abs(v - target) < tolerance for v in recent[-10:]):
                            converged = True
                            break

            time.sleep(0.01)
    except KeyboardInterrupt:
        pass
    finally:
        process.terminate()
        process.wait()

    elapsed = time.time() - start

    print(f"\n\nstopping...")
    subprocess.run(["docker", "compose", "down", "-v"],
                  stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    if converged:
        print(f"converged in {elapsed:.1f}s\n")
        return elapsed
    else:
        print(f"timeout after {elapsed:.1f}s\n")
        return None

results = []
for n in [3, 5, 7, 10, 15]:
    t = run_simulation(n, max_duration=180)
    if t:
        results.append((n, t))
    time.sleep(2)

if results:
    print(f"\n{'='*60}")
    print("generating plot...")
    print(f"{'='*60}\n")

    plt.figure(figsize=(10, 6))
    plt.plot([r[0] for r in results], [r[1] for r in results], 'o-', linewidth=2, markersize=8)
    plt.xlabel("number of peers (N)", fontsize=12)
    plt.ylabel("convergence time (s)", fontsize=12)
    plt.title("gossip convergence time vs network size", fontsize=14)
    plt.grid(True, alpha=0.3)
    plt.savefig("convergence.png", dpi=150, bbox_inches='tight')
    print("saved convergence.png")
    print(f"results: {results}")
else:
    print("no convergence")
