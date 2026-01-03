import random
import subprocess
import sys


def make_topology(n):
    edges = []
    for i in range(1, n):
        edges.append((random.randint(0, i-1), i))
    for i in range(n):
        for j in range(i+1, n):
            if random.random() < 0.3:
                edges.append((i, j))
    return edges

def make_network(n):
    topology = make_topology(n)
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

    print(f"network with {n} peers created")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("usage: python run.py <N>")
        sys.exit(1)

    n = int(sys.argv[1])
    make_network(n)

    if len(sys.argv) > 2 and sys.argv[2] == "start":
        subprocess.run(["docker", "compose", "up"])
