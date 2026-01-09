import random
import sys


def generate_compose(n):
    """
    Generate docker-compose for gossip protocol with random bidirectional topology.
    Each peer connects to a random number of other peers (1 to n-1).
    Connections are bidirectional - if A knows B, then B knows A.
    """
    compose = "services:\n"

    # Generate random edges (bidirectional)
    edges = set()
    for i in range(n):
        # Each peer gets at least 1 connection
        others = [j for j in range(n) if j != i]
        k = random.randint(1, len(others))
        targets = random.sample(others, k)
        for j in targets:
            # Add edge in both directions (use tuple with min/max to avoid duplicates)
            edges.add((min(i, j), max(i, j)))

    # Build adjacency list from bidirectional edges
    adjacency = [[] for _ in range(n)]
    for a, b in edges:
        adjacency[a].append(b)
        adjacency[b].append(a)

    for i in range(n):
        port = 8000 + i
        neighbors = ", ".join([f'"peer{j}:{8000+j}"' for j in adjacency[i]])
        state = '"1.0"' if i == 0 else '"0.0"'

        compose += f"""  peer{i}:
    build: .
    command: ["0.0.0.0:{port}", {neighbors}, {state}]
    networks:
      - gossip_net
    ports:
      - "{port}:{port}"
"""

    compose += "networks:\n  gossip_net:\n    driver: bridge\n"
    return compose

if __name__ == "__main__":
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 5
    print(generate_compose(n))
