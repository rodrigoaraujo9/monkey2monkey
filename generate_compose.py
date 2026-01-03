import sys


def generate_compose(n):
    compose = "services:\n"
    for i in range(n):
        port = 8000 + i
        neighbors = " ".join([f'"peer{j}:{8000+j}",' for j in range(n) if j != i])
        state = '"1.0"' if i == 0 else f'"{1e-9}"'
        compose += f"""  peer{i}:
    build: .
    command: ["0.0.0.0:{port}", {neighbors} {state}]
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
