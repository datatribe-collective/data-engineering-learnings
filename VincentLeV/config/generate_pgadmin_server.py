import os
import json

def load_env_file(env_file=".env"):
    if os.path.exists(env_file):
        with open(env_file, "r") as f:
            for line in f:
                if line.strip() and not line.startswith("#"):
                    key, value = line.strip().split("=", 1)
                    os.environ[key] = value

load_env_file()

name = os.getenv("PGADMIN_SERVER_NAME", "banned_books_db")
host = os.getenv("PGADMIN_SERVER_HOST", "postgres")
port = os.getenv("POSTGRES_PORT", 5432)
username = os.getenv("POSTGRES_USER", "airflow")
password = os.getenv("POSTGRES_PASSWORD", "airflow")
db = os.getenv("POSTGRES_DB", "postgres")

servers = {
    "Servers": {
        "1": {
            "Name": name,
            "Group": "Servers",
            "Host": host,
            "Port": int(port),
            "MaintenanceDB": db,
            "Username": username,
            "Password": password,
            "SSLMode": "prefer",
            "ConnectNow": True
        }
    }
}

servers_file = os.path.join("config", "servers.json")

if os.path.exists(servers_file):
    os.remove(servers_file)
    print(f"{servers_file} deleted.")

with open(servers_file, "w") as f:
    json.dump(servers, f, indent=2)

print("servers.json generated successfully!")