import subprocess
import time

def wait_for_postgres(host, max_retries=5, delay_second=5):
    retries = 0
    while retries < max_retries:
        try:
            result = subprocess.run(
                ["pg_isready", "-h", host],
                check=True,
                capture_output=True,
                text=True
            )
            if "accepting connections" in result.stdout:
                print(f"Successfully connected to {host}")
                return True
        except subprocess.CalledProcessError as e:
            print(f"Error connecting to {host}: {e}")
            retries += 1
            print(f"Retrying in {delay_second} seconds... (Attempt {retries}/{max_retries})")
            time.sleep(delay_second)
    print(f"Max retries reached for {host}. Exiting")
    return False


if not wait_for_postgres(host="source_postgres"):
    exit(1)

if not wait_for_postgres(host="destination_postgres"):
    exit(1)

print("Starting ELT script...")

source_config = {
    'dbname': 'source_db',
    'user': 'postgres',
    'password': 'secret',
    'host': 'source_postgres'
}

destination_config = {
    'dbname': 'destination_db',
    'user': 'postgres',
    'password': 'secret',
    'host': 'destination_postgres'
}

# Dump from source into a file
with open("data_dump.sql", "w") as dump_file:
    subprocess.run([
        'pg_dump',
        '-h', source_config['host'],
        '-U', source_config['user'],
        '-d', source_config['dbname']
    ], env=dict(PGPASSWORD=source_config['password']), check=True, stdout=dump_file)

# Load into destination
with open("data_dump.sql", "r") as dump_file:
    subprocess.run([
        'psql',
        '-h', destination_config['host'],
        '-U', destination_config['user'],
        '-d', destination_config['dbname']
    ], env=dict(PGPASSWORD=destination_config['password']), check=True, stdin=dump_file)


print("Ending ELT Script...")
