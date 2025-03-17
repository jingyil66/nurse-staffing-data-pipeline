import subprocess
import json

# Define connection details
conn_id = "aws_default"
conn_type = "aws"
access_key = ...
secret_key = ...
region_name = "us-west-2"
endpoint_url = "https://s3.amazonaws.com"
# Construct the extra JSON
extra = {
    "region_name": region_name,
    "host": endpoint_url,
}
# Convert to JSON string
extra_json = json.dumps(extra)
# Define the CLI command
command = [
    "airflow",
    "connections",
    "add",
    conn_id,
    "--conn-type",
    conn_type,
    "--conn-login",
    access_key,
    "--conn-password",
    secret_key,
    "--conn-extra",
    extra_json,
]
# Execute the command
subprocess.run(command, check=True)

def spark_connection():
    connection_id = "spark-conn"
    connection_type = "spark"
    host = "spark://192.168.0.1"
    port = "7077"
    cmd = [
        "airflow",
        "connections",
        "add",
        connection_id,
        "--conn-host",
        host,
        "--conn-type",
        connection_type,
        "--conn-port",
        port,
    ]

    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode == 0:
        print(f"Successfully added {connection_id} connection")
    else:
        print(f"Failed to add {connection_id} connection: {result.stderr}")

def add_snowflake_connection():
    connection_id = "snowflake-conn"
    connection_type = "snowflake"
    host = ...
    account = ...
    user = ...
    password = ...
    warehouse = ...
    database = ...
    schema = ...

    # Define the command for adding the Snowflake connection
    cmd = [
        "airflow",
        "connections",
        "add",
        connection_id,
        "--conn-type", connection_type,
        "--conn-login", user,
        "--conn-password", password,
        "--conn-host", host,
        "--conn-schema", schema,
        "--conn-extra", f'{{"warehouse": "{warehouse}", "database": "{database}", "account": "{account}"}}'
    ]

    # Execute the command
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode == 0:
        print(f"Successfully added {connection_id} connection to Snowflake")
    else:
        print(f"Failed to add {connection_id} connection: {result.stderr}")


add_snowflake_connection()
spark_connection()