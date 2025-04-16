import os
from pathlib import Path

# Compute absolute paths using the PROJ dir.
CUR_DIR = os.path.dirname(os.path.abspath(__file__))
PROJ_DIR = os.path.abspath(os.path.join(CUR_DIR, '..', '..'))
SANDBOX_DIR = os.path.join(PROJ_DIR, "waii-sandbox-test-integ")
PG_DIR = os.path.join(SANDBOX_DIR, "pg")
LOG_DIR = os.path.join(SANDBOX_DIR, "log")

# Place all additional docker files in the same directory, where this file is located.
SCRIPT_DIR = Path(__file__).parent.resolve()

# docker_configs.py
"""
- This file contains the Docker configurations for different setups.
- Provide the run_command, ready_message, startup_timeout
- Ensure to provide the proper base_url and api_key
"""


def get_logger_file(rel_logs_dir):
    this_file = os.path.join(PROJ_DIR, *rel_logs_dir.split("/"))
    os.makedirs(os.path.dirname(os.path.abspath(this_file)), exist_ok=True)
    return this_file

def get_pg_dir(container_name):
    this_dir = os.path.join(PG_DIR, container_name)
    os.makedirs(this_dir, exist_ok=True)
    return this_dir

def get_log_dir(container_name):
    this_dir = os.path.join(LOG_DIR, container_name)
    os.makedirs(this_dir, exist_ok=True)
    return this_dir

def get_base_url(current_config):
    api_port = str(current_config.get("api_port", 9859))
    base_url = current_config.get("base_url")
    return base_url.replace("{{port}}", api_port)


DOCKER_CONFIGS = {
    "krishna_birla_local": {
        "run_command": "echo 'Local Waii is ready!'",
        "api_port": 9859,
        "base_url": "http://localhost:{{port}}/api/",
        "ready_message": "Local Waii is ready!",
        "startup_timeout": 0,
        "api_key": ""
    },
    # ENSURE TO HAVE API_PORT DIFF FROM OTHER CONFIGS
    "waii_default": {
        "run_command": (
            "docker run --rm "
            "--env OPENAI_API_KEY=$OPENAI_API_KEY "
            "--env ENABLE_LOG_STREAMING_DOCKER=true "
            "--env LOAD_SAMPLE_DB=false "
            "-p 3000:3456 "
            "-p {{port}}:9859 "
            "-v {{pg_dir_container_name}}:/var/lib/postgresql/data:rw "
            "-v {{log_dir_container_name}}:/tmp/logs:rw "
            "--name '{{container_name}}' "
            "sandbox:latest --debug"
        ),
        "ready_message": "Waii is ready! Please visit http://localhost:3000 to start using it!",
        "startup_timeout": 120,
        "api_port": 9859,
        "base_url": "http://localhost:{{port}}/api/",
        "api_key": ""
    },
    # ENSURE TO HAVE API_PORT DIFF FROM OTHER CONFIGS
    "waii_default_ex_1": {
        "run_command": (
            "docker run --rm "
            "--env OPENAI_API_KEY=$OPENAI_API_KEY "
            "--env ENABLE_LOG_STREAMING_DOCKER=true "
            "--env LOAD_SAMPLE_DB=false "
            "-p 4000:3456 "
            "-p {{port}}:9859 "
            "-v {{pg_dir_container_name}}:/var/lib/postgresql/data:rw "
            "-v {{log_dir_container_name}}:/tmp/logs:rw "
            "--name '{{container_name}}' "
            "sandbox:latest --debug"
        ),
        "ready_message": "Waii is ready! Please visit http://localhost:3000 to start using it!",
        "startup_timeout": 120,
        "api_port": 9860,
        "base_url": "http://localhost:{{port}}/api/",
        "api_key": ""
    },
    # ENSURE TO HAVE API_PORT DIFF FROM OTHER CONFIGS
    "waii_default_ex_2": {
        "run_command": (
            "docker run --rm "
            "--env OPENAI_API_KEY=$OPENAI_API_KEY "
            "--env ENABLE_LOG_STREAMING_DOCKER=true "
            "--env LOAD_SAMPLE_DB=false "
            "-p 6000:3456 "
            "-p {{port}}:9859 "
            "-v {{pg_dir_container_name}}:/var/lib/postgresql/data:rw "
            "-v {{log_dir_container_name}}:/tmp/logs:rw "
            "--name '{{container_name}}' "
            "sandbox:latest --debug"
        ),
        "ready_message": "Waii is ready! Please visit http://localhost:3000 to start using it!",
        "startup_timeout": 120,
        "api_port": 9861,
        "base_url": "http://localhost:{{port}}/api/",
        "api_key": ""
    },
    # ENSURE TO HAVE API_PORT DIFF FROM OTHER CONFIGS
    "waii_default_postgres": {
        "run_command": (
            "docker run --rm "
            "--env OPENAI_API_KEY=$OPENAI_API_KEY "
            "--env ENABLE_LOG_STREAMING_DOCKER=true "
            "--env LOAD_SAMPLE_DB=false "
            "-p 6001:3456 "
            "-p {{port}}:9859 "
            "-v {{pg_dir_container_name}}:/var/lib/postgresql/data:rw "
            "-v {{log_dir_container_name}}:/tmp/logs:rw "
            "--name '{{container_name}}' "
            "sandbox:latest --debug"
        ),
        "ready_message": "Waii is ready! Please visit http://localhost:3000 to start using it!",
        "startup_timeout": 120,
        "api_port": 9862,
        "base_url": "http://localhost:{{port}}/api/",
        "api_key": ""
    }
}

if __name__ == "__main__":
    for k, v in DOCKER_CONFIGS.items():
        tes = ((v['run_command'].replace("{{pg_dir_container_name}}", get_pg_dir(k))
                .replace("{{log_dir_container_name}}", get_log_dir(k)))
               .replace("{{port}}", "5030")
               .replace("{{container_name}}", k))
        print(tes)
