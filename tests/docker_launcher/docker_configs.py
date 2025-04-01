import os

# Compute absolute paths using the HOME environment variable.
HOME_DIR = os.environ.get("HOME")
SANDBOX_DIR = os.path.join(HOME_DIR, "waii-sandbox-test-integ")
PG_DIR = os.path.join(SANDBOX_DIR, "pg")
LOG_DIR = os.path.join(SANDBOX_DIR, "log")

# docker_configs.py
"""
- This file contains the Docker configurations for different setups.
- Provide the run_command, ready_message, startup_timeout
- Ensure to provide the proper base_url and api_key
"""

DOCKER_CONFIGS = {
    "waii_default": {
        "run_command": (
            "docker run --rm "
            "--env OPENAI_API_KEY=$OPENAI_API_KEY "
            "--env ENABLE_LOG_STREAMING_DOCKER=true "
            "--env LOAD_SAMPLE_DB=false "
            "-p 3000:3456 "
            "-p 9859:9859 "
            f"-v {os.path.join(PG_DIR, 'waii_default')}:/var/lib/postgresql/data:rw " 
            f"-v {os.path.join(LOG_DIR, 'waii_default')}:/tmp/logs:rw " 
            "--name waii_default "
            "sandbox:latest --debug"
        ),
        "ready_message": "Waii is ready! Please visit http://localhost:3000 to start using it!", # Don't change this, as it is message in docker
        "startup_timeout": 120,
        "base_url": "http://localhost:9859/api/",  # Embedded base URL (the host port)
        "api_key": ""
    },
    "waii_default_ex_1": {
        "run_command": (
            "docker run --rm "
            "--env OPENAI_API_KEY=$OPENAI_API_KEY "
            "--env ENABLE_LOG_STREAMING_DOCKER=true "
            "--env LOAD_SAMPLE_DB=false "
            "-p 4000:3456 "
            "-p 9959:9859 "
            f"-v {os.path.join(PG_DIR, 'waii_default_ex_1')}:/var/lib/postgresql/data:rw " 
            f"-v {os.path.join(LOG_DIR, 'waii_default_ex_1')}:/tmp/logs:rw "
            "--name waii_default_ex_1 "
            "sandbox:latest --debug"
        ),
        "ready_message": "Waii is ready! Please visit http://localhost:3000 to start using it!", # Don't change this, as it is message in docker
        "startup_timeout": 120,
        "base_url": "http://localhost:9959/api/",
        "api_key": ""
    },
    "waii_default_ex_2": {
        "run_command": (
            "docker run --rm "
            "--env OPENAI_API_KEY=$OPENAI_API_KEY "
            "--env ENABLE_LOG_STREAMING_DOCKER=true "
            "--env LOAD_SAMPLE_DB=false "
            "-p 6000:3456 "
            "-p 10059:9859 "
            f"-v {os.path.join(PG_DIR, 'waii_default_ex_2')}:/var/lib/postgresql/data:rw " 
            f"-v {os.path.join(LOG_DIR, 'waii_default_ex_2')}:/tmp/logs:rw "
            "--name waii_default_ex_2 "
            "sandbox:latest --debug"
        ),
        "ready_message": "Waii is ready! Please visit http://localhost:3000 to start using it!", # Don't change this, as it is message in docker
        "startup_timeout": 120,
        "base_url": "http://localhost:10059/api/",
        "api_key": ""
    }
}
