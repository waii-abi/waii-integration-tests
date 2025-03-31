import subprocess
import threading
import time


def start_docker_container(run_command, ready_message, startup_timeout, container_name="waii"):
    """
    1. Start the Docker container.
    2. Monitor the container output until the 'ready_message' is detected or the
    startup_timeout (in seconds) is reached. Returns the subprocess.Popen object.
    All output is printed to the console in real time.
    """

    cleanup_existing_container(container_name)

    proc = subprocess.Popen(
        run_command,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        bufsize=1,
        text=True
    )

    ready = False
    start_time = time.time()

    def read_output():
        nonlocal ready
        for line in iter(proc.stdout.readline, ''):
            print(line, end='', flush=True)
            if ready_message in line:
                ready = True
                break

    reader_thread = threading.Thread(target=read_output, daemon=True)
    reader_thread.start()

    # Wait until the ready message is found or the timeout is reached
    while time.time() - start_time < startup_timeout:
        if ready:
            print("Ready message detected!")
            break
        time.sleep(0.5)
    else:
        proc.terminate()
        raise TimeoutError("Docker container did not become ready within the timeout period.")

    return proc


def cleanup_existing_container(container_name):
    """
    docker stop and docker rm.
    There are cases, where previous docker is still running. Ensure to kill if possible.
    """
    try:
        # First, try to stop the container (if it's running)
        subprocess.run(["docker", "stop", container_name], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        print(f"Stopped existing container '{container_name}'.")
    except subprocess.CalledProcessError as e:
        print(f"No running container '{container_name}' to stop or error stopping it: {e}")

    try:
        # Then, remove the container forcefully
        subprocess.run(["docker", "rm", "-f", container_name], check=True, stdout=subprocess.PIPE,
                       stderr=subprocess.PIPE)
        print(f"Removed existing container '{container_name}'.")
    except subprocess.CalledProcessError as e:
        print(f"Container '{container_name}' may not exist or could not be removed: {e}")


def stop_docker_container(container_name):
    """Stop the Docker container by its name."""
    subprocess.run(["docker", "stop", container_name], check=True)
    print(f"Docker container '{container_name}' stopped.")
