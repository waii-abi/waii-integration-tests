import base64
from pathlib import Path

def encode_file_to_base64(file_path: str) -> str:
    file_bytes = Path(file_path).read_bytes()
    encoded = base64.b64encode(file_bytes).decode("utf-8")
    return encoded

# Example usage:
file_path = "walmart-poc-internal-2cc1cd44593c.json"  # Replace with your secret file path
encoded_secret = encode_file_to_base64(file_path)
print(encoded_secret)