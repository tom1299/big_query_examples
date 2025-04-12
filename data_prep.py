import json
import requests
import urllib
import os


def get_token():
    token_path = os.path.join(os.path.dirname(__file__), "authentication/dataprep_token.json")
    try:
        with open(token_path, "r") as token_file:
            token_data = json.load(token_file)
            auth_token = token_data.get("dataprep_token")
            if not auth_token:
                raise ValueError("Token not found in dataprep_token.json")
            return auth_token
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"Error reading token file: {e}")
        return None


def test_dataprep_connection():
    auth_token = get_token()
    if not auth_token:
        return

    url = "https://api.clouddataprep.com/v4/open-api-spec"
    headers = {
        "Authorization": f"Bearer {auth_token}"
    }

    response = requests.get(url, headers=headers, proxies=urllib.request.getproxies())

    # Check the response
    if response.status_code == 200:
        print("Connection to Cloud Dataprep API successful.")
    else:
        print("Failed to connect to Cloud Dataprep API.")
        print("Status Code:", response.status_code)
        print("Response:", response.text)

if __name__ == "__main__":
    print("Testing connection to Cloud Dataprep API...")
    test_dataprep_connection()
