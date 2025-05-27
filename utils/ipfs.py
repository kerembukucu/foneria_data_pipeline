import hashlib
import pandas as pd
import requests

def csv_to_web3storage(csv_path: str, keyword: str, api_token: str):
    # Read CSV and convert to bytes
    df = pd.read_csv(csv_path)
    csv_bytes = df.to_csv(index=False).encode()

    # Generate hash with keyword
    combined = keyword.encode() + csv_bytes
    hash_digest = hashlib.sha256(combined).hexdigest()

    # Prepare the file for upload
    files = {
        'file': ('user_data.csv', csv_bytes, 'text/csv')
    }

    headers = {
        'Authorization': f'Bearer {api_token}'
    }

    response = requests.post(
        'https://api.web3.storage/upload',
        files=files,
        headers=headers
    )

    if response.status_code == 200:
        cid = response.json()['cid']
        return {
            'cid': cid,
            'hash_with_keyword': hash_digest,
            'ipfs_url': f'https://{cid}.ipfs.w3s.link/user_data.csv'
        }
    else:
        raise Exception(f"Failed to upload to Web3.Storage: {response.text}")