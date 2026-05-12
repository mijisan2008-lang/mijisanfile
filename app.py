from flask import Flask, request, jsonify
import asyncio
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad
from google.protobuf.json_format import MessageToJson
import binascii
import aiohttp
import requests
import json
import like_pb2
import like_count_pb2
import uid_generator_pb2
from google.protobuf.message import DecodeError
import logging
import warnings
from urllib3.exceptions import InsecureRequestWarning
import os
import threading
import time
from datetime import datetime, timedelta

warnings.simplefilter('ignore', InsecureRequestWarning)

app = Flask(__name__)
app.logger.setLevel(logging.INFO)

# ================= Token auto-refresh configuration =================
ACCOUNTS_FILE = "accounts.txt"
TOKEN_FILE_BD = "token_bd.json"
TOKEN_REFRESH_INTERVAL_HOURS = 2
TOKEN_API_URL = "https://jwt-api-ivory.vercel.app/api/token"

# Concurrency / batching settings
MAX_CONCURRENT_REQUESTS = 5      # never more than 5 simultaneous token fetches
BATCH_SIZE = 10                  # process up to 10 accounts per batch
REQUEST_TIMEOUT = 10             # seconds (reduced from 30)
RETRY_COUNT = 2                  # retries on failure
RETRY_DELAY = 0.5                # seconds between retries

def load_accounts_from_file():
    """Read uid:password pairs from accounts.txt."""
    accounts = []
    try:
        if not os.path.exists(ACCOUNTS_FILE):
            app.logger.error(f"Accounts file {ACCOUNTS_FILE} not found.")
            return accounts
        with open(ACCOUNTS_FILE, "r") as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if ":" not in line:
                    app.logger.warning(f"Line {line_num}: Invalid format (expected uid:password). Skipping.")
                    continue
                uid, password = line.split(":", 1)
                accounts.append({"uid": uid.strip(), "password": password.strip()})
        app.logger.info(f"Loaded {len(accounts)} accounts from {ACCOUNTS_FILE}.")
    except Exception as e:
        app.logger.error(f"Error loading accounts file: {e}")
    return accounts

async def fetch_token_async(session, uid, password, semaphore):
    """
    Async token fetch with retries, semaphore, and dynamic success/failure handling.
    Returns dict with uid, token, region or None.
    """
    # Use the semaphore to limit overall concurrency
    async with semaphore:
        for attempt in range(1 + RETRY_COUNT):  # original + retries
            try:
                params = {"uid": uid, "password": password}
                async with session.get(TOKEN_API_URL, params=params,
                                       timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)) as resp:
                    resp.raise_for_status()
                    data = await resp.json()
                    app.logger.debug(f"API response for UID {uid}: {data}")

                    token = None
                    region = "BD"

                    # Extract token – supports multiple response formats
                    if "token" in data and data["token"]:
                        token = data["token"]
                    elif "oauth_award" in data and isinstance(data["oauth_award"], dict):
                        token = data["oauth_award"].get("access_token")
                    elif "access_token" in data:
                        token = data["access_token"]

                    if not token:
                        app.logger.error(f"Token not found for UID {uid}. Response: {data}")
                        if attempt < RETRY_COUNT:
                            await asyncio.sleep(RETRY_DELAY)
                            continue
                        return None

                    # Extract region
                    if "region" in data and data["region"]:
                        region = data["region"]
                    elif "oauth_award" in data and isinstance(data["oauth_award"], dict):
                        region = data["oauth_award"].get("region", region)

                    # Success: we'll signal it back to the caller for dynamic batching delay
                    return {"uid": str(uid), "token": token, "region": region}

            except (aiohttp.ClientError, asyncio.TimeoutError, json.JSONDecodeError) as e:
                app.logger.error(f"Attempt {attempt+1}/{1+RETRY_COUNT} failed for UID {uid}: {e}")
                if attempt < RETRY_COUNT:
                    await asyncio.sleep(RETRY_DELAY)
                else:
                    return None
            except Exception as e:
                app.logger.error(f"Unexpected error for UID {uid}: {e}")
                if attempt < RETRY_COUNT:
                    await asyncio.sleep(RETRY_DELAY)
                else:
                    return None
        return None

def update_token_json(new_accounts_data):
    """Update token_bd.json, merging with existing data. (unchanged)"""
    try:
        existing_data = []
        if os.path.exists(TOKEN_FILE_BD):
            with open(TOKEN_FILE_BD, "r") as f:
                try:
                    existing_data = json.load(f)
                    if not isinstance(existing_data, list):
                        existing_data = []
                except json.JSONDecodeError:
                    existing_data = []
        uid_to_existing = {item["uid"]: item for item in existing_data}
        for new_item in new_accounts_data:
            uid_to_existing[new_item["uid"]] = new_item
        merged_data = list(uid_to_existing.values())

        if os.path.exists(TOKEN_FILE_BD):
            backup_file = f"{TOKEN_FILE_BD}.backup"
            try:
                os.rename(TOKEN_FILE_BD, backup_file)
                app.logger.info(f"Backup created: {backup_file}")
            except Exception as e:
                app.logger.warning(f"Failed to create backup: {e}")

        with open(TOKEN_FILE_BD, "w") as f:
            json.dump(merged_data, f, indent=2)
        app.logger.info(f"{TOKEN_FILE_BD} updated with {len(merged_data)} entries.")
        return True
    except Exception as e:
        app.logger.error(f"Failed to update token file: {e}")
        return False

async def refresh_all_tokens_async():
    """
    Async token refresh: fetches tokens in batches with limited concurrency,
    dynamic delays, retries, and connection reuse.
    """
    app.logger.info("Starting async token refresh process...")
    accounts = load_accounts_from_file()
    if not accounts:
        app.logger.warning("No accounts found. Token refresh aborted.")
        return

    successful = []
    failed_count = 0

    # Use a single aiohttp session for all requests (connection reuse)
    async with aiohttp.ClientSession() as session:
        # Semaphore to enforce max concurrent requests
        sem = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

        # Process in batches
        for batch_start in range(0, len(accounts), BATCH_SIZE):
            batch = accounts[batch_start:batch_start + BATCH_SIZE]
            app.logger.info(f"Processing batch {batch_start//BATCH_SIZE + 1} "
                            f"({len(batch)} accounts)")

            # Create tasks for this batch
            tasks = [fetch_token_async(session, acc["uid"], acc["password"], sem)
                     for acc in batch]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            batch_success = 0
            batch_failure = 0

            for acc, res in zip(batch, results):
                uid = acc["uid"]
                if isinstance(res, Exception):
                    app.logger.error(f"Unexpected exception for UID {uid}: {res}")
                    batch_failure += 1
                    failed_count += 1
                elif res is None:
                    app.logger.error(f"Failed to fetch token for UID {uid}")
                    batch_failure += 1
                    failed_count += 1
                else:
                    successful.append(res)
                    batch_success += 1
                    app.logger.info(f"Success UID {uid} -> region {res['region']}")

            # Dynamic delay between batches based on the batch outcome
            if batch_failure > 0:
                app.logger.info(f"Batch had failures – waiting 1.0s before next batch")
                await asyncio.sleep(1.0)
            else:
                app.logger.info(f"Batch fully successful – waiting 0.2s before next batch")
                await asyncio.sleep(0.2)

    # Write collected tokens to file
    if successful:
        update_token_json(successful)
        app.logger.info(f"Refresh complete. Success: {len(successful)}, Failed: {failed_count}")
    else:
        app.logger.error("No tokens were successfully fetched. File not updated.")

def scheduled_token_refresh():
    """Background scheduler loop – runs async token refresh on a dedicated event loop."""
    while True:
        next_run = datetime.now() + timedelta(hours=TOKEN_REFRESH_INTERVAL_HOURS)
        app.logger.info(f"Next token refresh scheduled at: {next_run.strftime('%Y-%m-%d %H:%M:%S')}")
        asyncio.run(refresh_all_tokens_async())
        time.sleep(TOKEN_REFRESH_INTERVAL_HOURS * 3600)

def start_background_scheduler():
    """Start the scheduler thread."""
    scheduler_thread = threading.Thread(target=scheduled_token_refresh, daemon=True)
    scheduler_thread.start()
    app.logger.info("Token refresh scheduler started in background.")

# ================= Helper functions (unchanged) =================

def load_tokens(server_name):
    """Load tokens based on server region."""
    try:
        if server_name == "IND":
            with open("token_ind.json", "r") as f:
                tokens = json.load(f)
        elif server_name in {"BR", "US", "SAC", "NA"}:
            with open("token_br.json", "r") as f:
                tokens = json.load(f)
        else:
            with open("token_bd.json", "r") as f:
                tokens = json.load(f)
        return tokens
    except Exception as e:
        app.logger.error(f"Token load failed: {server_name}. Error: {e}") 
        return None

def encrypt_message(plaintext):
    """AES-CBC encryption."""
    try:
        key = b'Yg&tc%DEuh6%Zc^8'
        iv = b'6oyZDr22E3ychjM%'
        cipher = AES.new(key, AES.MODE_CBC, iv)
        padded_message = pad(plaintext, AES.block_size)
        encrypted_message = cipher.encrypt(padded_message)
        return binascii.hexlify(encrypted_message).decode('utf-8')
    except Exception as e:
        app.logger.error(f"Encryption failed. Error: {e}")
        return None

def create_protobuf_message(user_id, region):
    """Create like protobuf."""
    try:
        message = like_pb2.like()
        message.uid = int(user_id)
        message.region = region
        return message.SerializeToString()
    except Exception as e:
        app.logger.error(f"Protobuf creation (like) failed. Error: {e}")
        return None

async def send_request(encrypted_uid, token, url):
    """Async POST request."""
    try:
        edata = bytes.fromhex(encrypted_uid)
        headers = {
            'User-Agent': "Dalvik/2.1.0 (Linux; U; Android 9; ASUS_Z01QD Build/PI)",
            'Connection': "Keep-Alive",
            'Accept-Encoding': "gzip",
            'Authorization': f"Bearer {token}",
            'Content-Type': "application/x-www-form-urlencoded",
            'Expect': "100-continue",
            'X-Unity-Version': "2018.4.11f1",
            'X-GA': "v1 1",
            'ReleaseVersion': "OB53"
        }
        async with aiohttp.ClientSession() as session:
            async with session.post(url, data=edata, headers=headers) as response:
                if response.status != 200:
                    app.logger.error(f"Request failed: Status {response.status}") 
                    return response.status
                return await response.text()
    except Exception as e:
        app.logger.error(f"send_request exception: {e}")
        return None

async def send_multiple_requests(uid, server_name, url):
    """Send 120 concurrent like requests."""
    try:
        region = server_name
        protobuf_message = create_protobuf_message(uid, region)
        if protobuf_message is None:
            app.logger.error("Like protobuf failed.")
            return None
        encrypted_uid = encrypt_message(protobuf_message)
        if encrypted_uid is None:
            app.logger.error("Like encryption failed.")
            return None
        tokens = load_tokens(server_name)
        if tokens is None:
            app.logger.error("Token load failed in multi-send.")
            return None
        tasks = []
        for i in range(120):
            token = tokens[i % len(tokens)]["token"]
            tasks.append(send_request(encrypted_uid, token, url))
        results = await asyncio.gather(*tasks, return_exceptions=True)
        return results
    except Exception as e:
        app.logger.error(f"send_multiple_requests exception: {e}")
        return None

def create_protobuf(uid):
    """Create UID generator protobuf."""
    try:
        message = uid_generator_pb2.uid_generator()
        message.saturn_ = int(uid)
        message.garena = 1
        return message.SerializeToString()
    except Exception as e:
        app.logger.error(f"Protobuf creation (uid) failed. Error: {e}")
        return None

def enc(uid):
    """Encrypt UID protobuf."""
    protobuf_data = create_protobuf(uid)
    if protobuf_data is None:
        return None
    return encrypt_message(protobuf_data)

def make_request(encrypt, server_name, token):
    """Get player info (before/after likes)."""
    try:
        if server_name == "IND":
            base_url = "https://client.ind.freefiremobile.com"
        elif server_name in {"BR", "US", "SAC", "NA"}:
            base_url = "https://client.us.freefiremobile.com"
        else:
            base_url = "https://clientbp.ggpolarbear.com"
        
        url = f"{base_url}/GetPlayerPersonalShow"
        edata = bytes.fromhex(encrypt)
        headers = {
            'User-Agent': "Dalvik/2.1.0 (Linux; U; Android 9; ASUS_Z01QD Build/PI)",
            'Connection': "Keep-Alive",
            'Accept-Encoding': "gzip",
            'Authorization': f"Bearer {token}",
            'Content-Type': "application/x-www-form-urlencoded",
            'Expect': "100-continue",
            'X-Unity-Version': "2018.4.11f1",
            'X-GA': "v1 1",
            'ReleaseVersion': "OB53"
        }
        response = requests.post(url, data=edata, headers=headers, verify=False, timeout=30)
        hex_data = response.content.hex()
        binary = bytes.fromhex(hex_data)
        decode = decode_protobuf(binary)
        if decode is None:
            app.logger.error("Protobuf decode failed in make_request.")
        return decode
    except Exception as e:
        app.logger.error(f"make_request exception: {e}")
        return None

def decode_protobuf(binary):
    """Decode like count protobuf."""
    try:
        items = like_count_pb2.Info()
        items.ParseFromString(binary)
        return items
    except DecodeError as e:
        app.logger.error(f"DecodeError: {e}")
        return None
    except Exception as e:
        app.logger.error(f"Decode failed: {e}")
        return None

# ================= Main API endpoint =================

@app.route('/like', methods=['GET'])
def handle_requests():
    uid = request.args.get("uid")
    server_name = request.args.get("server_name", "").upper()
    if not uid or not server_name:
        return jsonify({"error": "UID and server_name are required"}), 400

    try:
        def process_request():
            tokens = load_tokens(server_name)
            if tokens is None:
                raise Exception("Failed to load tokens.")
            token = tokens[0]['token']
            encrypted_uid = enc(uid)
            if encrypted_uid is None:
                raise Exception("Encryption of UID failed.")

            before = make_request(encrypted_uid, server_name, token)
            if before is None:
                raise Exception("Failed to retrieve initial player info.")
            try:
                jsone = MessageToJson(before)
            except Exception as e:
                raise Exception(f"'before' proto to JSON failed: {e}")
            data_before = json.loads(jsone)
            before_like = data_before.get('AccountInfo', {}).get('Likes', 0)
            try:
                before_like = int(before_like)
            except Exception:
                before_like = 0
            app.logger.info(f"Initial likes for UID {uid}: {before_like}")

            if server_name == "IND":
                like_url = "https://client.ind.freefiremobile.com/LikeProfile"
            elif server_name in {"BR", "US", "SAC", "NA"}:
                like_url = "https://client.us.freefiremobile.com/LikeProfile"
            else:
                like_url = "https://clientbp.ggpolarbear.com/LikeProfile"

            asyncio.run(send_multiple_requests(uid, server_name, like_url))

            after = make_request(encrypted_uid, server_name, token)
            if after is None:
                raise Exception("Failed to retrieve player info after like requests.")
            try:
                jsone_after = MessageToJson(after)
            except Exception as e:
                raise Exception(f"'after' proto to JSON failed: {e}")
            data_after = json.loads(jsone_after)
            after_like = int(data_after.get('AccountInfo', {}).get('Likes', 0))
            player_uid = int(data_after.get('AccountInfo', {}).get('UID', 0))
            player_name = str(data_after.get('AccountInfo', {}).get('PlayerNickname', ''))
            like_given = after_like - before_like
            status = 1 if like_given != 0 else 2
            result = {
                "LikesGivenByAPI": like_given,
                "LikesafterCommand": after_like,
                "LikesbeforeCommand": before_like,
                "PlayerNickname": player_name,
                "UID": player_uid,
                "status": status
            }
            return result

        result = process_request()
        return jsonify(result)
    except Exception as e:
        app.logger.error(f"Main request processing failed: {e}")
        return jsonify({"error": str(e)}), 500

# ================= Start server =================
if __name__ == "__main__":
    start_background_scheduler()
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)