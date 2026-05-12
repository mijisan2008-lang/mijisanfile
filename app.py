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

MAX_CONCURRENT_REQUESTS = 5
BATCH_SIZE = 10
REQUEST_TIMEOUT = 10
RETRY_COUNT = 2
RETRY_DELAY = 0.5

def load_accounts_from_file():
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
                    app.logger.warning(f"Line {line_num}: Invalid format. Skipping.")
                    continue
                uid, password = line.split(":", 1)
                accounts.append({"uid": uid.strip(), "password": password.strip()})
        app.logger.info(f"Loaded {len(accounts)} accounts from {ACCOUNTS_FILE}.")
    except Exception as e:
        app.logger.error(f"Error loading accounts file: {e}")
    return accounts

async def fetch_token_async(session, uid, password, semaphore):
    async with semaphore:
        for attempt in range(1 + RETRY_COUNT):
            try:
                params = {"uid": uid, "password": password}
                async with session.get(TOKEN_API_URL, params=params,
                                       timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)) as resp:
                    resp.raise_for_status()
                    data = await resp.json()

                    token = None
                    region = "BD"

                    if "token" in data and data["token"]:
                        token = data["token"]
                    elif "oauth_award" in data and isinstance(data["oauth_award"], dict):
                        token = data["oauth_award"].get("access_token")
                    elif "access_token" in data:
                        token = data["access_token"]

                    if not token:
                        app.logger.error(f"Token not found for UID {uid}.")
                        if attempt < RETRY_COUNT:
                            await asyncio.sleep(RETRY_DELAY)
                            continue
                        return None

                    if "region" in data and data["region"]:
                        region = data["region"]
                    elif "oauth_award" in data and isinstance(data["oauth_award"], dict):
                        region = data["oauth_award"].get("region", region)

                    # শুধু BD region এর token রাখব
                    if region != "BD":
                        app.logger.warning(f"UID {uid} region={region}, skipping (not BD).")
                        return None

                    return {"uid": str(uid), "token": token, "region": "BD"}

            except (aiohttp.ClientError, asyncio.TimeoutError, json.JSONDecodeError) as e:
                app.logger.error(f"Attempt {attempt+1} failed for UID {uid}: {e}")
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
            try:
                os.rename(TOKEN_FILE_BD, f"{TOKEN_FILE_BD}.backup")
            except Exception as e:
                app.logger.warning(f"Backup failed: {e}")

        with open(TOKEN_FILE_BD, "w") as f:
            json.dump(merged_data, f, indent=2)
        app.logger.info(f"{TOKEN_FILE_BD} updated with {len(merged_data)} BD entries.")
        return True
    except Exception as e:
        app.logger.error(f"Failed to update token file: {e}")
        return False

async def refresh_all_tokens_async():
    app.logger.info("Starting async token refresh process...")
    accounts = load_accounts_from_file()
    if not accounts:
        app.logger.warning("No accounts found. Token refresh aborted.")
        return

    successful = []
    failed_count = 0

    async with aiohttp.ClientSession() as session:
        sem = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
        for batch_start in range(0, len(accounts), BATCH_SIZE):
            batch = accounts[batch_start:batch_start + BATCH_SIZE]
            app.logger.info(f"Processing batch {batch_start//BATCH_SIZE + 1} ({len(batch)} accounts)")
            tasks = [fetch_token_async(session, acc["uid"], acc["password"], sem) for acc in batch]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            batch_failure = 0
            for acc, res in zip(batch, results):
                if isinstance(res, Exception) or res is None:
                    batch_failure += 1
                    failed_count += 1
                else:
                    successful.append(res)
                    app.logger.info(f"Success UID {acc['uid']} -> region {res['region']}")

            await asyncio.sleep(1.0 if batch_failure > 0 else 0.2)

    if successful:
        update_token_json(successful)
        app.logger.info(f"Refresh complete. BD tokens: {len(successful)}, Failed/Skipped: {failed_count}")
    else:
        app.logger.error("No BD tokens fetched. File not updated.")

def start_background_scheduler():
    token_ready = threading.Event()

    def run():
        asyncio.run(refresh_all_tokens_async())
        token_ready.set()
        while True:
            time.sleep(TOKEN_REFRESH_INTERVAL_HOURS * 3600)
            asyncio.run(refresh_all_tokens_async())

    t = threading.Thread(target=run, daemon=True)
    t.start()
    app.logger.info("Waiting for initial token refresh...")
    token_ready.wait(timeout=120)
    app.logger.info("Tokens ready. Server accepting requests.")

# ================= Helper functions =================

def load_tokens(server_name):
    try:
        if server_name == "IND":
            fname = "token_ind.json"
        elif server_name in {"BR", "US", "SAC", "NA"}:
            fname = "token_br.json"
        else:
            fname = "token_bd.json"

        with open(fname, "r") as f:
            tokens = json.load(f)

        if not tokens:
            app.logger.error(f"Token file {fname} is empty.")
            return None

        return tokens
    except Exception as e:
        app.logger.error(f"Token load failed: {server_name}. Error: {e}")
        return None

def encrypt_message(plaintext):
    try:
        key = b'Yg&tc%DEuh6%Zc^8'
        iv  = b'6oyZDr22E3ychjM%'
        cipher = AES.new(key, AES.MODE_CBC, iv)
        encrypted = cipher.encrypt(pad(plaintext, AES.block_size))
        return binascii.hexlify(encrypted).decode('utf-8')
    except Exception as e:
        app.logger.error(f"Encryption failed: {e}")
        return None

def create_protobuf_message(user_id, region):
    try:
        message = like_pb2.like()
        message.uid = int(user_id)
        message.region = region
        return message.SerializeToString()
    except Exception as e:
        app.logger.error(f"Protobuf (like) failed: {e}")
        return None

def create_protobuf(uid):
    try:
        message = uid_generator_pb2.uid_generator()
        message.saturn_ = int(uid)
        message.garena = 1
        return message.SerializeToString()
    except Exception as e:
        app.logger.error(f"Protobuf (uid) failed: {e}")
        return None

def enc(uid):
    protobuf_data = create_protobuf(uid)
    if protobuf_data is None:
        return None
    return encrypt_message(protobuf_data)

def decode_protobuf(binary):
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

def make_request(encrypt, server_name, token):
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
        app.logger.info(f"make_request HTTP status: {response.status_code}")

        if response.status_code != 200:
            app.logger.error(f"make_request failed: HTTP {response.status_code}, body: {response.content[:200]}")
            return None
        if not response.content:
            app.logger.error("make_request: empty response")
            return None

        decode = decode_protobuf(response.content)
        if decode is None:
            app.logger.error(f"Protobuf decode failed. Raw hex: {response.content.hex()[:100]}")
        return decode
    except Exception as e:
        app.logger.error(f"make_request exception: {e}")
        return None

async def send_request(encrypted_uid, token, url):
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
                    return response.status
                return await response.text()
    except Exception as e:
        app.logger.error(f"send_request exception: {e}")
        return None

async def send_multiple_requests(uid, server_name, url):
    try:
        protobuf_message = create_protobuf_message(uid, server_name)
        if protobuf_message is None:
            return None
        encrypted_uid = encrypt_message(protobuf_message)
        if encrypted_uid is None:
            return None
        tokens = load_tokens(server_name)
        if tokens is None:
            return None
        tasks = [send_request(encrypted_uid, tokens[i % len(tokens)]["token"], url)
                 for i in range(120)]
        return await asyncio.gather(*tasks, return_exceptions=True)
    except Exception as e:
        app.logger.error(f"send_multiple_requests exception: {e}")
        return None

# ================= Main API endpoint =================

@app.route('/like', methods=['GET'])
def handle_requests():
    uid = request.args.get("uid")
    server_name = request.args.get("server_name", "").upper()

    if not uid or not server_name:
        return jsonify({"error": "UID and server_name are required"}), 400

    try:
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

        data_before = json.loads(MessageToJson(before))
        before_like = int(data_before.get('AccountInfo', {}).get('Likes', 0) or 0)
        app.logger.info(f"Initial likes for UID {uid}: {before_like}")

        if server_name == "IND":
            like_url = "https://client.ind.freefiremobile.com/LikeProfile"
        elif server_name in {"BR", "US", "SAC", "NA"}:
            like_url = "https://client.us.freefiremobile.com/LikeProfile"
        else:
            like_url = "https://clientbp.ggpolarbear.com/LikeProfile"

        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(send_multiple_requests(uid, server_name, like_url))
        finally:
            loop.close()

        after = make_request(encrypted_uid, server_name, token)
        if after is None:
            raise Exception("Failed to retrieve player info after like requests.")

        data_after = json.loads(MessageToJson(after))
        after_like  = int(data_after.get('AccountInfo', {}).get('Likes', 0) or 0)
        player_uid  = int(data_after.get('AccountInfo', {}).get('UID', 0) or 0)
        player_name = str(data_after.get('AccountInfo', {}).get('PlayerNickname', '') or '')
        like_given  = after_like - before_like

        return jsonify({
            "LikesGivenByAPI":    like_given,
            "LikesafterCommand":  after_like,
            "LikesbeforeCommand": before_like,
            "PlayerNickname":     player_name,
            "UID":                player_uid,
            "status":             1 if like_given != 0 else 2
        })

    except Exception as e:
        app.logger.error(f"Main request processing failed: {e}")
        return jsonify({"error": str(e)}), 500

# ================= Start server =================
start_background_scheduler()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
