import argparse
import configparser
import datetime
import logging
import pickle
import time
import uuid
from pathlib import Path

from telethon import TelegramClient
from telethon.errors import (
    ChannelPrivateError,
    FloodWaitError,
    SessionPasswordNeededError,
)
from telethon.tl import types
from telethon.tl.functions.channels import GetFullChannelRequest
from tqdm import tqdm

import db_utilities

DB_NAME = "Telegram"
STOP_BEFORE_UTC = datetime.datetime(2022, 1, 1, tzinfo=datetime.timezone.utc)
STOP_BEFORE_TIMESTAMP = STOP_BEFORE_UTC.timestamp()


######################################################
# Utilities
######################################################


# Get the client from config file
def get_client(config_file="config.ini"):
    # Reading Configs
    config = configparser.ConfigParser()
    config.read(config_file)

    api_id = config["Telegram"]["api_id"]
    api_hash = config["Telegram"]["api_hash"]

    phone = config["Telegram"]["phone"]
    username = config["Telegram"]["username"]

    client = TelegramClient(username, api_id, api_hash)

    return client, phone


# return the ID type of a peer object
def get_peer_id(peer):
    if type(peer) is types.PeerChannel:
        return peer.channel_id, "channel"
    if type(peer) is types.PeerUser:
        return peer.user_id, "user"
    if type(peer) is types.PeerChat:
        return peer.chat_id, "chat"

    return [None, None]


# save preprocess docs in pickle
def save_as_pickle(text_list, outfile_name):
    with open(outfile_name, "wb") as fp:
        pickle.dump(text_list, fp)


def parse_seed_file(path):
    if not Path(path).is_file():
        raise FileNotFoundError(path)

    usernames = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                usernames.append(line.lstrip("@"))

    return usernames


def extract_media_info(message):
    if not message.media:
        return None

    if message.gif or message.sticker:
        return None

    if message.photo:
        photo = message.photo
        return {
            "title": None,
            "media_id": str(photo.id),
            "extension": ".jpg",
        }

    if message.file:
        return {
            "title": message.file.name,
            "media_id": str(message.file.id) if message.file.id else None,
            "extension": message.file.ext,
        }

    return None


######################################################


# get client
client, phone = get_client()


# Download the messages of a channel by username
async def download_content_by_name(limit, execution_uuid):
    with tqdm(
        total=None,
        desc="Channels",
        unit="ch",
        ascii=True,
    ) as channels_progress:
        while True:
            channel = db_utilities.get_next_unprocessed_channel(execution_uuid, DB_NAME)
            if channel is None:
                print("[OK] No more unprocessed channels found")
                break

            current_channel = db_utilities.get_channel(channel, execution_uuid, DB_NAME)
            current_distance = current_channel.get("_distance")
            channel_id_for_status = None
            try:
                channel_peer = await client.get_input_entity(channel)
                channel = channel_peer.channel_id
                channel_id_for_status = channel
                channel_connect = await client.get_entity(channel)
                title = channel_connect.title
                n_subscribers = channel_connect.participants_count
                creation_date = datetime.datetime.timestamp(channel_connect.date)
                description = ""
                username = ""
                is_scam = False
                verified = False

                if type(channel_connect) is types.Channel:
                    channel_full_info = None
                    try:
                        channel_full_info = await client(
                            GetFullChannelRequest(channel=channel_connect)
                        )
                    except FloodWaitError as e:
                        print("Flood waited for", e.seconds)
                    description = channel_full_info.full_chat.about
                    username = channel_full_info.chats[0].username
                    is_scam = channel_full_info.chats[0].scam
                    n_subscribers = channel_full_info.full_chat.participants_count
                    verified = channel_connect.verified

                # Pretty Print some info about the currently processed channel
                print(
                    f"\n[OK] Processing channel: {title} (ID: {channel}, Subscribers: {n_subscribers}, Created: {datetime.datetime.fromtimestamp(creation_date, tz=datetime.timezone.utc).strftime('%Y-%m-%d')}, Username: {username}, Scam: {is_scam}, Verified: {verified}, Distance: {current_distance})"
                )

                if limit is None or limit < 0:
                    effective_limit = None
                else:
                    effective_limit = limit

                with tqdm(
                    total=(
                        effective_limit
                        if effective_limit and effective_limit > 0
                        else None
                    ),
                    desc="Messages",
                    unit="msg",
                    ascii=True,
                    leave=False,
                ) as messages_progress:
                    async for message in client.iter_messages(
                        channel, limit=limit, wait_time=2
                    ):
                        message_dt = message.date
                        if message_dt.tzinfo is None:
                            message_dt = message_dt.replace(
                                tzinfo=datetime.timezone.utc
                            )
                        message_date = message_dt.timestamp()
                        if message_date < STOP_BEFORE_TIMESTAMP:
                            print(
                                "[OK] Reached stop-before date, stopping channel crawl"
                            )
                            break

                        message_author = get_peer_id(message.from_id)[0]

                        is_forwarded = False
                        forwarded_from_id = None
                        forwarded_message_date = None
                        if message.forward:
                            is_forwarded = True
                            forwarded_from_id = get_peer_id(message.forward.from_id)[0]
                            forwarded_message_date = datetime.datetime.timestamp(
                                message.forward.date
                            )

                        if forwarded_from_id:
                            new_distance = current_distance + 1
                            existing_channel = db_utilities.get_channel(
                                forwarded_from_id, execution_uuid, DB_NAME
                            )
                            existing_channel_distance = (
                                existing_channel.get("_distance")
                                if existing_channel
                                else float("inf")
                            )
                            if existing_channel is not None:
                                db_utilities.update_channel_distance(
                                    forwarded_from_id,
                                    execution_uuid,
                                    min(new_distance, existing_channel_distance),
                                    DB_NAME,
                                )
                            else:
                                db_utilities.insert_discovered_channel(
                                    forwarded_from_id,
                                    execution_uuid,
                                    DB_NAME,
                                    distance=new_distance,
                                )

                        if message.text:
                            db_utilities.insert_text_message(
                                channel,
                                execution_uuid,
                                message.id,
                                {
                                    "message": message.text,
                                    "date": message_date,
                                    "author": message_author,
                                    "is_forwarded": is_forwarded,
                                    "forwarded_from_id": forwarded_from_id,
                                    "forwarded_message_date": forwarded_message_date,
                                },
                                DB_NAME,
                            )

                        media_info = extract_media_info(message)
                        if media_info:
                            db_utilities.insert_media_item(
                                channel,
                                execution_uuid,
                                message.id,
                                {
                                    "title": media_info["title"],
                                    "date": message_date,
                                    "author": message_author,
                                    "extension": media_info["extension"],
                                    "is_forwarded": is_forwarded,
                                    "forwarded_from_id": forwarded_from_id,
                                    "media_id": media_info["media_id"],
                                    "forwarded_message_date": forwarded_message_date,
                                },
                                DB_NAME,
                            )

                        message_date_str = datetime.datetime.fromtimestamp(
                            message_date, tz=datetime.timezone.utc
                        ).strftime("%Y-%m-%d")
                        messages_progress.set_postfix({"date": message_date_str})
                        messages_progress.update(1)

                # insert all to the end
                new_ch = {
                    "_id": channel,
                    "creation_date": creation_date,
                    "username": username,
                    "title": title,
                    "description": description,
                    "scam": is_scam,
                    "n_subscribers": n_subscribers,
                    "verified": verified,
                    "execution": execution_uuid,
                    "_status": "processed",
                }
                db_utilities.insert_channel(new_ch, DB_NAME)

                time.sleep(1)

            except FloodWaitError as e:
                print("Flood waited for", e.seconds)
                if channel_id_for_status is not None:
                    db_utilities.update_channel_status(
                        channel_id_for_status,
                        execution_uuid,
                        "unprocessed",
                        DB_NAME,
                    )
            except ChannelPrivateError:
                print("Private channel")
                logging.warning("error with this channel " + str(channel))
                if channel_id_for_status is not None:
                    db_utilities.update_channel_status(
                        channel_id_for_status, execution_uuid, "skipped", DB_NAME
                    )

            channels_progress.update(1)


def format_timestamp(ts_value):
    if ts_value is None:
        return ""
    return datetime.datetime.fromtimestamp(ts_value, tz=datetime.timezone.utc).strftime(
        "%Y-%m-%d %H:%M:%S"
    )


def print_executions_table(db_name=DB_NAME):
    executions = db_utilities.list_execution_contexts(db_name)
    headers = [
        "execution_uuid",
        "status",
        "seed_file",
        "stop_before",
        "limit",
        "started_at",
        "ended_at",
    ]
    rows = []
    for doc in executions:
        rows.append(
            [
                str(doc.get("_id", "")),
                str(doc.get("status", "")),
                str(doc.get("seed_file", "")),
                str(doc.get("stop_before", "")),
                str(doc.get("limit", "")),
                format_timestamp(doc.get("started_at")),
                format_timestamp(doc.get("ended_at")),
            ]
        )

    if not rows:
        print("[OK] No executions found")
        return

    column_widths = [len(header) for header in headers]
    for row in rows:
        for idx, value in enumerate(row):
            column_widths[idx] = max(column_widths[idx], len(value))

    header_line = " | ".join(
        header.ljust(column_widths[idx]) for idx, header in enumerate(headers)
    )
    separator_line = "-+-".join("-" * width for width in column_widths)
    print(header_line)
    print(separator_line)
    for row in rows:
        print(
            " | ".join(value.ljust(column_widths[idx]) for idx, value in enumerate(row))
        )


async def main(seed_file=None, resume_execution_uuid=None, list_executions=False):
    if list_executions:
        print_executions_table(DB_NAME)
        return

    if resume_execution_uuid and seed_file:
        print("[FAIL] --resume cannot be used with --seed-file")
        return

    await client.start()
    # Ensure you're authorized
    if not await client.is_user_authorized():
        await client.send_code_request(phone)
        try:
            await client.sign_in(phone, input("Enter the code: "))
        except SessionPasswordNeededError:
            await client.sign_in(password=input("Password: "))

    # client.flood_sleep_threshold = 0  # Don't auto-sleep
    if resume_execution_uuid:
        execution_doc = db_utilities.get_execution_context(
            resume_execution_uuid, DB_NAME
        )
        if execution_doc is None:
            print(f"[FAIL] Execution not found: {resume_execution_uuid}")
            return

        execution_uuid = resume_execution_uuid
        db_utilities.update_execution_context(
            execution_uuid, {"status": "running"}, DB_NAME
        )
    else:
        execution_uuid = str(uuid.uuid4())
        started_at = int(datetime.datetime.now(datetime.timezone.utc).timestamp())
        execution_doc = {
            "_id": execution_uuid,
            "seed_file": seed_file,
            "stop_before": int(STOP_BEFORE_TIMESTAMP),
            "limit": None,
            "started_at": started_at,
            "ended_at": None,
            "status": "running",
        }
        db_utilities.insert_execution_context(execution_doc, DB_NAME)

    if seed_file:
        try:
            channels_to_find = parse_seed_file(seed_file)
        except FileNotFoundError:
            print(f"[FAIL] Seed file not found: {seed_file}")
            ended_at = int(datetime.datetime.now(datetime.timezone.utc).timestamp())
            db_utilities.update_execution_context(
                execution_uuid,
                {"ended_at": ended_at, "status": "failed"},
                DB_NAME,
            )
            return

        if not channels_to_find:
            print(f"[FAIL] Seed file is empty: {seed_file}")
            ended_at = int(datetime.datetime.now(datetime.timezone.utc).timestamp())
            db_utilities.update_execution_context(
                execution_uuid,
                {"ended_at": ended_at, "status": "failed"},
                DB_NAME,
            )
            return

        print(f"[OK] Seeding {len(channels_to_find)} channels as unprocessed")
        for channel_username in channels_to_find:
            channel_peer = await client.get_input_entity(channel_username)
            if isinstance(channel_peer, types.InputPeerChannel):
                print(
                    f"[OK] Seeding channel: {channel_username} (ID: {channel_peer.channel_id})"
                )
                db_utilities.insert_discovered_channel(
                    channel_peer.channel_id, execution_uuid, DB_NAME, distance=0
                )

    await download_content_by_name(None, execution_uuid)

    ended_at = int(datetime.datetime.now(datetime.timezone.utc).timestamp())
    db_utilities.update_execution_context(
        execution_uuid,
        {"ended_at": ended_at, "status": "completed"},
        DB_NAME,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Crawler with optional seed channel list"
    )
    parser.add_argument(
        "--seed-file",
        default=None,
        help="Path to a file containing seed channels (one per line)",
    )
    parser.add_argument(
        "--resume",
        default=None,
        help="Resume a previous execution by execution_uuid",
    )
    parser.add_argument(
        "--list-executions",
        action="store_true",
        help="List execution contexts and exit",
    )
    args = parser.parse_args()

    with client:
        client.loop.run_until_complete(
            main(args.seed_file, args.resume, args.list_executions)
        )
