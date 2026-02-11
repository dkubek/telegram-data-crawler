from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError
from telethon.tl.functions.channels import GetFullChannelRequest
from telethon.errors import FloodWaitError, ChannelPrivateError
from telethon.tl import types

import asyncio
import argparse
import time
import datetime
import logging
import pickle
from pathlib import Path

from tqdm import tqdm
import configparser

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
async def download_content_by_name(channels, limit):

    # Start from the last checkpoint
    print("Number of channels to search: ", len(channels))

    with tqdm(
        total=len(channels),
        desc="Channels",
        unit="ch",
        ascii=True,
    ) as channels_progress:
        for channel in channels:
            try:
                channel_peer = await client.get_input_entity(channel)
                channel = channel_peer.channel_id
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

                media = {}
                messages = {}

                if limit is None or limit == -1:
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
                    async for message in client.iter_messages(channel, limit=limit, wait_time=2):
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

                        if message.text:
                            messages[message.id] = {
                                "message": message.text,
                                "date": message_date,
                                "author": message_author,
                                "is_forwarded": is_forwarded,
                                "forwarded_from_id": forwarded_from_id,
                                "forwarded_message_date": forwarded_message_date,
                            }


                        media_info = extract_media_info(message)
                        if media_info:
                            media[message.id] = {
                                "title": media_info["title"],
                                "date": message_date,
                                "author": message_author,
                                "extension": media_info["extension"],
                                "is_forwarded": is_forwarded,
                                "forwarded_from_id": forwarded_from_id,
                                "media_id": media_info["media_id"],
                                "forwarded_message_date": forwarded_message_date,
                            }

                        messages_progress.update(1)

                # insert all to the end
                new_ch = {
                    "_id": channel,
                    "creation_date": creation_date,
                    "username": username,
                    "title": title,
                    "description": description,
                    "scam": is_scam,
                    "text_messages": messages,
                    "generic_media": media,
                    "n_subscribers": n_subscribers,
                    "verified": verified,
                }
                db_utilities.insert_channel(new_ch, DB_NAME)

                time.sleep(1)

            except FloodWaitError as e:
                print("Flood waited for", e.seconds)
            except ChannelPrivateError:
                print("Private channel")
                logging.warning("error with this channel " + str(channel))

            channels_progress.update(1)


async def main(seed_file=None):
    await client.start()
    # Ensure you're authorized
    if not await client.is_user_authorized():
        await client.send_code_request(phone)
        try:
            await client.sign_in(phone, input("Enter the code: "))
        except SessionPasswordNeededError:
            await client.sign_in(password=input("Password: "))

    # client.flood_sleep_threshold = 0  # Don't auto-sleep
    if seed_file:
        try:
            channels_to_find = parse_seed_file(seed_file)
        except FileNotFoundError:
            print(f"[FAIL] Seed file not found: {seed_file}")
            return

        if not channels_to_find:
            print(f"[FAIL] Seed file is empty: {seed_file}")
            return
    else:
        channels_to_find = db_utilities.get_other_channels_references(DB_NAME)

    while channels_to_find.__len__() > 0:
        save_as_pickle(channels_to_find, "channels_to_find")
        await download_content_by_name(channels_to_find, None)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Crawler (old) with optional seed channel list"
    )
    parser.add_argument(
        "--seed-file",
        default=None,
        help="Path to a file containing seed channels (one per line)",
    )
    args = parser.parse_args()

    with client:
        client.loop.run_until_complete(main(args.seed_file))
