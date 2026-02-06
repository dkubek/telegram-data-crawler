#!/usr/bin/env python3
"""
Telegram Channel Crawler: Snowball sampling with network tracking.

Downloads channel messages and discovers new channels via forwarded messages.
Tracks network structure with degree-of-separation from seed channels.

Usage:
    uv run python crawler.py [--max-degree N] [--since DATE] [--until DATE] [--db-name NAME]
"""

import argparse
import asyncio
import configparser
import datetime
import logging
import re
from dataclasses import dataclass

from tqdm import tqdm

from telethon import TelegramClient
from telethon.errors import (
    FloodWaitError,
    ChannelPrivateError,
    SessionPasswordNeededError,
    UsernameNotOccupiedError,
    UsernameInvalidError,
)
from telethon.tl.functions.channels import GetFullChannelRequest
from telethon.tl import types
from telethon.tl.types import (
    MessageEntityMention,
    MessageEntityMentionName,
    MessageEntityTextUrl,
    MessageEntityUrl,
)

import db_utilities

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

TELETHON_RETRY_COUNT = 3
DEFAULT_MESSAGE_LIMIT = 10000
BATCH_ENTITY_RESOLVE_SIZE = 30


# Data class for complete extraction result from a channel
@dataclass
class ChannelExtraction:
    """Complete extraction result from processing a single channel."""
    messages: dict  # Text messages by message ID
    media: dict  # Media metadata by message ID
    discovered_channels: set  # Channel IDs found in forwarded messages
    mentioned_usernames: set  # Usernames from @mentions and URLs (need resolution)
    known_user_ids: set  # User IDs from MessageEntityMentionName (already resolved, no API call needed)


# ----------------------------------------------------------------------
# Configuration
# ----------------------------------------------------------------------


def get_client(config_file="config.ini"):
    """Load Telegram client from config file."""
    config = configparser.ConfigParser()
    config.read(config_file)

    api_id = int(config["Telegram"]["api_id"])
    api_hash = config["Telegram"]["api_hash"]
    phone = config["Telegram"]["phone"]
    username = config["Telegram"]["username"]

    client = TelegramClient(username, api_id, api_hash)
    return client, phone


def parse_date(date_str):
    """Parse date string to timezone-aware UTC datetime."""
    if date_str is None:
        return None
    dt = datetime.datetime.strptime(date_str, "%Y-%m-%d")
    return dt.replace(tzinfo=datetime.timezone.utc)


async def telethon_call_with_retry(call, *, label, max_retries=TELETHON_RETRY_COUNT):
    """Retry a Telethon call on FloodWaitError."""
    for attempt in range(max_retries):
        try:
            return await call()
        except FloodWaitError as e:
            log.warning("FloodWait %ds during %s", e.seconds, label)
            await asyncio.sleep(e.seconds)
            if attempt < max_retries - 1:
                continue
            raise


async def batch_resolve_entities(
    client, usernames, max_retries=TELETHON_RETRY_COUNT
):
    """
    Batch resolve a list of usernames to entities.
    Returns dict mapping username -> entity object (or None if failed).
    """
    if not usernames:
        return {}

    username_list = list(usernames)
    result_map = {}

    # Batch in chunks to avoid overwhelming a single API call
    for i in range(0, len(username_list), BATCH_ENTITY_RESOLVE_SIZE):
        batch = username_list[i : i + BATCH_ENTITY_RESOLVE_SIZE]
        try:
            entities = await telethon_call_with_retry(
                lambda b=batch: client.get_entity(b),
                label=f"batch_resolve {len(batch)} entities",
                max_retries=max_retries,
            )
            # Ensure we always get a list
            if not isinstance(entities, list):
                entities = [entities]
            for entity in entities:
                username_resolved = getattr(entity, "username", None)
                if username_resolved:
                    result_map[username_resolved.lower()] = entity
        except (UsernameNotOccupiedError, UsernameInvalidError, ValueError) as e:
            log.debug("Batch resolve error for %s: %s", batch, e)
            # Try individually to at least get the ones that do exist
            for username in batch:
                try:
                    entity = await telethon_call_with_retry(
                        lambda u=username: client.get_entity(u),
                        label=f"resolve {username}",
                        max_retries=1,
                    )
                    result_map[username.lower()] = entity
                except (UsernameNotOccupiedError, UsernameInvalidError, ValueError):
                    log.debug("Entity not found: %s", username)

    return result_map


# ----------------------------------------------------------------------
# Peer ID extraction
# ----------------------------------------------------------------------


def get_peer_id(peer):
    """Extract ID and type from a peer object."""
    if isinstance(peer, types.PeerChannel):
        return peer.channel_id, "channel"
    if isinstance(peer, types.PeerUser):
        return peer.user_id, "user"
    if isinstance(peer, types.PeerChat):
        return peer.chat_id, "chat"
    return None, None


# ----------------------------------------------------------------------
# Channel mention extraction from message entities
# ----------------------------------------------------------------------

# Regex to extract username from t.me URLs
TELEGRAM_URL_PATTERN = re.compile(
    r"(?:https?://)?(?:t\.me|telegram\.me)/(?:joinchat/)?([a-zA-Z][a-zA-Z0-9_]{3,30}[a-zA-Z0-9])"
)


def extract_mentioned_usernames(message):
    """
    Extract channel/user mentions from message entities.

    Returns:
        (usernames_to_resolve, known_user_ids) tuple where:
        - usernames_to_resolve: set of usernames (without @) that need resolution
        - known_user_ids: set of user IDs already embedded in MessageEntityMentionName
    
    The known_user_ids are extracted from MessageEntityMentionName entities,
    which contain embedded user_id fields (no API call needed).
    """
    usernames = set()
    known_ids = set()

    if not message.entities:
        return usernames, known_ids

    text = message.text or ""

    for entity in message.entities:
        # Direct mention with embedded user_id (no resolution needed!)
        if isinstance(entity, MessageEntityMentionName):
            # This entity type has a user_id field that's already resolved
            if hasattr(entity, 'user_id') and entity.user_id:
                known_ids.add(entity.user_id)

        # @username mentions (need resolution)
        elif isinstance(entity, MessageEntityMention):
            mention = text[entity.offset : entity.offset + entity.length]
            if mention.startswith("@"):
                username = mention[1:]  # Remove @
                if username:
                    usernames.add(username.lower())

        # Clickable text URLs (e.g., [link text](https://t.me/channel))
        elif isinstance(entity, MessageEntityTextUrl):
            url = entity.url
            match = TELEGRAM_URL_PATTERN.search(url)
            if match:
                username = match.group(1)
                # Skip joinchat links (private invite links)
                if not url.endswith("/joinchat/" + username):
                    usernames.add(username.lower())

        # Plain URLs in text
        elif isinstance(entity, MessageEntityUrl):
            url = text[entity.offset : entity.offset + entity.length]
            match = TELEGRAM_URL_PATTERN.search(url)
            if match:
                username = match.group(1)
                if not url.endswith("/joinchat/" + username):
                    usernames.add(username.lower())

    return usernames, known_ids


async def resolve_username_entity(
    client, username, *, max_retries=TELETHON_RETRY_COUNT, logger=None
):
    """
    Resolve a username to a Telegram entity.

    Returns the resolved entity, or None if resolution failed.
    """
    if logger is None:
        logger = log

    if username is None:
        logger.error("[FAIL] Username is None")
        return None

    normalized = username.strip()
    if normalized.startswith("@"):  # Defensive normalization for input variety
        normalized = normalized[1:]

    if not normalized:
        logger.error("[FAIL] Username is empty")
        return None

    try:
        return await telethon_call_with_retry(
            lambda: client.get_entity(normalized),
            label=f"get_entity {normalized}",
            max_retries=max_retries,
        )
    except FloodWaitError as e:
        logger.error(
            "[FAIL] FloodWait %ds resolving username: %s",
            e.seconds,
            normalized,
        )
        raise
    except (UsernameNotOccupiedError, UsernameInvalidError, ValueError):
        logger.error("[FAIL] Username not found or invalid: %s", normalized)
        return None

    return None


async def resolve_username_to_channel_id(client, username):
    """
    Resolve a username to a channel ID.
    Returns channel_id if it's a channel, None otherwise.
    """
    entity = await resolve_username_entity(
        client,
        username,
        max_retries=1,
        logger=log,
    )
    if isinstance(entity, types.Channel):
        return entity.id
    return None


# ----------------------------------------------------------------------
# Media extraction (handles PhotoSize edge case)
# ----------------------------------------------------------------------


def extract_media_info(message):
    """
    Extract media metadata from a message.
    Returns dict with media info, or None if not applicable.

    Handles PhotoSize objects which lack the 'location' attribute
    that message.file.id tries to access internally.
    """
    if not message.media:
        return None

    # Skip gifs and stickers
    if message.gif or message.sticker:
        return None

    # For photos, use photo-specific attributes
    if message.photo:
        # message.photo is a Photo object with sizes
        photo = message.photo
        return {
            "title": None,
            "media_id": str(photo.id),
            "extension": ".jpg",
        }

    # For documents/other media with file attribute
    if message.file:
        return {
            "title": message.file.name,
            "media_id": str(message.file.id) if message.file.id else None,
            "extension": message.file.ext,
        }

    return None


# ----------------------------------------------------------------------
# Message extraction
# ----------------------------------------------------------------------


def make_tz_aware(dt):
    """Ensure datetime is timezone-aware (UTC)."""
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=datetime.timezone.utc)
    return dt


async def extract_messages(client, channel_id, limit=DEFAULT_MESSAGE_LIMIT, since=None, until=None):
    """
    Extract text messages and media metadata from a channel.

    Args:
        client: Telethon client
        channel_id: Channel to download from
        limit: Max messages to retrieve
        since: Only messages after this date (inclusive)
        until: Only messages before this date (exclusive), used as offset_date

    Returns:
        ChannelExtraction with complete extraction data
    """
    messages = {}
    media = {}
    discovered_channels = set()
    mentioned_usernames = set()
    known_user_ids = set()
    offset_date = until

    effective_limit = None if limit == -1 else limit
    progress = tqdm(
        total=effective_limit if effective_limit and effective_limit > 0 else None,
        desc="Messages",
        unit="msg",
        ascii=True,
        leave=False,
    )
    try:
        async for message in client.iter_messages(
            channel_id, limit=effective_limit, offset_date=offset_date
        ):
            progress.update(1)

            msg_date = make_tz_aware(message.date)

            # Skip messages without date (shouldn't happen, but be safe)
            if msg_date is None:
                continue

            # Filter by since date
            if since and msg_date < since:
                # Messages are in reverse chronological order by default
                # Once we hit a message older than 'since', we can stop
                break

            msg_timestamp = msg_date.timestamp()
            author_id, _ = get_peer_id(message.from_id)

            # Forwarding info
            is_forwarded = False
            forwarded_from_id = None
            forwarded_message_date = None

            if message.forward:
                is_forwarded = True
                fwd_id, fwd_type = get_peer_id(message.forward.from_id)
                forwarded_from_id = fwd_id
                fwd_date = make_tz_aware(message.forward.date)
                if fwd_date:
                    forwarded_message_date = fwd_date.timestamp()

                # Track discovered channels
                if fwd_type == "channel" and fwd_id:
                    discovered_channels.add(fwd_id)

            # Extract mentioned channels from message entities
            usernames, embedded_ids = extract_mentioned_usernames(message)
            mentioned_usernames.update(usernames)
            known_user_ids.update(embedded_ids)

            # Text messages
            if message.text:
                messages[str(message.id)] = {
                    "message": message.text,
                    "date": msg_timestamp,
                    "author": author_id,
                    "is_forwarded": is_forwarded,
                    "forwarded_from_id": forwarded_from_id,
                    "forwarded_message_date": forwarded_message_date,
                }

            # Media
            media_info = extract_media_info(message)
            if media_info:
                media[str(message.id)] = {
                    "title": media_info["title"],
                    "media_id": media_info["media_id"],
                    "extension": media_info["extension"],
                    "date": msg_timestamp,
                    "author": author_id,
                    "is_forwarded": is_forwarded,
                    "forwarded_from_id": forwarded_from_id,
                    "forwarded_message_date": forwarded_message_date,
                }
    finally:
        progress.close()

    log.info(
        "  Extracted %d text messages, %d media items, %d mentioned usernames, %d known user IDs",
        len(messages),
        len(media),
        len(mentioned_usernames),
        len(known_user_ids),
    )
    
    return ChannelExtraction(
        messages=messages,
        media=media,
        discovered_channels=discovered_channels,
        mentioned_usernames=mentioned_usernames,
        known_user_ids=known_user_ids,
    )


# ----------------------------------------------------------------------
# Channel metadata extraction
# ----------------------------------------------------------------------


async def get_channel_metadata(
    client, channel_entity, *, max_retries=TELETHON_RETRY_COUNT
):
    """Extract channel metadata."""
    creation_date = 0
    if channel_entity.date:
        dt = make_tz_aware(channel_entity.date)
        if dt:
            creation_date = int(dt.timestamp())

    metadata = {
        "creation_date": creation_date,
        "username": getattr(channel_entity, "username", "") or "",
        "title": getattr(channel_entity, "title", "") or "",
        "description": "",
        "scam": getattr(channel_entity, "scam", False),
        "verified": getattr(channel_entity, "verified", False),
        "n_subscribers": getattr(channel_entity, "participants_count", 0) or 0,
    }

    # Get full info for description and accurate subscriber count
    if isinstance(channel_entity, types.Channel):
        full_info = await telethon_call_with_retry(
            lambda: client(GetFullChannelRequest(channel=channel_entity)),
            label="get_full_channel",
            max_retries=max_retries,
        )
        metadata["description"] = full_info.full_chat.about or ""
        metadata["n_subscribers"] = full_info.full_chat.participants_count or 0
        if full_info.chats:
            metadata["username"] = full_info.chats[0].username or ""
            metadata["scam"] = getattr(full_info.chats[0], "scam", False)

    return metadata


# ----------------------------------------------------------------------
# Single channel processing
# ----------------------------------------------------------------------


async def process_channel(
    client,
    channel_id,
    current_degree,
    db_name,
    limit=DEFAULT_MESSAGE_LIMIT,
    since=None,
    until=None,
    max_retries=TELETHON_RETRY_COUNT,
):
    """
    Process a single channel: download messages, extract metadata, store results.

    Returns:
        ChannelExtraction with all extracted data
    """
    try:
        channel_entity = await telethon_call_with_retry(
            lambda: client.get_entity(channel_id),
            label=f"get_entity {channel_id}",
            max_retries=max_retries,
        )
    except ChannelPrivateError:
        log.warning("[SKIP] Private channel: %d", channel_id)
        return ChannelExtraction({}, {}, set(), set(), set())

    if not isinstance(channel_entity, (types.Channel, types.Chat)):
        log.debug("Skipping non-channel entity: %s", type(channel_entity).__name__)
        return ChannelExtraction({}, {}, set(), set(), set())

    log.info(
        "Processing: %s (id=%d, degree=%d)",
        getattr(channel_entity, "username", "") or channel_entity.title,
        channel_id,
        current_degree,
    )

    # Get metadata
    metadata = await get_channel_metadata(client, channel_entity)

    # Get messages
    try:
        extraction = await extract_messages(
            client, channel_id, limit=limit, since=since, until=until
        )
    except ChannelPrivateError:
        log.warning("[SKIP] Private channel: %d", channel_id)
        return ChannelExtraction({}, {}, set(), set(), set())

    # Build and store channel document
    channel_doc = {
        "_id": channel_id,
        "text_messages": extraction.messages,
        "generic_media": extraction.media,
        **metadata,
    }
    db_utilities.insert_channel(channel_doc, db_name)

    log.info(
        "[OK] Stored %d messages, %d media, %d discovered channels, %d mentioned usernames, %d known user IDs",
        len(extraction.messages),
        len(extraction.media),
        len(extraction.discovered_channels),
        len(extraction.mentioned_usernames),
        len(extraction.known_user_ids),
    )

    return extraction


# ----------------------------------------------------------------------
# Snowball crawl with degree tracking
# ----------------------------------------------------------------------


async def crawl(client, db_name, max_degree, limit=DEFAULT_MESSAGE_LIMIT, since=None, until=None):
    """
    Snowball crawl from seed channels up to max_degree of separation.
    
    Batch-resolves mentioned entities at each degree level to minimize API calls
    and FloodWait risk.
    """
    log.info(
        "Starting crawl: max_degree=%d, since=%s, until=%s", max_degree, since, until
    )

    for current_degree in range(max_degree + 1):
        # Get channels at this degree that need processing
        if current_degree == 0:
            channels_to_process = db_utilities.get_seed_channel_ids(db_name)
        else:
            channels_to_process = db_utilities.get_channels_at_degree(
                current_degree, db_name
            )

        # Filter to only channels not yet in Channel collection
        existing_ids = []  # set(db_utilities.get_channel_ids(db_name))
        #existing_ids = set(db_utilities.get_channel_ids(db_name))
        channels_to_process = [c for c in channels_to_process if c not in existing_ids]

        if not channels_to_process:
            log.info("Degree %d: no new channels to process", current_degree)
            continue

        log.info(
            "Degree %d: processing %d channels",
            current_degree,
            len(channels_to_process),
        )

        all_known = set(db_utilities.get_all_known_channel_ids(db_name))
        all_mentioned_usernames = set()  # Collect all mentioned usernames at this degree
        all_known_user_ids = set()  # User IDs that don't need resolution

        for channel_id in channels_to_process:
            result = await process_channel(
                client,
                channel_id,
                current_degree,
                db_name,
                limit=limit,
                since=since,
                until=until,
            )

            # Record network edges for newly discovered channels (from forwards)
            next_degree = current_degree + 1
            if next_degree <= max_degree:
                for target_id in result.discovered_channels:
                    if target_id not in all_known:
                        db_utilities.insert_network_edge(
                            source_id=channel_id,
                            target_id=target_id,
                            degree=next_degree,
                            db_name=db_name,
                        )
                        all_known.add(target_id)

            all_mentioned_usernames.update(result.mentioned_usernames)
            all_known_user_ids.update(result.known_user_ids)

            # Rate limiting
            await asyncio.sleep(2)

        # NOTE: Mentioned usernames are currently ignored by request.
        # WARN: This disables discovery of channels that are only referenced via @mentions or URLs.
        if all_mentioned_usernames and current_degree < max_degree:
            log.warning(
                "Degree %d: ignoring %d mentioned usernames (resolution disabled)",
                current_degree,
                len(all_mentioned_usernames),
            )

        # Note: all_known_user_ids are users extracted from MessageEntityMentionName
        # They are already resolved (no API call needed) but are user IDs, not channels
        if all_known_user_ids:
            log.info(
                "Degree %d: skipped %d known user IDs (no API call needed)",
                current_degree,
                len(all_known_user_ids),
            )

    log.info("Crawl complete")


# ----------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------


async def main(args):
    client, phone = get_client()

    since = parse_date(args.since)
    until = parse_date(args.until)

    async with client:
        if not await client.is_user_authorized():
            await telethon_call_with_retry(
                lambda: client.send_code_request(phone),
                label="send_code_request",
            )
            try:
                code = input("Enter the code: ")
                await telethon_call_with_retry(
                    lambda: client.sign_in(phone, code),
                    label="sign_in code",
                )
            except SessionPasswordNeededError:
                password = input("Password: ")
                await telethon_call_with_retry(
                    lambda: client.sign_in(password=password),
                    label="sign_in password",
                )

        await crawl(
            client,
            db_name=args.db_name,
            max_degree=args.max_degree,
            limit=args.limit,
            since=since,
            until=until,
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Telegram channel crawler")
    parser.add_argument(
        "--max-degree",
        type=int,
        default=2,
        help="Max degrees of separation from seeds (default: 2)",
    )
    parser.add_argument(
        "--since",
        type=str,
        default=None,
        help="Only messages after this date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--until",
        type=str,
        default=None,
        help="Only messages before this date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=10000,
        help="Max messages per channel (default: 10000)",
    )
    parser.add_argument(
        "--db-name",
        default="Telegram",
        help="MongoDB database name (default: Telegram)",
    )
    args = parser.parse_args()

    asyncio.run(main(args))
