import json
import os
import pickle
from pathlib import Path

import ijson
import networkx as nx
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from tqdm import tqdm

# MongoDB URI
uri = os.environ.get("MONGO_DB_URL", "mongodb://localhost:27017")


def insert_execution_context(context_doc, db_name="Telegram"):
    with MongoClient(uri) as client:
        db = client[db_name]
        db.Execution.insert_one(context_doc)


def update_execution_context(context_uuid, updates, db_name="Telegram"):
    with MongoClient(uri) as client:
        db = client[db_name]
        db.Execution.update_one({"_id": context_uuid}, {"$set": updates})


def get_execution_context(context_uuid, db_name="Telegram"):
    with MongoClient(uri) as client:
        db = client[db_name]
        return db.Execution.find_one({"_id": context_uuid})


def list_execution_contexts(db_name="Telegram"):
    with MongoClient(uri) as client:
        db = client[db_name]
        return list(db.Execution.find({}).sort("started_at", -1))


def insert_discovered_channel(
    channel_id, execution_uuid, db_name="Telegram", distance=None
):
    with MongoClient(uri) as client:
        db = client[db_name]
        set_fields = {"execution": execution_uuid, "_status": "unprocessed"}
        if distance is not None:
            set_fields["_distance"] = distance

        db.Channel.update_one(
            {"_id": channel_id},
            {
                "$set": set_fields,
                "$setOnInsert": {
                    "creation_date": 0,
                    "username": "",
                    "title": "",
                    "description": "",
                    "scam": False,
                    "verified": False,
                    "n_subscribers": 0,
                },
            },
            upsert=True,
        )


def update_channel_status(channel_id, execution_uuid, status, db_name="Telegram"):
    with MongoClient(uri) as client:
        db = client[db_name]
        db.Channel.update_one(
            {"_id": channel_id},
            {"$set": {"execution": execution_uuid, "_status": status}},
        )


def get_next_unprocessed_channel(execution_uuid, db_name="Telegram"):
    with MongoClient(uri) as client:
        db = client[db_name]
        doc = db.Channel.find_one(
            {"execution": execution_uuid, "_status": "unprocessed"},
            sort=[("_distance", 1), ("_id", 1)],
        )
        if doc:
            return doc["_id"]
        return None


def get_channel(channel_id, execution_uuid, db_name="Telegram"):
    """Get channel document by ID without loading messages/media."""
    with MongoClient(uri) as client:
        db = client[db_name]
        return db.Channel.find_one({"_id": channel_id, "execution": execution_uuid})


def update_channel_distance(channel_id, execution_uuid, distance, db_name="Telegram"):
    """Update channel distance to the given value."""
    with MongoClient(uri) as client:
        db = client[db_name]
        db.Channel.update_one(
            {"_id": channel_id, "execution": execution_uuid},
            {
                "$set": {
                    "_distance": distance,
                }
            },
        )


def insert_text_message(
    channel_id,
    execution_uuid,
    message_id,
    message_doc,
    db_name="Telegram_test",
):
    message_key = f"{channel_id}:{execution_uuid}:{message_id}"
    payload = message_doc.copy()
    payload.update(
        {
            "_id": message_key,
            "channel_id": channel_id,
            "execution": execution_uuid,
            "message_id": message_id,
        }
    )
    with MongoClient(uri) as client:
        db = client[db_name]
        db.TextMessage.update_one({"_id": message_key}, {"$set": payload}, upsert=True)


def insert_media_item(
    channel_id,
    execution_uuid,
    message_id,
    media_doc,
    db_name="Telegram_test",
):
    media_key = f"{channel_id}:{execution_uuid}:{message_id}"
    payload = media_doc.copy()
    payload.update(
        {
            "_id": media_key,
            "channel_id": channel_id,
            "execution": execution_uuid,
            "message_id": message_id,
        }
    )
    with MongoClient(uri) as client:
        db = client[db_name]
        db.Media.update_one({"_id": media_key}, {"$set": payload}, upsert=True)


# ----------------------------------------------------------------------
# Network Collection Functions
# Stores edges: {source_id, target_id, degree}
# degree = shortest path distance from any seed channel
# ----------------------------------------------------------------------


def insert_network_edge(source_id, target_id, degree, db_name="Telegram"):
    """Insert or update a network edge, keeping minimum degree."""
    with MongoClient(uri) as client:
        db = client[db_name]
        edges = db.Network
        existing = edges.find_one({"source_id": source_id, "target_id": target_id})
        if existing:
            if degree < existing["degree"]:
                edges.update_one({"_id": existing["_id"]}, {"$set": {"degree": degree}})
        else:
            edges.insert_one(
                {"source_id": source_id, "target_id": target_id, "degree": degree}
            )


def get_channel_degree(channel_id, db_name="Telegram"):
    """Return the minimum degree for a channel (as target), or None if not found."""
    with MongoClient(uri) as client:
        db = client[db_name]
        result = db.Network.find({"target_id": channel_id}).sort("degree", 1).limit(1)
        for doc in result:
            return doc["degree"]
        return None


def get_seed_channel_ids(db_name="Telegram"):
    """Return IDs of all seed channels (degree 0)."""
    with MongoClient(uri) as client:
        db = client[db_name]
        return [doc["target_id"] for doc in db.Network.find({"degree": 0})]


def insert_seed_channel(channel_id, db_name="Telegram"):
    """Mark a channel as seed (degree 0, self-referencing edge)."""
    with MongoClient(uri) as client:
        db = client[db_name]
        db.Network.update_one(
            {"source_id": channel_id, "target_id": channel_id},
            {"$set": {"degree": 0}},
            upsert=True,
        )


def get_channels_at_degree(degree, db_name="Telegram"):
    """Return all channel IDs discovered at exactly this degree."""
    with MongoClient(uri) as client:
        db = client[db_name]
        return list(
            set(doc["target_id"] for doc in db.Network.find({"degree": degree}))
        )


def get_all_known_channel_ids(db_name="Telegram"):
    """Return all channel IDs in the network."""
    with MongoClient(uri) as client:
        db = client[db_name]
        return list(set(doc["target_id"] for doc in db.Network.find({})))


def export_networkx_from_mongoDB(db_name="Telegram", include_channel_info=False):
    """Return the network as a NetworkX DiGraph from MongoDB."""
    with MongoClient(uri) as client:
        db = client[db_name]
        graph = nx.DiGraph()

        cursor = db.Network.find(
            {}, {"_id": 0, "source_id": 1, "target_id": 1, "degree": 1}
        )
        for doc in cursor:
            source_id = int(doc["source_id"])
            target_id = int(doc["target_id"])
            degree = doc.get("degree")
            graph.add_edge(source_id, target_id, degree=degree)

        if include_channel_info:
            node_ids = list(graph.nodes)
            chunk_size = 1000
            for start in range(0, len(node_ids), chunk_size):
                chunk = node_ids[start : start + chunk_size]
                for ch in db.Channel.find(
                    {"_id": {"$in": chunk}},
                    {
                        "_id": 1,
                        "username": 1,
                        "title": 1,
                        "description": 1,
                        "scam": 1,
                        "verified": 1,
                        "n_subscribers": 1,
                    },
                ):
                    node_id = int(ch["_id"])
                    graph.nodes[node_id].update(
                        {
                            "username": ch.get("username", ""),
                            "title": ch.get("title", ""),
                            "description": ch.get("description", ""),
                            "scam": ch.get("scam", False),
                            "verified": ch.get("verified", False),
                            "n_subscribers": ch.get("n_subscribers", 0),
                        }
                    )

        return graph


# Insert the channel in MongoDB
# Parameters:
#   - new_channel -> new channel to insert
#   - db_name -> specify the name of the collection in MongoDB
def insert_channel(new_channel, db_name="Telegram_test", execution_uuid=None):
    if execution_uuid:
        new_channel["execution"] = execution_uuid
    if "_status" not in new_channel:
        new_channel["_status"] = "processed"
    new_channel.pop("text_messages", None)
    new_channel.pop("generic_media", None)

    with MongoClient(uri) as client:
        db = client[db_name]
        channel = db.Channel
        try:
            channel.insert_one(new_channel)
        except DuplicateKeyError:
            channel.update_one(
                {"_id": new_channel["_id"]},
                {
                    "$set": {
                        "creation_date": new_channel["creation_date"],
                        "username": new_channel["username"],
                        "title": new_channel["title"],
                        "description": new_channel["description"],
                        "scam": new_channel["scam"],
                        "verified": new_channel["verified"],
                        "n_subscribers": new_channel["n_subscribers"],
                        "execution": new_channel.get("execution"),
                        "_status": new_channel.get("_status"),
                    },
                },
            )


# Return the text messages of target channel
# Parameters:
#   - id_channel -> ID of the channel from which return the text messages
#   - db_name -> specify the name of the collection in MongoDB
def get_text_messages_by_id_ch(
    id_channel, db_name="Telegram_test", execution_uuid=None
):
    with MongoClient(uri) as client:
        db = client[db_name]
        if execution_uuid is None:
            return {}
        cursor = db.TextMessage.find(
            {"channel_id": id_channel, "execution": execution_uuid}
        )
        messages = {}
        for doc in cursor:
            message_id = doc.get("message_id")
            if message_id is None:
                continue
            message = doc.copy()
            message.pop("_id", None)
            message.pop("channel_id", None)
            message.pop("execution", None)
            message.pop("message_id", None)
            messages[message_id] = message

        return messages


# Return the generic media of target channel
# Parameters:
#   - id_channel -> ID of the channel from which return the generic media
#   - db_name -> specify the name of the collection in MongoDB
def get_generic_media_by_id_ch(
    id_channel, db_name="Telegram_test", execution_uuid=None
):
    with MongoClient(uri) as client:
        db = client[db_name]
        if execution_uuid is None:
            return {}
        cursor = db.Media.find({"channel_id": id_channel, "execution": execution_uuid})
        media = {}
        for doc in cursor:
            message_id = doc.get("message_id")
            if message_id is None:
                continue
            entry = doc.copy()
            entry.pop("_id", None)
            entry.pop("channel_id", None)
            entry.pop("execution", None)
            entry.pop("message_id", None)
            media[message_id] = entry

        return media


# Return the channel with ID id_channel
# Parameters:
#   - id_channel -> ID of channel to return
#   - execution_uuid -> execution context UUID to filter by
#   - db_name -> specify the name of the collection in MongoDB
def get_channel_by_id(id_channel, execution_uuid, db_name="Telegram_test"):
    ch = {}
    with MongoClient(uri) as client:
        db = client[db_name]
        ch = db.Channel.find_one({"_id": id_channel, "execution": execution_uuid})
        if ch is None:
            return None
        ch["text_messages"] = get_text_messages_by_id_ch(
            id_channel, db_name, execution_uuid
        )
        if "generic_media" not in ch:
            ch["generic_media"] = get_generic_media_by_id_ch(
                id_channel, db_name, execution_uuid
            )
        ch["_id"] = int(ch["_id"])

    return ch


# Return the channel with target username
# Parameters:
#   - username -> username of the channel to return
#   - execution_uuid -> execution context UUID to filter by
#   - db_name -> specify the name of the collection in MongoDB
def get_channel_by_username(username, execution_uuid, db_name="Telegram_test"):
    ch = {}
    with MongoClient(uri) as client:
        db = client[db_name]
        ch = db.Channel.find_one({"username": username, "execution": execution_uuid})
        if ch is None:
            return None
        ch["text_messages"] = get_text_messages_by_id_ch(
            ch["_id"], db_name, execution_uuid
        )
        if "generic_media" not in ch:
            ch["generic_media"] = get_generic_media_by_id_ch(
                ch["_id"], db_name, execution_uuid
            )
        ch["_id"] = int(ch["_id"])

    return ch


# Return the channels with ID belonging to the given list of IDs
# Parameters:
#   - ids_channels -> IDs list of channels to return
#   - execution_uuid -> execution context UUID to filter by
#   - db_name -> specify the name of the collection in MongoDB
def get_channels_by_ids(ids_channels, execution_uuid, db_name="Telegram_test"):
    chs = []
    with MongoClient(uri) as client:
        db = client[db_name]

        for ch in db.Channel.find(
            {"_id": {"$in": ids_channels}, "execution": execution_uuid}
        ):
            ch["text_messages"] = get_text_messages_by_id_ch(
                ch["_id"], db_name, execution_uuid
            )
            if "generic_media" not in ch:
                ch["generic_media"] = get_generic_media_by_id_ch(
                    ch["_id"], db_name, execution_uuid
                )
            ch["_id"] = int(ch["_id"])
            chs.append(ch)

    return chs


# Return the channel ID of all the channels stored in MongoDB
# Parameters:
#   - db_name -> specify the name of the collection in MongoDB
def get_channel_ids(db_name="Telegram_test", execution_uuid=None):
    with MongoClient(uri) as client:
        db = client[db_name]
        query = {}
        if execution_uuid:
            query["execution"] = execution_uuid
        ids = [ch["_id"] for ch in db.Channel.find(query, {"_id": 1})]
    return ids


# Upload the json file to mongo db performing the parsing (less memory required)
# Parameters:
#   - json_file -> the name of the file to upload
#   - db_name -> specify the name of the collection in MongoDB
def upload_json_file_to_mongo(json_file, db_name):
    with open(json_file) as f:
        events = ijson.basic_parse(f)

        matched_key = None
        ch_dict = {}
        matched_sub_key = None
        sub_dict = {}
        id_message = None
        message_dict = {}
        nest = -1
        for event, value in events:
            if event == "start_map":
                nest += 1
            if event == "end_map":
                nest -= 1
                if nest == 0:
                    ch_dict["creation_date"] = int(ch_dict["creation_date"])
                    insert_channel(ch_dict, db_name)
                    ch_dict = {}

                if nest == 1 and matched_key in ["text_messages", "generic_media"]:
                    ch_dict[matched_key] = sub_dict
                    sub_dict = {}

                if nest == 2:
                    sub_dict[id_message] = message_dict
                    message_dict = {}

            if event == "map_key":
                if nest == 0:
                    ch_dict["_id"] = int(value)
                if nest == 2:
                    id_message = value

            if event == "map_key":
                if nest == 1:
                    matched_key = value
                if nest == 3:
                    matched_sub_key = value

            if event not in ["map_key", "start_map", "end_map"]:
                if nest == 1:
                    ch_dict[matched_key] = value

                if nest == 3:
                    if (
                        matched_sub_key in ["date", "forwarded_message_date"]
                        and value is not None
                    ):
                        message_dict[matched_sub_key] = int(value)
                    else:
                        message_dict[matched_sub_key] = value


# Imports the channels from json format to MongoDB
# Parameters:
#   - db_name -> specify the name of the collection to create in MongoDB
#   - root_directory -> is the directory containing the files to upload
#   - fast_mode -> if set to False parse the json to reduce the required memory
def import_channels_to_mongoDB(db_name, root_directory="public_db", fast_mode=False):

    file_list = []
    for directory, _, files in os.walk(root_directory):
        for name in files:
            if name.endswith(".json"):
                file_list.append(os.path.join(directory, name))

    for file in tqdm(file_list):
        if fast_mode:
            with open(file) as f:
                channels = json.load(f)

            for ch_id in channels:
                channel = channels[ch_id]
                channel["_id"] = int(ch_id)
                insert_channel(channel, db_name)
        else:
            upload_json_file_to_mongo(file, db_name)
            print(file + " IMPORTED SUCCESSFULLY")


# Export channels from MongoDB into JSON files (counterpart to import).
# Parameters:
#   - db_name -> specify the name of the collection to read from
#   - output_directory -> directory to write JSON files to
#   - file_prefix -> prefix for output files
#   - max_file_size_gb -> split output into multiple files by size in GB
def export_channels_from_mongoDB(
    db_name,
    output_directory="public_db",
    file_prefix="tgdataset_export",
    max_file_size_gb=4,
):
    if max_file_size_gb <= 0:
        raise ValueError("max_file_size_gb must be > 0")

    os.makedirs(output_directory, exist_ok=True)
    max_bytes = int(max_file_size_gb * 1024 * 1024 * 1024)

    def open_export_file(file_index):
        filename = f"{file_prefix}_{file_index:03d}.json"
        path = os.path.join(output_directory, filename)
        fp = open(path, "wb")
        fp.write(b"{")
        return fp, path

    with MongoClient(uri) as client:
        db = client[db_name]

        file_index = 1
        count_in_file = 0
        total_count = 0
        fp, current_path = open_export_file(file_index)
        first_entry = True

        cursor = db.Channel.find({}, {"_id": 1, "generic_media": 1})
        for ch in cursor:
            channel_id = int(ch["_id"])
            channel_doc = db.Channel.find_one({"_id": channel_id})
            execution_uuid = channel_doc.get("execution")
            text_messages = get_text_messages_by_id_ch(
                channel_id, db_name, execution_uuid
            )
            if "generic_media" in channel_doc:
                generic_media = channel_doc["generic_media"]
            else:
                generic_media = get_generic_media_by_id_ch(
                    channel_id, db_name, execution_uuid
                )

            export_doc = {
                "creation_date": channel_doc.get("creation_date", 0),
                "username": channel_doc.get("username", ""),
                "title": channel_doc.get("title", ""),
                "description": channel_doc.get("description", ""),
                "scam": channel_doc.get("scam", False),
                "verified": channel_doc.get("verified", False),
                "n_subscribers": channel_doc.get("n_subscribers", 0),
                "text_messages": text_messages,
                "generic_media": generic_media,
            }

            entry_prefix = "," if not first_entry else ""
            entry_str = f"{json.dumps(str(channel_id))}:{json.dumps(export_doc, separators=(',', ':'))}"
            entry_bytes = (entry_prefix + entry_str).encode("utf-8")

            if not first_entry and fp.tell() + len(entry_bytes) + 1 > max_bytes:
                fp.write(b"}")
                fp.close()
                print(f"[OK] Exported {count_in_file} channels to {current_path}")

                file_index += 1
                count_in_file = 0
                fp, current_path = open_export_file(file_index)
                first_entry = True

                entry_prefix = ""
                entry_bytes = entry_str.encode("utf-8")

            fp.write(entry_bytes)
            first_entry = False

            count_in_file += 1
            total_count += 1

        fp.write(b"}")
        fp.close()
        print(f"[OK] Exported {count_in_file} channels to {current_path}")
        print(f"[OK] Total exported channels: {total_count}")


# Return the IDs of the new channels to search during the snowball approach
# Parameters:
#   - db_name -> specify the name of the collection in MongoDB
def get_other_channels_references(db_name="Telegram", execution_uuid=None):
    old_references = get_channel_ids(db_name, execution_uuid)
    print("Total number of channels in the db: ", len(old_references))

    path = Path("channels_to_find")
    if path.is_file():
        with open("channels_to_find", "rb") as fp:
            last_checked_channels = pickle.load(fp)
    else:
        last_checked_channels = old_references

    new_references = {}

    with MongoClient(uri) as client:
        db = client[db_name]

        channel_query = {"_id": {"$in": last_checked_channels}}
        if execution_uuid:
            channel_query["execution"] = execution_uuid

        if execution_uuid is None:
            return []

        for ch in db.Channel.find(channel_query):
            ch_id = int(ch["_id"])
            for doc in db.TextMessage.find(
                {
                    "channel_id": ch_id,
                    "execution": execution_uuid,
                    "is_forwarded": True,
                },
                {"forwarded_from_id": 1},
            ):
                forwarded_id = doc.get("forwarded_from_id")
                if forwarded_id is not None:
                    new_references.add(forwarded_id)

            for doc in db.Media.find(
                {
                    "channel_id": ch_id,
                    "execution": execution_uuid,
                    "is_forwarded": True,
                },
                {"forwarded_from_id": 1},
            ):
                forwarded_id = doc.get("forwarded_from_id")
                if forwarded_id is not None:
                    new_references.add(forwarded_id)

    new_references = list(new_references.difference(old_references))

    return new_references


if __name__ == "__main__":
    import_channels_to_mongoDB("Telegram_test")
    print(get_channel_ids("Telegram_test"))
