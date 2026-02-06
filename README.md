# TGDataset

TGDataset is a collection of Telegram channels that takes a snapshot of the actual Telegram ecosystem instead of focusing on a particular topic. 

The dataset size is approximately 460 GB and is available for download in its zipped version (roughly 71 GB) through the Zenodo service [here](https://zenodo.org/record/7640712#.Y-9PjNLMKXI).

If you use this dataset, please cite:
```
@inproceedings{10.1145/3690624.3709397,
author = {La Morgia, Massimo and Mei, Alessandro and Mongardini, Alberto Maria},
title = {TGDataset: Collecting and Exploring the Largest Telegram Channels Dataset},
year = {2025},
isbn = {9798400712456},
publisher = {Association for Computing Machinery},
address = {New York, NY, USA},
url = {https://doi.org/10.1145/3690624.3709397},
doi = {10.1145/3690624.3709397},
booktitle = {Proceedings of the 31st ACM SIGKDD Conference on Knowledge Discovery and Data Mining V.1},
pages = {2325–2334},
numpages = {10},
keywords = {conspiracy theories, copyright infringement, dataset, telegram},
location = {Toronto ON, Canada},
series = {KDD '25}
}
```




## Structure

The dataset contains 120,979 Telegram channels stored in (alphabetically sorted) 121 JSON files divided in 4 folders:
- TGDataset_1 -> channels with username starting with A to freeJul
- TGDataset_2 -> channels with username starting with freejur to NaturKind
- TGDataset_3 -> channels with username starting with Naturmedi to theslog
- TGDataset_4 -> the remaining channels

For each channel, we store the following information:
- **channel_id**: the ID of Telegram channel (*int*),
- **creation_date**: the timestamp related to the creation date of the channel (*int*),
- **username**: the Telegram username of the channel (*string*),
- **title**: the title of the channel (*string*),
- **description**: the description of the channel (*string*),
- **scam**: indicates if Telegram marked the channel as a scam (*bool*),
- **verified**: indicates if Telegram marked the channel as verified (*bool*),
- **n_subscribers**: the number of subscribers of the channel (*int*),
- **text_messages**: the text messages posted in the channel,
- **generic_media**: the media content posted in the channel.
 
Each text message has: 
- **message**: the text of the message (*string*),
- **date**: the timestamp related to the date of the message (*int*),
- **author**: the ID of who posted the message (*int*),
- **forwarding information**:
  - **is_forwarded**: indicates if the message is forwarded (*bool*),
  - **forwarded_from_id**: the ID from which the message is forwarded (*int*),
  - **forwarded_message_date**: the timestamp related to the date of the first post of the message (*int*).

Each media content has:
- **title**: the title of the content (*string*),
- **media_id**: the ID of the content on Telegram (*string*),
- **date**: the timestamp related to the date of the content (*int*),
- **author**: the ID of who posted the content (*int*),
- **extension**: the format of the content (*string*),
- **forwarding information**.


The JSON files are in the following structure:
```perl
{channel_id:
  {'creation_date': channel_creation_date,
   'username': channel_username,
   'title': channel_title,
   'description': channel_description,
   'scam': is_scam,
   'verified': is_verified,
   'n_subscribers': n_subscribers,
   'text_messages':
    {message_id:
      {'message':message, 
       'date': message_date, 
       'author': message_author, 
       'is_forwarded':is_forwarded, 
       'forwarded_from_id':forwarded_from_id, 
       'forwarded_message_date':forwarded_message_date},...
    }, 
   'generic_media': 
    {local_media_id:
       {'title': title, 
        'media_id':global_media_id,
        'date': message_date,  
        'author': message_author, 
        'extension': extension,
        'is_forwarded':is_forwarded, 
        'forwarded_from_id':forwarded_from_id, 
        'forwarded_message_date':forwarded_message_date},...
    }
  },...                  
}
``` 

## Importing data into MongoDB

- Install MongoDB following the instruction reported on the [official website](https://www.mongodb.com/docs/manual/administration/install-community/)
- Download a portion or the whole dataset from [Zenodo](https://zenodo.org/record/7640712#.Y-9PjNLMKXI).
- Unpack the dataset and move the Json files into the folder `public_db`
- Install all the necessary python packages running the following command:
```bash
pip install -r requirements.txt
```
- Run the script `db_utilities.py`
```bash
python db_utilities.py
```

## Creating Your Own Dataset from Seed Channels

If you want to crawl Telegram channels yourself instead of using the pre-built TGDataset, follow these steps:

### Step 1: Get Telegram API Credentials

1. Go to [https://my.telegram.org](https://my.telegram.org)
2. Log in with your phone number
3. Go to "API development tools" and create a new application
4. Note your `api_id` and `api_hash`

### Step 2: Create Configuration File

Create a `config.ini` file in the project root:

```ini
[Telegram]
api_id = YOUR_API_ID
api_hash = YOUR_API_HASH
phone = +1234567890
username = my_session
```

### Step 3: Prepare Seed Channels

Edit `seed-channels.txt` with your seed channels (one per line). Supported formats:
```
# Comments start with #
https://t.me/channelname
@channelname
channelname
```

### Step 4: Start MongoDB

Using Docker (recommended):
```bash
docker-compose up -d mongodb
```

Or install MongoDB locally following the [official instructions](https://www.mongodb.com/docs/manual/administration/install-community/).

### Step 5: Install Dependencies

```bash
pip install -r requirements.txt
```

### Step 6: Run the Crawler

```bash
# Basic usage (crawls 2 degrees of separation by default)
python crawler.py

# Custom options
python crawler.py --max-degree 3 --limit 5000

# Full options
python crawler.py --help
```

**Command-line options:**
| Option | Description | Default |
|--------|-------------|---------|
| `-d`, `--max-degree` | Maximum degrees of separation from seed channels | 2 |
| `-s`, `--seed-file` | Path to seed channels file | seed-channels.txt |
| `-l`, `--limit` | Maximum messages to download per channel | 10000 |

### Step 7: Export the Network Graph (Optional)

```python
import db_utilities

# Export to CSV (edge list)
db_utilities.export_network('network.csv', 'Telegram')

# Export to JSON with channel metadata
db_utilities.export_network('network.json', 'Telegram', include_channel_info=True)

# Export to GEXF for Gephi (requires networkx)
db_utilities.export_network('network.gexf', 'Telegram', include_channel_info=True)
```

### How the Snowball Crawler Works

1. **Degree 0**: Downloads all seed channels from `seed-channels.txt`
2. **Degree 1**: Discovers channels from forwarded messages in seed channels
3. **Degree 2+**: Continues discovering channels up to `--max-degree`
4. **Network**: Stores all forwarding relationships in a `Network` collection

The crawler stores:
- **Channel collection**: Channel metadata + messages
- **Network collection**: Directed edges (source → forwarded_from)
- **degree**: Distance from nearest seed channel (0 = seed)

# Docker

## Importing data into MongoDB

- Run the following script
```bash
docker-compose run build_db
```

## Running script

```bash
docker-compose run python_app
```

## Other data
The `labeled_data` folder contains three csv files:

- **ch_to_topic_mapping.csv**: indicates the topic addressed by each channel (identified by its ID).
- **channel_to_language_mapping.csv**: indicates the language used by each channel (identified by its ID).
- **sabmyk_network.csv**: the list of channels belonging to the Sabmyk network (identified by its ID).
- **conspiracy_channels.csv**: the list of conspiracy channels posting URLs contained in the Conspiracy Resources Dataset presented in the paper: [The Conspiracy Money Machine: Uncovering
 Telegram’s Conspiracy Channels and their Profit Model](https://arxiv.org/pdf/2310.15977.pdf).


## Additional files
This repository contains the following scripts.


**db_utilities.py**: defines utility functions to interact with MongoDB.

- *import_channels_to_mongoDB(db_name)*: imports the channels from json format files to MongoDB creating a new db called db_name.
- *get_channel_ids()*: returns all the ID of the channels within the MongoDB database.
- *get_channels_by_ids(ids_channels)*: return the channels with ID belonging to the given list of IDs.
- *get_channels_by_id(id_channel)*: return the channel with ID id_channel.
- *get_channels_by_username(username)*: return the channel with target username.

**language_detection.py**: defines the functions used to perform language detection.

- *preprocessDocs(docs)*: performs the preprocessing of channels
- *detect_language(channel)*: detects the language of target channel

**topic_modeling_LDA.py**: defines the functions used to perform topic modeling.

- *perform_preprocessing()*: performs the preprocessing of the channels
- *perform_LDA()*: performs LDA on the collected channels
