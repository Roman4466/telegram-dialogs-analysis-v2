import os
import pandas as pd
import telethon
from telethon import TelegramClient
from utils.utils import init_config, read_dialogs

# Load configuration
config_path = "config/config.json"
config = init_config(config_path)
gifs_folder = config["gifs_folder"]
dialogs_data_folder = config["dialogs_data_folder"]

# Fetch user ID dynamically from config.json
user_id = int(config["user_id"])  # Ensure it is an integer
full_id = f'PeerUser(user_id={user_id})'

# Ensure the GIFs folder exists
if not os.path.exists(gifs_folder):
    os.makedirs(gifs_folder)

# Initialize Telegram client
client = TelegramClient(
    "tmp",
    config["api_id"],
    config["api_hash"],
    system_version="4.16.30-vxCUSTOM"
)


async def is_gif(message):
    """
    Check if a message is a GIF even if its type is marked as video.
    """
    if message.video:
        # Check if the file is actually a GIF
        for attribute in message.video.attributes:
            if isinstance(attribute, telethon.tl.types.DocumentAttributeAnimated):
                return True
    return False


async def download_gifs_from_csv(dialog_file):
    """
    Load messages from the CSV file, filter for GIFs sent by the user,
    and download the GIFs.
    """
    print(f"Processing dialog file: {dialog_file}")

    # Load the CSV file
    try:
        df = pd.read_csv(dialog_file)
    except pd.errors.EmptyDataError:
        print(f"Warning: {dialog_file} is empty or corrupted.")
        return

    # Filter for messages that are videos sent by the user
    video_messages = df[
        (df['type'] == 'video') &
        ((df['from_id'] == user_id) | (df['from_id'] == full_id))
    ]

    print(f"Found {len(video_messages)} potential GIFs sent by you.")

    # Download each GIF if it's actually a GIF
    for _, row in video_messages.iterrows():
        if pd.notnull(row['id']):
            message_id = row['id']
            file_name = os.path.join(gifs_folder, f"{message_id}.gif")
            if os.path.exists(file_name):
                print(f"GIF {file_name} already downloaded, skipping...")
                continue

            try:
                # Fetch the actual message from Telegram
                message = await client.get_messages(entity=row['to_id'], ids=message_id)
                if await is_gif(message):
                    await client.download_media(message.media, file=file_name)
                    print(f"Downloaded GIF: {file_name}")
            except Exception as e:
                print(f"Error downloading GIF {message_id}: {str(e)}")


async def process_all_dialogs():
    """
    Process all existing dialog CSV files to download GIFs.
    """
    dialog_files = [
        os.path.join(dialogs_data_folder, f)
        for f in os.listdir(dialogs_data_folder)
        if f.endswith('.csv')
    ]

    print(f"Found {len(dialog_files)} dialog files.")

    async with client:
        for dialog_file in dialog_files:
            await download_gifs_from_csv(dialog_file)


if __name__ == "__main__":
    with client:
        client.loop.run_until_complete(process_all_dialogs())
