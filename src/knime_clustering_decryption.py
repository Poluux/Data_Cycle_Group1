from encryption import decrypt_table
from pathlib import Path
from dotenv import load_dotenv
import pandas as pd
import sys
import os
import json


base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

config_path = os.path.join(base_dir, "config", "settings.json")
with open(config_path) as f:
    config = json.load(f)

silver_dir = Path(base_dir) / config["paths"]["silver"]
knime_silver_decrypted = Path(base_dir) / config["paths"]["knime_decrypted"]

knime_silver_decrypted.mkdir(parents=True, exist_ok=True)

# Load depending on context of execution
if os.getenv("RUNNING_IN_DOCKER"):
    load_dotenv(".env", override=True)
else:
    load_dotenv(os.path.join(os.path.dirname(__file__), ".env.local"), override=True)


def knime_decrypt_files():
    print("Decrypting encrypted files")

    silver_path = silver_dir / "price_history"
    output_path = knime_silver_decrypted / "price_history"

    output_path.mkdir(parents=True, exist_ok=True)

    files = list(silver_path.glob("clean_price_history_*.parquet"))

    if not files:
        print("No files found")
        return

    for file in files:
        print(f"\nProcessing {file.name}")

        df = pd.read_parquet(file)
        df = decrypt_table(df)

        ticker_name = file.stem.replace("clean_price_history_", "")

        file_name = f"decrypted_price_history_{ticker_name}.parquet"
        save_path = output_path / file_name

        df.to_parquet(save_path, index=False)
        print(f"- file for {ticker_name} decrypted")

    print("Decryption process finished")


if __name__ == "__main__":
    knime_decrypt_files()
