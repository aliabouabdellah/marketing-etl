import kagglehub
import os
import shutil
from datetime import datetime

# ---------------------------------------------
# CONFIGURATION
# ---------------------------------------------

# Kaggle dataset identifier
DATASET_ID = "olistbr/brazilian-ecommerce"

# Local destination path
DESTINATION = r"D:\DATA end to end projects\marketing etl\data\raw"
OUTPUT_DIR = "data/raw/ga"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ---------------------------------------------
# FUNCTIONS
# ---------------------------------------------

def download_olist_dataset():
    """Download the Olist dataset using Kaggle Hub."""
    print("📥 Downloading Olist dataset...")
    source_path = kagglehub.dataset_download(DATASET_ID)
    print(f"✅ Dataset downloaded at: {source_path}")
    return source_path


def copy_csv_files(source_path, destination):
    """Copy all CSV files from KaggleHub cache to the project raw folder."""
    os.makedirs(destination, exist_ok=True)
    copied_files = []

    for file in os.listdir(source_path):
        if file.endswith(".csv"):
            shutil.copy(os.path.join(source_path, file), destination)
            copied_files.append(file)
            print(f"📄 Copied: {file}")

    return copied_files


def log_extraction(files):
    """Create a log file to track extraction timestamp."""
    log_path = os.path.join(DESTINATION, "extract_log.txt")
    with open(log_path, "a") as log:
        log.write("\n--- Extraction Run ---\n")
        log.write(f"Timestamp: {datetime.now()}\n")
        for f in files:
            log.write(f"- {f}\n")
    print(f"\n📝 Extraction log saved at: {log_path}")


# ---------------------------------------------
# MAIN
# ---------------------------------------------

def main():
    print("🚀 Starting e-commerce data extraction...\n")

    # 1. Download Olist dataset
    source_path = download_olist_dataset()

    # 2. Copy files to raw folder
    copied_files = copy_csv_files(source_path, DESTINATION)

    # 3. Log extraction
    log_extraction(copied_files)

    print("\n🎉 Extraction completed successfully!")


if __name__ == "__main__":
    main()
