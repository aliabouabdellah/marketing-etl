# Marketing ETL (Portfolio)

## Overview
- Extract marketing/ecommerce datasets (GA, ads, ecommerce) and build staged tables plus a marketing fact/dims.

## Prereqs
- Python 3.x
- Kaggle account + API token:
  - Kaggle -> Account/Settings -> API -> Create New Token
  - Save kaggle.json to %USERPROFILE%\.kaggle\kaggle.json (Windows) or ~/.kaggle/kaggle.json
  - Or place in credentials/kaggle.json and set KAGGLE_CONFIG_DIR to that folder

## Setup
python -m venv .venv
.venv\Scripts\activate            # Windows (on mac/linux: source .venv/bin/activate)
pip install -r requirements.txt

## Run (example flow)
python extract_ga.py
python transform_ga.py
python extract_brazil_olist.py    # uses Kaggle token
python transform_ecommerce.py
python generate_ads_data.py
python transforme_ads.py
python crazy_marketing.py         # builds final marketing output

Outputs land in data/staged/ and data/warehouse/marketing/ (both git-ignored). Sample data lives in data/sample/.

## Notes
- Full datasets and PBIX files are excluded; only small samples (if provided) are included.
