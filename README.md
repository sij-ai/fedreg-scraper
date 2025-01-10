# fedreg-scraper

Scrapes all notices of specified agencies from the U.S. Federal Register and uploads them to a MinIO object store.

## Usage

1. Setup MinIO.

Create a bucket and an access policy on your MinIO instance. 

Use the example access policy as a template if you like but updating the bucket name according to your own setup.

2. Configure to your local setup.

```bash
cp example-config.yaml config.yaml
```

Update config.yaml with your own MinIO connection, access, and bucket details.

Update config.yaml with the agencies you're looking to scrape from.

3. Install dependencies.

```bash
pip install -r requirements.txt
```

4. Run the script.

```bash
python frscraper.py
```