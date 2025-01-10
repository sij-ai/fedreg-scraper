import requests
import json
import yaml
from io import BytesIO
from minio import Minio
import os

def load_config(config_path="config.yaml"):
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

def load_existing_abstracts(client, bucket_name, abstracts_path):
    """
    Attempts to load existing abstracts JSON from MinIO.
    If the file does not exist, returns an empty dictionary.
    """
    try:
        data = client.get_object(bucket_name, abstracts_path)
        existing_data = data.read().decode('utf-8')
        return json.loads(existing_data)
    except Exception:
        return {}

def save_abstracts_to_minio(client, bucket_name, abstracts_path, abstracts_dict):
    """
    Uploads the updated abstracts JSON to MinIO, appending new data.
    """
    abstracts_json = json.dumps(abstracts_dict)
    abstracts_data = BytesIO(abstracts_json.encode())
    client.put_object(
        bucket_name,
        abstracts_path,
        abstracts_data,
        length=len(abstracts_json),
        content_type='application/json'
    )

def main():
    # Load config
    config = load_config("config.yaml")
    
    # Extract MinIO config
    minio_config = config["minio"]
    endpoint = minio_config["endpoint"]
    access_key = minio_config["access_key"]
    secret_key = minio_config["secret_key"]
    region = minio_config["region"]
    secure = minio_config["secure"]
    
    # Extract bucket name, parent folder, and agencies
    bucket_name = config["bucket_name"]
    parent_folder = config["parent_folder"]
    agencies_to_scrape = config["agencies"]

    # Set up MinIO client
    client = Minio(
        endpoint,
        access_key=access_key,
        secret_key=secret_key,
        region=region,
        secure=secure
    )

    # Load existing abstracts (if any)
    abstracts_path = f"{parent_folder}/abstracts.json"
    existing_abstracts = load_existing_abstracts(client, bucket_name, abstracts_path)

    # Fetch full list of agencies from Federal Register
    agency_api_url = 'https://www.federalregister.gov/api/v1/agencies'
    response = requests.get(agency_api_url)
    all_fr_agencies = response.json()

    # Sort agencies by their short name or name
    all_fr_agencies.sort(key=lambda x: x['short_name'] if x['short_name'] else x['name'])
    
    # We will track newly fetched abstracts in this dict
    new_abstracts = {}

    # Iterate through agencies from the config
    for agency_keyword in agencies_to_scrape:
        # Try to match with the FR agencies list
        matched_agency = None
        for a in all_fr_agencies:
            # If the agency's short_name is not None, check it; otherwise, check its name
            short_name_or_name = a['short_name'] if a['short_name'] else a['name']
            if short_name_or_name.lower() == agency_keyword.lower():
                matched_agency = a
                break
            # Some agencies might have no short_name, so we also check name in either case
            if a['name'].lower() == agency_keyword.lower():
                matched_agency = a
                break
        
        if not matched_agency:
            print(f"No agency match found for '{agency_keyword}'. Skipping.")
            continue
        
        short_name = matched_agency['short_name'] if matched_agency['short_name'] else matched_agency['name']
        agency_name = matched_agency['name']

        print(f"\n--- Scraping documents for: {agency_name} ---")
        
        # Fetch documents for the chosen agency
        next_page_url = matched_agency['recent_articles_url']
        
        # Loop to handle pagination
        while next_page_url:
            print(f"Fetching documents from {next_page_url}...")
            response = requests.get(next_page_url)
            data = response.json()
            next_page_url = data.get('next_page_url', None)

            # Loop through the documents and download PDFs if not already uploaded
            for result in data['results']:
                pdf_url = result['pdf_url']
                document_number = result['document_number']
                abstract = result['abstract']
                title = result['title']
                publication_date = result['publication_date']

                # Build the PDF filename
                truncated_title = title[:30] + "..." if len(title) > 30 else title
                pdf_filename = f"{document_number} - {truncated_title}.pdf".replace('/', '_')
                # Build the object path for MinIO, using the folder structure
                pdf_object_path = f"{parent_folder}/{short_name}/{pdf_filename}"

                # Check if the object already exists on MinIO
                try:
                    client.stat_object(bucket_name, pdf_object_path)
                    print(f"Skipping {pdf_filename}, already exists on MinIO.")
                except Exception:
                    # Download and upload
                    print(f"Downloading and uploading {pdf_filename}...")
                    pdf_response = requests.get(pdf_url)
                    pdf_data = BytesIO(pdf_response.content)
                    client.put_object(
                        bucket_name,
                        pdf_object_path,
                        pdf_data,
                        length=len(pdf_response.content),
                        content_type='application/pdf'
                    )

                # Collect abstract data (will update or add new entries)
                existing_abstract_entry = existing_abstracts.get(document_number, {})
                existing_abstract_entry.update({
                    'abstract': abstract,
                    'title': title,
                    'publication_date': publication_date,
                    'agency_name': agency_name,
                    'pdf_path': pdf_object_path
                })
                existing_abstracts[document_number] = existing_abstract_entry

    # Finally, upload the updated abstracts to MinIO
    save_abstracts_to_minio(client, bucket_name, abstracts_path, existing_abstracts)
    print("\nAll done.")

if __name__ == "__main__":
    main()
