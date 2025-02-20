import requests
import json
import yaml
from io import BytesIO
from minio import Minio
import os
import argparse
import time
from datetime import datetime

def log_with_timestamp(message, level="INFO"):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    print(f"[{timestamp}] {level}: {message}")

def parse_args():
    parser = argparse.ArgumentParser(description='Scrape Federal Register documents')
    parser.add_argument('--all', action='store_true', 
                       help='Scrape all documents, even if some already exist')
    return parser.parse_args()

def load_config(config_path="config.yaml"):
    log_with_timestamp(f"Loading config from {config_path}")
    start_time = time.time()
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    log_with_timestamp(f"Config loaded in {(time.time() - start_time):.2f} seconds")
    return config

def load_existing_abstracts(client, bucket_name, abstracts_path):
    """
    Attempts to load existing abstracts JSON from MinIO.
    If the file does not exist, returns an empty dictionary.
    """
    start_time = time.time()
    log_with_timestamp(f"Attempting to load existing abstracts from {bucket_name}/{abstracts_path}")
    try:
        data = client.get_object(bucket_name, abstracts_path)
        existing_data = data.read().decode('utf-8')
        result = json.loads(existing_data)
        log_with_timestamp(f"Loaded {len(result)} existing abstracts in {(time.time() - start_time):.2f} seconds")
        return result
    except Exception as e:
        log_with_timestamp(f"No existing abstracts found: {str(e)}", "WARNING")
        return {}

def save_abstracts_to_minio(client, bucket_name, abstracts_path, abstracts_dict):
    """
    Uploads the updated abstracts JSON to MinIO, appending new data.
    """
    start_time = time.time()
    log_with_timestamp(f"Saving {len(abstracts_dict)} abstracts to MinIO")
    abstracts_json = json.dumps(abstracts_dict, indent=4)
    abstracts_data = BytesIO(abstracts_json.encode())
    client.put_object(
        bucket_name,
        abstracts_path,
        abstracts_data,
        length=len(abstracts_json),
        content_type='application/json'
    )
    log_with_timestamp(f"Abstracts saved in {(time.time() - start_time):.2f} seconds")

def main():
    script_start_time = time.time()
    log_with_timestamp("Starting Federal Register document scraper")
    
    # Parse command line arguments
    args = parse_args()
    log_with_timestamp(f"Running with --all={args.all}")
    
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
    
    log_with_timestamp(f"Will process {len(agencies_to_scrape)} agencies: {', '.join(agencies_to_scrape)}")

    # Set up MinIO client
    log_with_timestamp(f"Connecting to MinIO at {endpoint}")
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
    log_with_timestamp("Fetching agency list from Federal Register API")
    api_start_time = time.time()
    agency_api_url = 'https://www.federalregister.gov/api/v1/agencies'
    response = requests.get(agency_api_url)
    all_fr_agencies = response.json()
    log_with_timestamp(f"Retrieved {len(all_fr_agencies)} agencies in {(time.time() - api_start_time):.2f} seconds")

    # Sort agencies by their short name or name
    all_fr_agencies.sort(key=lambda x: x['short_name'] if x['short_name'] else x['name'])

    # Track overall statistics
    total_documents_processed = 0
    total_documents_skipped = 0
    total_documents_downloaded = 0
    
    # Iterate through agencies from the config
    for agency_keyword in agencies_to_scrape:
        agency_start_time = time.time()
        
        # Try to match with the FR agencies list
        matched_agency = None
        for a in all_fr_agencies:
            short_name_or_name = a['short_name'] if a['short_name'] else a['name']
            if short_name_or_name.lower() == agency_keyword.lower():
                matched_agency = a
                break
        
        if not matched_agency:
            log_with_timestamp(f"No agency match found for '{agency_keyword}'. Skipping.", "WARNING")
            continue
        
        short_name = matched_agency['short_name'] if matched_agency['short_name'] else matched_agency['name']
        agency_name = matched_agency['name']

        log_with_timestamp(f"Processing agency: {agency_name}")
        
        # Track per-agency statistics
        agency_docs_processed = 0
        agency_docs_skipped = 0
        agency_docs_downloaded = 0
        
        # Fetch documents for the chosen agency
        next_page_url = matched_agency['recent_articles_url']
        page_number = 1
        
        # Loop to handle pagination
        while next_page_url:
            page_start_time = time.time()
            log_with_timestamp(f"Fetching page {page_number} from {next_page_url}")
            
            response = requests.get(next_page_url)
            if response.status_code != 200:
                log_with_timestamp(f"Error fetching documents: {response.status_code}", "ERROR")
                break
                
            data = response.json()
            next_page_url = data.get('next_page_url', None)
            
            log_with_timestamp(f"Retrieved {len(data['results'])} documents on page {page_number}")

            # Flag to track if we should move to next agency
            skip_to_next_agency = False

            # Loop through the documents and download PDFs if not already uploaded
            for result in data['results']:
                doc_start_time = time.time()
                
                pdf_url = result.get('pdf_url')
                document_number = result['document_number']
                title = result.get('title', 'Untitled')

                if not pdf_url:
                    log_with_timestamp(f"No PDF URL for document {document_number}", "WARNING")
                    continue

                # Build the PDF filename
                truncated_title = title[:30] + "..." if len(title) > 30 else title
                pdf_filename = f"{document_number} - {truncated_title}.pdf".replace('/', '_')
                pdf_object_path = f"{parent_folder}/{short_name}/{pdf_filename}"

                agency_docs_processed += 1
                total_documents_processed += 1

                # Check if the object already exists on MinIO
                try:
                    client.stat_object(bucket_name, pdf_object_path)
                    doc_time = time.time() - doc_start_time
                    log_with_timestamp(f"Found existing document: {pdf_filename} (checked in {doc_time:.2f}s)")
                    agency_docs_skipped += 1
                    total_documents_skipped += 1
                    
                    if not args.all:
                        log_with_timestamp(f"Stopping pagination for {agency_name} - existing documents found")
                        skip_to_next_agency = True
                        break
                    else:
                        log_with_timestamp("Continuing due to --all flag")
                    continue
                    
                except Exception:
                    download_start_time = time.time()
                    log_with_timestamp(f"Downloading {pdf_filename}")
                    
                    pdf_response = requests.get(pdf_url)
                    if pdf_response.status_code != 200:
                        log_with_timestamp(f"Error downloading {pdf_url}: {pdf_response.status_code}", "ERROR")
                        continue
                        
                    upload_start_time = time.time()
                    log_with_timestamp(f"Uploading {pdf_filename} to MinIO")
                    
                    pdf_data = BytesIO(pdf_response.content)
                    client.put_object(
                        bucket_name,
                        pdf_object_path,
                        pdf_data,
                        length=len(pdf_response.content),
                        content_type='application/pdf'
                    )
                    
                    agency_docs_downloaded += 1
                    total_documents_downloaded += 1
                    
                    doc_time = time.time() - doc_start_time
                    download_time = upload_start_time - download_start_time
                    upload_time = time.time() - upload_start_time
                    log_with_timestamp(
                        f"Processed {pdf_filename} in {doc_time:.2f}s "
                        f"(download: {download_time:.2f}s, upload: {upload_time:.2f}s)"
                    )

                # Update abstract data
                abstract = result.get('abstract', 'No abstract available.')
                publication_date = result.get('publication_date', 'Unknown')
                existing_abstract_entry = existing_abstracts.get(document_number, {})
                existing_abstract_entry.update({
                    'abstract': abstract,
                    'title': title,
                    'publication_date': publication_date,
                    'agency_name': agency_name,
                    'pdf_path': pdf_object_path
                })
                existing_abstracts[document_number] = existing_abstract_entry

            page_time = time.time() - page_start_time
            log_with_timestamp(f"Completed page {page_number} in {page_time:.2f} seconds")
            
            if skip_to_next_agency:
                break
                
            page_number += 1

        agency_time = time.time() - agency_start_time
        log_with_timestamp(
            f"Completed {agency_name} in {agency_time:.2f} seconds:\n"
            f"  - Documents processed: {agency_docs_processed}\n"
            f"  - Documents skipped: {agency_docs_skipped}\n"
            f"  - Documents downloaded: {agency_docs_downloaded}"
        )

    # Save final abstracts to MinIO
    save_abstracts_to_minio(client, bucket_name, abstracts_path, existing_abstracts)
    
    script_time = time.time() - script_start_time
    log_with_timestamp(
        f"Script completed in {script_time:.2f} seconds\n"
        f"Total statistics:\n"
        f"  - Documents processed: {total_documents_processed}\n"
        f"  - Documents skipped: {total_documents_skipped}\n"
        f"  - Documents downloaded: {total_documents_downloaded}"
    )

if __name__ == "__main__":
    main()
