import logging
import azure.functions as func
import os
import requests
from openpyxl import load_workbook
from azure.storage.blob import BlobServiceClient, BlobSasPermissions, generate_blob_sas
from datetime import datetime, timedelta, timezone

# --- Configuration ---
# Uses the connection string set in the Azure Function App's settings
STORAGE_CONN_STR = os.environ.get("AzureWebJobsStorage") 
OUTPUT_CONTAINER_NAME = "xlsx-output" # Your target blob container

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('XLSM Converter function processing request.')

    # 1. Parse Input URL from Request Body
    try:
        req_body = req.get_json()
        xlsm_url = req_body.get('xlsm_url')
    except Exception:
        return func.HttpResponse("Please pass a JSON payload with 'xlsm_url'", status_code=400)

    if not xlsm_url or not xlsm_url.lower().endswith(".xlsm"):
        return func.HttpResponse("Invalid or missing 'xlsm_url'. Must end with .xlsm", status_code=400)

    # Define File Paths
    original_filename = xlsm_url.split('/')[-1]
    file_name_without_ext = os.path.splitext(original_filename)[0]
    
    # Use the function's temporary execution directory
    temp_xlsm_path = os.path.join(os.getcwd(), original_filename)
    temp_xlsx_path = os.path.join(os.getcwd(), file_name_without_ext + ".xlsx")
    output_blob_name = file_name_without_ext + ".xlsx"

    try:
        # 2. Download the XLSM file
        r = requests.get(xlsm_url, allow_redirects=True)
        r.raise_for_status() 
        with open(temp_xlsm_path, 'wb') as f:
            f.write(r.content)
            
        # 3. Convert XLSM to XLSX (The core logic)
        logging.info("Starting conversion with openpyxl (data_only=True)...")
        wb = load_workbook(temp_xlsm_path, data_only=True)
        wb.save(temp_xlsx_path)
        logging.info("Conversion successful.")

        # 4. Upload the new XLSX file to Blob Storage
        blob_service_client = BlobServiceClient.from_connection_string(STORAGE_CONN_STR)
        blob_client = blob_service_client.get_blob_client(
            container=OUTPUT_CONTAINER_NAME, 
            blob=output_blob_name
        )
        
        with open(temp_xlsx_path, "rb") as data:
            # Overwrite if the file already exists
            blob_client.upload_blob(data, overwrite=True) 
        
        # 5. Generate Secure SAS URL
        sas_token = generate_blob_sas(
            account_name=blob_service_client.account_name,
            container_name=OUTPUT_CONTAINER_NAME,
            blob_name=output_blob_name,
            account_key=blob_service_client.credential.account_key,
            permission=BlobSasPermissions(read=True),
            # Token expires after 60 minutes
            expiry=datetime.now(timezone.utc) + timedelta(minutes=60) 
        )
                
        output_url = f"{blob_client.url}?{sas_token}"
        
        # 6. Cleanup temporary files
        os.remove(temp_xlsm_path)
        os.remove(temp_xlsx_path)
        
        # 7. Return the secured URL
        return func.HttpResponse(
            f'{{"status": "success", "converted_url": "{output_url}"}}',
            mimetype="application/json",
            status_code=200
        )
        
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        return func.HttpResponse(
             f"Conversion or upload failed: {e}",
             status_code=500
        )