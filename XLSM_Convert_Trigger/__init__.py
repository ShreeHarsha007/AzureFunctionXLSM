import logging
import azure.functions as func
import os
import requests
import json
import io
from openpyxl import load_workbook
from azure.storage.blob import BlobServiceClient, BlobSasPermissions, generate_blob_sas
from datetime import datetime, timedelta, timezone

# --- Configuration ---
# Uses the connection string set in the Azure Function App's settings
# Ensure this setting exists in your Function App configuration!
STORAGE_CONN_STR = os.environ.get("AzureWebJobsStorage") 
OUTPUT_CONTAINER_NAME = "xlsx-output" # Your target blob container name

def main(req: func.HttpRequest) -> func.HttpResponse:
    """
    HTTP Trigger function to download an XLSM file via URL,
    convert it to XLSX (extracting calculated values), upload it, 
    and return a secure SAS download URL.
    """
    logging.info('XLSM Converter function processing request.')

    # ----------------------------------------------------
    # CRITICAL: Debugging wrapper to catch startup errors
    # ----------------------------------------------------
    try:
        # 1. Parse Input URL from Request Body
        try:
            req_body = req.get_json()
            xlsm_url = req_body.get('xlsm_url')
        except ValueError:
            return func.HttpResponse(
                 "Please pass a JSON payload with 'xlsm_url'",
                 status_code=400
            )

        if not xlsm_url or not xlsm_url.lower().endswith(".xlsm"):
            return func.HttpResponse(
                "Invalid or missing 'xlsm_url'. Must be a URL ending with .xlsm",
                status_code=400
            )

        # Define File Paths and Names
        original_filename = xlsm_url.split('/')[-1]
        file_name_without_ext = os.path.splitext(original_filename)[0]
        output_blob_name = f"converted/{file_name_without_ext}_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}.xlsx"
        
        # 2. Download the XLSM file
        logging.info(f"Downloading file from: {xlsm_url}")
        
        # Use io.BytesIO to keep files entirely in memory, avoiding local disk issues
        r = requests.get(xlsm_url, allow_redirects=True)
        r.raise_for_status() 
        xlsm_in_memory = io.BytesIO(r.content)
        
        # 3. Convert XLSM to XLSX (The core logic)
        logging.info("Starting conversion with openpyxl (data_only=True)...")
        # Load workbook from memory, preserving calculated values only
        wb = load_workbook(xlsm_in_memory, data_only=True)
        
        # Save the new XLSX file to a new memory buffer
        xlsx_out_memory = io.BytesIO()
        wb.save(xlsx_out_memory)
        xlsx_out_memory.seek(0)
        logging.info("Conversion successful and file saved to memory.")

        # 4. Upload the new XLSX file to Blob Storage
        if not STORAGE_CONN_STR:
             raise ValueError("AzureWebJobsStorage connection string is not set.")

        blob_service_client = BlobServiceClient.from_connection_string(STORAGE_CONN_STR)
        blob_client = blob_service_client.get_blob_client(
            container=OUTPUT_CONTAINER_NAME, 
            blob=output_blob_name
        )
        
        # Upload the file from the memory buffer
        blob_client.upload_blob(xlsx_out_memory, overwrite=True) 
        logging.info(f"Upload successful to blob: {output_blob_name}")
        
        # 5. Generate Secure SAS URL for download
        # Note: Access to account_key is needed for generate_blob_sas
        account_key = blob_service_client.credential.account_key
        if not account_key:
             raise ValueError("Account key could not be retrieved from connection string.")

        sas_token = generate_blob_sas(
            account_name=blob_service_client.account_name,
            container_name=OUTPUT_CONTAINER_NAME,
            blob_name=output_blob_name,
            account_key=account_key,
            permission=BlobSasPermissions(read=True),
            # Token expires after 60 minutes
            expiry=datetime.now(timezone.utc) + timedelta(minutes=60) 
        )
                
        output_url = f"{blob_client.url}?{sas_token}"
        
        # 6. Return the secured URL
        return func.HttpResponse(
            json.dumps({"status": "success", "converted_url": output_url}),
            mimetype="application/json",
            status_code=200
        )
        
    except requests.HTTPError as e:
        logging.error(f"HTTP Error during download: {e}. Status code: {e.response.status_code}")
        return func.HttpResponse(
             f"Download failed due to HTTP Error: {e.response.status_code}. Check XLSM URL/SAS token.",
             status_code=400
        )
    except Exception as e:
        # --- CRITICAL: Log the full traceback for debugging ---
        logging.error(f"Execution Failed: {e}")
        logging.exception(e) 
        # ------------------------------------------------------
        
        return func.HttpResponse(
             f"Internal conversion or storage error. See Application Insights logs for details. Error: {e}",
             status_code=500
        )