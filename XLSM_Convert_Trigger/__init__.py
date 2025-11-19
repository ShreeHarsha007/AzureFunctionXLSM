import logging
import azure.functions as func
import os
import requests
import json
import io
from openpyxl import load_workbook
from azure.storage.blob import BlobServiceClient, BlobSasPermissions, generate_blob_sas
from datetime import datetime, timedelta, timezone


# =======================
# CONFIGURATION
# =======================
STORAGE_CONN_STR = os.environ.get("AzureWebJobsStorage")
OUTPUT_CONTAINER_NAME = "xlsx-output"   # MUST EXIST IN STORAGE


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('XLSM Converter function started.')

    try:
        # ------------------------------
        # 1. Parse Request Body
        # ------------------------------
        try:
            req_body = req.get_json()
            xlsm_url = req_body.get('xlsm_url')
        except:
            return func.HttpResponse(
                "Request must contain JSON body with 'xlsm_url'",
                status_code=400
            )

        if not xlsm_url or not xlsm_url.lower().endswith(".xlsm"):
            return func.HttpResponse(
                "Invalid or missing 'xlsm_url'. Must be a direct URL ending in .xlsm",
                status_code=400
            )

        logging.info(f"Downloading XLSM from: {xlsm_url}")

        # ------------------------------
        # 2. Download XLSM file
        # ------------------------------
        response = requests.get(xlsm_url, allow_redirects=True)
        response.raise_for_status()

        xlsm_bytes = io.BytesIO(response.content)

        # ------------------------------
        # 3. Convert XLSM → XLSX
        # ------------------------------
        logging.info("Converting file using openpyxl...")

        wb = load_workbook(xlsm_bytes, data_only=True)

        xlsx_buffer = io.BytesIO()
        wb.save(xlsx_buffer)
        xlsx_buffer.seek(0)

        logging.info("Conversion successful.")

        # ------------------------------
        # 4. Upload XLSX to Blob Storage
        # ------------------------------
        if not STORAGE_CONN_STR:
            raise ValueError("AzureWebJobsStorage is missing in App Settings.")

        blob_service = BlobServiceClient.from_connection_string(STORAGE_CONN_STR)

        # Extract original name
        file_name = os.path.basename(xlsm_url)
        base_name = os.path.splitext(file_name)[0]

        final_blob_name = f"converted/{base_name}_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}.xlsx"

        blob_client = blob_service.get_blob_client(
            container=OUTPUT_CONTAINER_NAME,
            blob=final_blob_name
        )

        blob_client.upload_blob(xlsx_buffer, overwrite=True)
        logging.info(f"Uploaded to blob: {final_blob_name}")

        # ------------------------------
        # 5. Generate SAS URL
        # ------------------------------
        # Extract AccountName + AccountKey from connection string
        parts = {
            kv.split('=')[0]: kv.split('=')[1]
            for kv in STORAGE_CONN_STR.split(';')
            if '=' in kv
        }

        account_name = parts.get("AccountName")
        account_key = parts.get("AccountKey")

        if not account_key:
            raise ValueError("AccountKey missing in AzureWebJobsStorage connection string.")

        sas_token = generate_blob_sas(
            account_name=account_name,
            container_name=OUTPUT_CONTAINER_NAME,
            blob_name=final_blob_name,
            account_key=account_key,
            permission=BlobSasPermissions(read=True),
            expiry=datetime.now(timezone.utc) + timedelta(hours=1)
        )

        download_url = f"https://{account_name}.blob.core.windows.net/{OUTPUT_CONTAINER_NAME}/{final_blob_name}?{sas_token}"

        # ------------------------------
        # 6. Return Response
        # ------------------------------
        return func.HttpResponse(
            json.dumps({"status": "success", "converted_url": download_url}),
            mimetype="application/json",
            status_code=200
        )

    except requests.HTTPError as e:
        logging.error(f"HTTP error while downloading XLSM: {e}")
        return func.HttpResponse(
            f"Download failed: {e.response.status_code}. Check URL/SAS token.",
            status_code=400
        )

    except Exception as e:
        logging.exception(f"Function failed: {e}")
        return func.HttpResponse(
            f"Internal error: {e}",
            status_code=500
        )
