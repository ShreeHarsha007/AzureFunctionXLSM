import logging
import azure.functions as func
import os
import requests
import json
import io
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse, unquote

from openpyxl import load_workbook
from azure.storage.blob import (
    BlobServiceClient,
    BlobSasPermissions,
    generate_blob_sas
)


STORAGE_CONN_STR = os.environ.get("AzureWebJobsStorage")
OUTPUT_CONTAINER_NAME = "xlsx-output"  


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("XLSM Converter function started.")

    try:
        try:
            body = req.get_json()
            xlsm_url = body.get("xlsm_url")
        except Exception:
            return func.HttpResponse(
                "Request must contain JSON with field 'xlsm_url'",
                status_code=400
            )

        # Validate file extension
        parsed = urlparse(xlsm_url or "")
        path = parsed.path

        if not xlsm_url or not path.lower().endswith(".xlsm"):
            return func.HttpResponse(
                "Invalid or missing 'xlsm_url'. Must end with .xlsm",
                status_code=400
            )

        logging.info(f"Downloading XLSM from URL: {xlsm_url}")

        response = requests.get(xlsm_url, allow_redirects=True)
        response.raise_for_status()

        xlsm_bytes = io.BytesIO(response.content)

        logging.info("Converting XLSM to XLSX...")

        wb = load_workbook(xlsm_bytes, data_only=True)

        xlsx_buffer = io.BytesIO()
        wb.save(xlsx_buffer)
        xlsx_buffer.seek(0)

        logging.info("Conversion successful.")

        if not STORAGE_CONN_STR:
            raise ValueError("AzureWebJobsStorage missing in Function App Settings.")

        blob_service = BlobServiceClient.from_connection_string(STORAGE_CONN_STR)

        decoded_filename = unquote(os.path.basename(path))
        base_name = os.path.splitext(decoded_filename)[0]

        final_blob_name = (
            f"{base_name}_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}.xlsx"
        )

        blob_client = blob_service.get_blob_client(
            container=OUTPUT_CONTAINER_NAME,
            blob=final_blob_name
        )

        blob_client.upload_blob(xlsx_buffer, overwrite=True)

        logging.info(f"Uploaded XLSX → {final_blob_name}")

        account_name = blob_service.account_name
        account_key = blob_service.credential.account_key

        sas_token = generate_blob_sas(
            account_name=account_name,
            container_name=OUTPUT_CONTAINER_NAME,
            blob_name=final_blob_name,
            account_key=account_key,
            permission=BlobSasPermissions(read=True),
            expiry=datetime.now(timezone.utc) + timedelta(hours=1),
            protocol="https",
            version="2020-02-10"     
        )

        download_url = (
            f"https://{account_name}.blob.core.windows.net/"
            f"{OUTPUT_CONTAINER_NAME}/{final_blob_name}?{sas_token}"
        )

        # --------------------------------------------------
        # 6. Return Final Response
        # --------------------------------------------------
        return func.HttpResponse(
            json.dumps({
                "status": "success",
                "converted_url": download_url
            }),
            mimetype="application/json",
            status_code=200
        )

    except requests.HTTPError as e:
        logging.error(f"Download failed: {e}")
        return func.HttpResponse(
            f"Download error: {e.response.status_code}",
            status_code=400
        )

    except Exception as e:
        logging.exception(f"Conversion failed: {e}")
        return func.HttpResponse(
            f"Internal Error: {str(e)}",
            status_code=500
        )
