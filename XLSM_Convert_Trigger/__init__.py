import logging
import azure.functions as func
import os
import json
import io
from datetime import datetime, timezone
from urllib.parse import urlparse, unquote

from openpyxl import load_workbook
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential

OUTPUT_CONTAINER_NAME = "xlsx-output"

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("XLSM Converter function started.")

    try:
        body = req.get_json()
        xlsm_url = body.get("xlsm_url")

        if not xlsm_url or not xlsm_url.lower().endswith(".xlsm"):
            return func.HttpResponse(
                "Invalid or missing 'xlsm_url'. Must be blob URL ending with .xlsm",
                status_code=400
            )

        parsed = urlparse(xlsm_url)
        account_url = f"{parsed.scheme}://{parsed.netloc}"

        parts = parsed.path.lstrip("/").split("/")
        container_name = parts[0]
        blob_name = "/".join(parts[1:])

        credential = DefaultAzureCredential()
        blob_service = BlobServiceClient(account_url=account_url, credential=credential)

        blob_client = blob_service.get_blob_client(container=container_name, blob=blob_name)
        stream = blob_client.download_blob()
        xlsm_bytes = io.BytesIO(stream.readall())

        logging.info("Converting XLSM to XLSX...")

        wb = load_workbook(xlsm_bytes, data_only=True)
        xlsx_buffer = io.BytesIO()
        wb.save(xlsx_buffer)
        xlsx_buffer.seek(0)

        decoded_filename = unquote(os.path.basename(blob_name))
        base_name = os.path.splitext(decoded_filename)[0]

        final_blob_name = f"{base_name}_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}.xlsx"

        output_blob_client = blob_service.get_blob_client(
            container=OUTPUT_CONTAINER_NAME,
            blob=final_blob_name
        )

        output_blob_client.upload_blob(xlsx_buffer, overwrite=True)

        result_url = f"{account_url}/{OUTPUT_CONTAINER_NAME}/{final_blob_name}"

        return func.HttpResponse(
            json.dumps({
                "status": "success",
                "converted_url": result_url
            }),
            mimetype="application/json",
            status_code=200
        )

    except Exception as e:
        logging.exception("Conversion failed")
        return func.HttpResponse(
            f"Internal Error: {str(e)}",
            status_code=500
        )
