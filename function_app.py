import logging
import os
import tempfile
import requests
from datetime import datetime, timedelta

import azure.functions as func
from openpyxl import load_workbook
from azure.storage.blob import (
    BlobServiceClient,
    generate_blob_sas,
    BlobSasPermissions
)

app = func.FunctionApp()

@app.function_name(name="convertxlsm")
@app.route(route="convertxlsm", methods=["POST"])
def convert_xlsm(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("convertxlsm function started")

    try:
        # Parse JSON
        data = req.get_json()
        file_url = data.get("url")

        if not file_url:
            return func.HttpResponse("Missing 'url' in request.", status_code=400)

        # Download XLSM file
        try:
            response = requests.get(file_url, timeout=30)
            response.raise_for_status()
        except Exception as e:
            logging.exception("Failed to download XLSM")
            return func.HttpResponse(f"Download error: {str(e)}", status_code=500)

        # Save to temporary file
        temp_xlsm = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsm")
        temp_xlsm.write(response.content)
        temp_xlsm.close()

        # Convert XLSM → XLSX
        try:
            wb = load_workbook(temp_xlsm.name, data_only=True)
        except Exception as e:
            logging.exception("openpyxl failed to load XLSM")
            return func.HttpResponse(f"openpyxl error: {str(e)}", status_code=500)

        temp_xlsx = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx")
        wb.save(temp_xlsx.name)

        # Load environment variables
        conn_str = os.getenv("STORAGE_CONNECTION_STRING")
        account_name = os.getenv("STORAGE_ACCOUNT_NAME")
        account_key = os.getenv("STORAGE_ACCOUNT_KEY")
        container_name = os.getenv("CONVERTED_CONTAINER", "converted")

        if not conn_str or not account_name or not account_key:
            return func.HttpResponse(
                "Azure storage settings missing.",
                status_code=500
            )

        blob_service = BlobServiceClient.from_connection_string(conn_str)

        # Generate new filename
        import urllib.parse as up
        parsed = up.urlparse(file_url)
        original_name = os.path.basename(parsed.path)
        xlsx_name = original_name.replace(".xlsm", ".xlsx")

        # Upload XLSX to blob
        blob_client = blob_service.get_blob_client(
            container=container_name,
            blob=xlsx_name
        )

        with open(temp_xlsx.name, "rb") as f:
            blob_client.upload_blob(f, overwrite=True)

        # SAS URL generation
        sas = generate_blob_sas(
            account_name=account_name,
            container_name=container_name,
            blob_name=xlsx_name,
            account_key=account_key,
            permission=BlobSasPermissions(read=True),
            expiry=datetime.utcnow() + timedelta(hours=1)
        )

        xlsx_url = (
            f"https://{account_name}.blob.core.windows.net/"
            f"{container_name}/{xlsx_name}?{sas}"
        )

        # Cleanup temp files
        os.unlink(temp_xlsm.name)
        os.unlink(temp_xlsx.name)

        return func.HttpResponse(
            f'{{"status":"success","xlsx_url":"{xlsx_url}"}}',
            mimetype="application/json",
            status_code=200
        )

    except Exception as e:
        logging.exception("Unhandled error")
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)
