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
        data = req.get_json()
        file_url = data.get("url")

        if not file_url:
            return func.HttpResponse("Missing 'url' in request.", status_code=400)

        # Download XLSM
        response = requests.get(file_url)
        if response.status_code != 200:
            return func.HttpResponse(
                f"Failed to download file. Status: {response.status_code}",
                status_code=500
            )

        # Save XLSM to temp file
        temp_xlsm = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsm")
        temp_xlsm.write(response.content)
        temp_xlsm.close()

        # Convert XLSM → XLSX
        wb = load_workbook(temp_xlsm.name, data_only=True)
        temp_xlsx = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx")
        wb.save(temp_xlsx.name)

        # Upload XLSX to Blob
        conn_str = os.getenv("STORAGE_CONNECTION_STRING")
        account_name = os.getenv("STORAGE_ACCOUNT_NAME")
        account_key = os.getenv("STORAGE_ACCOUNT_KEY")
        container_name = os.getenv("CONVERTED_CONTAINER", "converted")

        blob_service = BlobServiceClient.from_connection_string(conn_str)

        import urllib.parse as up
        parsed = up.urlparse(file_url)
        original = os.path.basename(parsed.path)
        xlsx_name = original.replace(".xlsm", ".xlsx")

        blob_client = blob_service.get_blob_client(
            container=container_name,
            blob=xlsx_name
        )

        with open(temp_xlsx.name, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)

        # Generate SAS URL for XLSX
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

        # Cleanup
        os.unlink(temp_xlsm.name)
        os.unlink(temp_xlsx.name)

        return func.HttpResponse(
            f'{{"status":"success","xlsx_url":"{xlsx_url}"}}',
            status_code=200,
            mimetype="application/json"
        )

    except Exception as e:
        logging.exception("Conversion error")
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)
