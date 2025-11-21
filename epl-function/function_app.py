"""
Blob-triggered Excel splitter for Eagle Peak Logistics (Python v2 model)

What it does
------------
1) Triggers when a new .xlsx is uploaded to the 'ingestion/' container path.
2) Extracts YYYY/MM from the filename (first 'YYYY-MM-DD' in the name; falls back to UTC today).
3) For each worksheet:
   - Writes CSV to: ingestion/dsp_summary/{YYYY}/{MM}/{stem}__{sheet}.csv
   - Sheet names are sanitized (spaces and special chars -> '_').
4) Archives the original Excel to: ingestion/_archive/{YYYY}/{MM}/{filename}
   - Uses server-side copy and waits for success before deleting source.
5) Auth to data storage: Managed Identity + app setting alias:
   EPL_STORAGE__serviceUri = https://epldatastore01.blob.core.windows.net

Operational properties
----------------------
- Idempotent: CSV uploads use overwrite=True. Re-running on the same file is safe.
- Logging: emits INFO for each sheet and archive step; ERROR with traceback if anything fails.
- Safe to extend: set ACCEPTED_SHEETS to restrict which worksheets are exported.
"""

import io
import os
import re
import time
import logging
from datetime import datetime

import pandas as pd
import azure.functions as func
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

# ------------------ Configuration (adjust as needed) ------------------

# Container we read from / write to (paths below are inside this container)
CONTAINER_NAME = "ingestion"

# Output/Archive prefixes (virtual folders inside the same container)
OUT_PREFIX_TEMPLATE = "{file_type}/{year}/{month}/"
ARCHIVE_PREFIX_TEMPLATE = "_archive/{file_type}/{year}/{month}/"
ACCEPTED_SHEETS: set[str] | None = None

# OUT_PREFIX_TEMPLATE = "dsp_summary/{year}/{month}/"
# ARCHIVE_PREFIX_TEMPLATE = "_archive/{year}/{month}/"

# Binding/connection alias. We resolve this to an app setting:
# Preferred (no secrets): EPL_STORAGE__serviceUri = https://<account>.blob.core.windows.net
# Optional (temporary):   EPL_STORAGE = <full connection string>
CONNECTION_ALIAS = "EPL_STORAGE"

# If you want to export only certain sheets, list them here (exact match).
# Example: {"Solution", "Dispatch Plan"}  -> only those two will be exported.
ACCEPTED_SHEETS: set[str] | None = None  # None = export all sheets.


# ------------------ Helpers ------------------

def _get_blob_service_client() -> BlobServiceClient:
    """
    Create a BlobServiceClient using Managed Identity if serviceUri is set.
    Falls back to connection string if provided (useful for temporary key-based testing).
    """
    service_uri = os.getenv(f"{CONNECTION_ALIAS}__serviceUri")
    if service_uri:
        cred = DefaultAzureCredential()
        return BlobServiceClient(account_url=service_uri, credential=cred)

    conn_str = os.getenv(CONNECTION_ALIAS)
    if conn_str:
        return BlobServiceClient.from_connection_string(conn_str)

    raise RuntimeError(
        f"Storage connection not configured. Add app setting "
        f"'{CONNECTION_ALIAS}__serviceUri' (preferred, MI) or '{CONNECTION_ALIAS}' (conn string)."
    )
# 10/2 Files getting saved in month folder based on upload date, not file date.

def _extract_year_month(stem: str) -> tuple[str, str]:
    """
    Find the first YYYY-MM-DD in the filename stem; return (year, month).
    If none found or parse fails, use current UTC date.
    """
    # More flexible regex - doesn't rely on word boundaries
    m = re.search(r"(\d{4})-(\d{2})-(\d{2})", stem)
    if m:
        try:
            year = m.group(1)
            month = m.group(2)
            # Validate it's a real date
            datetime.strptime(f"{year}-{month}-01", "%Y-%m-%d")
            logging.info(f"Extracted date from filename: {year}-{month}")
            return year, month
        except Exception as e:
            logging.warning(f"Date parsing failed: {e}")
    
    # Fallback to current date
    now = datetime.utcnow()
    logging.warning(f"No valid date in filename '{stem}', using current: {now.year}-{now.month:02d}")
    return str(now.year), f"{now.month:02d}"

# def _extract_year_month(stem: str) -> tuple[str, str]:
#     """
#     Find the first YYYY-MM-DD in the filename stem; return (year, month).
#     If none found or parse fails, use current UTC date.
#     """
#     m = re.search(r"\b(\d{4}-\d{2}-\d{2})\b", stem)
#     if m:
#         try:
#             d = datetime.strptime(m.group(1), "%Y-%m-%d")
#             return str(d.year), f"{d.month:02d}"
#         except Exception:
#             pass
#     now = datetime.utcnow()
#     return str(now.year), f"{now.month:02d}"

# 10/1 Added _determine_file_type
def _determine_file_type(filename: str) -> tuple[str, str | None]:
    """
    Determine the file type and target sheet based on filename.
    
    Returns:
        (file_type_folder, target_sheet_name or None)
        
    Examples:
        "2025-02-10-DSK4-CYCLE_1-DSP-DayOfOpsPlan.xlsx" -> ("dsp_summary", None)
        "Routes_DSK4_2025-09-13_15_16 (EDT).xlsx" -> ("driver_routes", "Routes")
    """
    filename_lower = filename.lower()
    
    # Routes files from DSP Logistics portal
    if filename_lower.startswith("routes_") and "dsk4" in filename_lower:
        return ("driver_routes", "Routes")
    
    # DayOfOpsPlan files (Solution/Dispatch Plan)
    if "dayofopsplan" in filename_lower:
        return ("dsp_summary", None)  # None = all sheets
    
    # Default fallback
    logging.warning(f"Unknown file pattern: {filename}. Defaulting to dsp_summary.")
    return ("dsp_summary", None)


def _safe(name: str) -> str:
    """Sanitize sheet name for blob paths."""
    return "".join(c if (c.isalnum() or c in ("-", "_")) else "_" for c in name)


def _wait_for_copy_success(dest_blob_client, copy_id: str, timeout_s: int = 60) -> bool:
    """
    Poll the destination blob's copy status until 'success' or timeout.
    This avoids deleting the source before the server-side copy completes.
    """
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        props = dest_blob_client.get_blob_properties()
        status = (props.copy.status if props.copy else None)
        if status == "success":
            return True
        if status in {"aborted", "failed"}:
            return False
        time.sleep(1)
    return False


# ------------------ Function App & Trigger ------------------

app = func.FunctionApp()

@app.blob_trigger(
    arg_name="inputblob",
    path="ingestion/{name}.xlsx",   # path="ingestion/{name}.xlsx",
    connection="EPL_STORAGE",        #connection=CONNECTION_ALIAS             # resolves to EPL_STORAGE__serviceUri (Managed Identity)
    source="EventGrid"             # Added Oct 19 2025.
)
def split_excel(inputblob: func.InputStream) -> None:
    """
    Entry point for blob-triggered split & route.
    """
    try:
        full_path = inputblob.name  # e.g., "ingestion/2025-09-15-DSK4-...xlsx"
        # Blob name = path inside the container (strip the leading "ingestion/")
        blob_name = full_path.split("/", 1)[1] if "/" in full_path else full_path
        # Skip our own outputs to avoid recursion
        if blob_name.startswith("_archive/") or blob_name.startswith("dsp_summary/"):
            logging.info("Skipping internal path: %s", blob_name)
            return

        filename = os.path.basename(full_path).strip().rstrip("'").rstrip('"')

        # Skip Office temp/lock files and non-xlsx
        if filename.startswith("~$") or not filename.lower().endswith(".xlsx"):
            logging.info("Skipping non-xlsx or temp file: %s", filename)
            return

        stem = filename[:-5]  # strip ".xlsx"
        year, month = _extract_year_month(stem)
        
        # NEW: Determine file type and target sheet
        file_type, target_sheet = _determine_file_type(filename)
        logging.info(f"File type: {file_type}, Target sheet: {target_sheet or 'ALL'}")

        # Connect to storage
        bsc = _get_blob_service_client()
        container = bsc.get_container_client(CONTAINER_NAME)

        # Read workbook
        raw_bytes = inputblob.read()
        xls = pd.ExcelFile(io.BytesIO(raw_bytes), engine="openpyxl")

        # Determine which sheets to export
        sheet_names = xls.sheet_names or []
        
        # If target_sheet is specified, only export that one
        if target_sheet:
            if target_sheet in sheet_names:
                sheet_names = [target_sheet]
            else:
                logging.warning(f"Target sheet '{target_sheet}' not found in {filename}. Available: {sheet_names}")
                return
        elif ACCEPTED_SHEETS is not None:
            sheet_names = [s for s in sheet_names if s in ACCEPTED_SHEETS]
            
        if not sheet_names:
            logging.warning("No sheets to export for file: %s", filename)
            return

        # Output/Archive paths (now include file_type)
        out_prefix = OUT_PREFIX_TEMPLATE.format(file_type=file_type, year=year, month=month)
        archive_prefix = ARCHIVE_PREFIX_TEMPLATE.format(file_type=file_type, year=year, month=month)

        # stem = filename[:-5]  # strip ".xlsx"
        # year, month = _extract_year_month(stem)

        # # Connect to storage (Managed Identity → serviceUri; or connection string fallback)
        # bsc = _get_blob_service_client()
        # container = bsc.get_container_client(CONTAINER_NAME)

        # # Read workbook
        # raw_bytes = inputblob.read()
        # xls = pd.ExcelFile(io.BytesIO(raw_bytes), engine="openpyxl")

        # # Determine which sheets to export
        # sheet_names = xls.sheet_names or []
        # if ACCEPTED_SHEETS is not None:
        #     sheet_names = [s for s in sheet_names if s in ACCEPTED_SHEETS]
        # if not sheet_names:
        #     logging.warning("No sheets to export for file: %s", filename)

        # # Output/Archive paths
        # out_prefix = OUT_PREFIX_TEMPLATE.format(year=year, month=month)
        # archive_prefix = ARCHIVE_PREFIX_TEMPLATE.format(year=year, month=month)

        # Export sheets → CSV
        for sheet in sheet_names:
            df = pd.read_excel(xls, sheet_name=sheet)
            csv_bytes = df.to_csv(index=False).encode("utf-8")
            out_blob_name = f"{out_prefix}{stem}__{_safe(sheet)}.csv"

            container.upload_blob(
                name=out_blob_name,
                data=io.BytesIO(csv_bytes),
                overwrite=True,  # idempotent
                metadata={"source": "excel", "sheet": sheet}
            )
            logging.info("Wrote CSV: %s (rows=%d)", out_blob_name, len(df))

        # Archive original: server-side copy, wait for success, then delete
        archive_blob_name = f"{archive_prefix}{filename}"
        #src_blob = container.get_blob_client(full_path)
        src_blob = container.get_blob_client(blob_name)     # <-- blob name only
        dst_blob = container.get_blob_client(archive_blob_name)

        copy = dst_blob.start_copy_from_url(src_blob.url)
        copy_id = copy["copy_id"] if isinstance(copy, dict) else None
        if _wait_for_copy_success(dst_blob, copy_id or "", timeout_s=60):
            container.delete_blob(blob_name)                # <-- blob name only
            logging.info("Archived original to %s and deleted source %s",
                        archive_blob_name, full_path)
        else:
            logging.error("Archive copy did not succeed for %s; source not deleted.", full_path)

    except Exception as exc:
        logging.exception("Split/route failed for %s: %s", getattr(inputblob, "name", "<unknown>"), exc)
        # Raise so Functions runtime can retry per its default policy.
        raise

# --- Quick diagnostics endpoint: GET /api/diag ---
@app.route(route="diag", methods=["GET"])
def diag(req: func.HttpRequest) -> func.HttpResponse:
    try:
        # Show what alias value we resolved
        svc = os.getenv("EPL_STORAGE__serviceUri", "<missing>")
        bsc = _get_blob_service_client()
        container = bsc.get_container_client(CONTAINER_NAME)

        # Try to list a few blobs in the 'ingestion' container
        names = []
        for i, b in enumerate(container.list_blobs(name_starts_with=""), start=1):
            names.append(b.name)
            if i >= 5:
                break

        body = {
            "serviceUri": svc,
            "container": CONTAINER_NAME,
            "sample_blobs": names,
        }
        return func.HttpResponse(str(body), status_code=200)
    except Exception as e:
        logging.exception("Diag failed: %s", e)
        return func.HttpResponse(f"Diag error: {e}", status_code=500)
