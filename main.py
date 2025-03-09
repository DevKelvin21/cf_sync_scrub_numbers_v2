import csv
import logging
from io import StringIO
import os
import functions_framework
from flask import abort, Request

from google.cloud import firestore
from google.cloud import storage


def count_csv_rows(gs_path: str) -> int:
    """
    Downloads a CSV file from Cloud Storage (given a gs:// URL),
    counts the rows excluding the header, and returns the count.
    If any issue occurs or the file content is empty, returns 0.
    """
    if not gs_path.startswith("gs://"):
        logging.warning(f"Invalid path format: {gs_path}")
        return 0

    try:
        # Extract bucket and blob name from gs:// path
        path_parts = gs_path[5:].split("/", 1)
        bucket_name = path_parts[0]
        blob_name = path_parts[1] if len(path_parts) > 1 else ""
        if not blob_name:
            logging.warning(f"No blob name found in path: {gs_path}")
            return 0

        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)

        # Download the file content as text
        content = blob.download_as_text()
        if not content:
            return 0

        # Use StringIO to process the CSV content
        csv_file = StringIO(content)
        reader = csv.reader(csv_file)

        # Skip the header row and count the remaining rows
        header = next(reader, None)
        if header is None:
            return 0
        row_count = sum(1 for _ in reader)
        return row_count
    except Exception as e:
        logging.error(f"Error processing file {gs_path}: {e}")
        return 0


def process_documents():
    """
    Iterates over each document in the Firestore collection.
    For documents without a 'results' field, it:
      - Checks the blacklisted file path and counts CSV rows (setting results.dnc).
      - Checks the clean file path and counts CSV rows (setting results.clean).
      - Computes results.total as the sum of the two counts.
    The function updates the document with the new results.
    """
    db = firestore.Client()
    # Replace with your actual Firestore collection name
    collection_name = os.getenv("FIRESTORE_COLLECTION")
    docs = db.collection(collection_name).stream()

    for doc in docs:
        data = doc.to_dict()
        if "results" in data:
            logging.info(f"Skipping document {doc.id}: already processed.")
            continue

        output_files = data.get("outputFiles", {})

        # Process blacklisted file: if the path is provided, count rows; if not, set dnc to 0.
        blacklisted_path = output_files.get("blacklistedFilePath", "")
        results_dnc = count_csv_rows(blacklisted_path) if blacklisted_path else 0

        # Process clean file similarly.
        clean_path = output_files.get("cleanFilePath", "")
        results_clean = count_csv_rows(clean_path) if clean_path else 0

        results_total = results_dnc + results_clean

        results = {
            "dnc": results_dnc,
            "clean": results_clean,
            "total": results_total,
        }

        try:
            doc.reference.update({"results": results})
            logging.info(f"Updated document {doc.id} with results: {results}")
        except Exception as e:
            logging.error(f"Failed to update document {doc.id}: {e}")


@functions_framework.http
def main(request: Request):
    """
    HTTP Cloud Function entrypoint using Functions Framework.
    When triggered by an HTTP request, it processes all Firestore documents.
    """
    try:
        process_documents()
        return "Documents processed successfully.", 200
    except Exception as e:
        logging.error(f"Error processing documents: {e}")
        abort(500, description="Error processing documents")
