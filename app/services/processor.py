import logging
import csv
import hashlib
from io import TextIOWrapper
from abc import ABC, abstractmethod
from fastapi import UploadFile
from typing import Dict, Any
from decimal import Decimal
from datetime import datetime
from uuid import UUID
import os
from app.schemas.charge_notification import ChargeNotification
from app.models import CSVFile, ChargeRow, ChargeStatus
from app.db import SessionLocal
from sqlalchemy.dialects.postgresql import insert
from app.tasks import process_charge  # Import the celery task

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler()]
)

logger = logging.getLogger(__name__)


class FileProcessor(ABC):
    @abstractmethod
    async def process(self, file: UploadFile) -> Dict[str, Any]:
        pass


class CSVProcessor(FileProcessor):
    def __init__(self):
        self.BATCH_SIZE = int(os.getenv("CSV_BATCH_SIZE", 1000))

    async def process(self, file: UploadFile) -> dict:
        """
        Optimized CSV processor that stream-processes the file, computes its fingerprint,
        and inserts rows in bulk batches for high performance. After inserting, it enqueues
        celery tasks to process each pending row.
        """
        stats = {
            "total_rows": 0,
            "processed_rows": 0,
            "failed_rows": 0,
            "errors": []
        }
        session = SessionLocal()
        try:
            # Compute file fingerprint incrementally.
            md5 = hashlib.md5()
            while True:
                chunk = await file.read(8192)
                if not chunk:
                    break
                md5.update(chunk)
            fingerprint = md5.hexdigest()
            file.file.seek(0)  # Reset pointer to beginning.
            
            # Check for duplicate file via fingerprint.
            existing_csv = session.query(CSVFile).filter(CSVFile.fingerprint == fingerprint).first()
            if existing_csv:
                raise ValueError("This CSV file has already been processed.")
            
            # Create a record for the CSV file.
            csv_file = CSVFile(filename=file.filename, fingerprint=fingerprint)
            session.add(csv_file)
            session.commit()
            
            # Create a text stream wrapper (reading line by line in UTF-8).
            text_stream = TextIOWrapper(file.file, encoding='utf-8')
            csv_reader = csv.DictReader(text_stream)
            
            batch_size = 10000  # Tune this value based on your environment.
            rows_batch = []
            
            for row_num, row in enumerate(csv_reader, start=1):
                stats["total_rows"] += 1
                try:
                    # Parse, validate, and prepare the row data.
                    row_data = {
                        "csv_file_id": csv_file.id,
                        "name": row["name"],
                        "government_id": row["governmentId"],
                        "email": row["email"],
                        "debt_amount": Decimal(row["debtAmount"]),
                        "debt_due_date": datetime.strptime(row["debtDueDate"], "%Y-%m-%d").date(),
                        "debt_id": str(UUID(row["debtId"].strip())),
                        "status": ChargeStatus.PENDING,
                    }
                    rows_batch.append(row_data)
                    
                    if len(rows_batch) >= batch_size:
                        stmt = insert(ChargeRow).values(rows_batch)
                        # Rely on the unique constraint for debt_id to ignore duplicates.
                        stmt = stmt.on_conflict_do_nothing(index_elements=["debt_id"])
                        session.execute(stmt)
                        session.commit()
                        stats["processed_rows"] += len(rows_batch)
                        rows_batch = []
                except Exception as e:
                    stats["failed_rows"] += 1
                    stats["errors"].append({
                        "row": row_num,
                        "error": str(e)
                    })
            
            # Insert any remaining rows.
            if rows_batch:
                stmt = insert(ChargeRow).values(rows_batch)
                stmt = stmt.on_conflict_do_nothing(index_elements=["debt_id"])
                session.execute(stmt)
                session.commit()
                stats["processed_rows"] += len(rows_batch)
            
            # Now query all rows for this CSV file that are still pending.
            pending_rows = session.query(ChargeRow).filter(
                ChargeRow.csv_file_id == csv_file.id,
                ChargeRow.status == ChargeStatus.PENDING
            ).all()
            for row in pending_rows:
                # Enqueue a celery task for each pending charge_row.
                process_charge.delay(str(row.id))
            
            logger.info("Enqueued %d celery tasks for CSV file %s", len(pending_rows), csv_file.id)
            return stats
        finally:
            session.close()


class ProcessorFactory:
    @staticmethod
    def get_processor(file_type: str) -> FileProcessor:
        processors = {"csv": CSVProcessor}
        
        processor_class = processors.get(file_type.lower())
        if not processor_class:
            raise ValueError(f"Unsupported file type: {file_type}")
            
        return processor_class()
