import logging
from abc import ABC, abstractmethod
from fastapi import UploadFile
from io import StringIO
from typing import AsyncGenerator, Dict, Any, List
import csv
from app.schemas.charge_notification import ChargeNotification
from decimal import Decimal
from datetime import datetime
from uuid import UUID
import os
from app.models import CSVFile, ChargeRow, ChargeStatus
from app.db import SessionLocal

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
        stats = {
            "total_rows": 0,
            "processed_rows": 0,
            "failed_rows": 0,
            "errors": []
        }
        session = SessionLocal()
        try:
            # Create a new CSVFile record:
            csv_file = CSVFile(filename=file.filename)
            session.add(csv_file)
            session.commit()

            async for result in self._process_stream(file):
                stats["total_rows"] += 1
                if isinstance(result, ChargeNotification):
                    stats["processed_rows"] += 1
                    # Create and persist a ChargeRow linked to the csv_file.
                    charge_row = ChargeRow(
                        csv_file_id=csv_file.id,
                        name=result.name,
                        government_id=result.government_id,
                        email=result.email,
                        debt_amount=result.debt_amount,
                        debt_due_date=result.debt_due_date,
                        debt_id=str(result.debt_id),
                        status=ChargeStatus.PENDING
                    )
                    session.add(charge_row)
                    session.commit()
                else:
                    stats["failed_rows"] += 1
                    stats["errors"].append({
                        "row": stats["total_rows"],
                        "error": str(result)
                    })
            return stats
        finally:
            session.close()

    async def _process_stream(self, file: UploadFile) -> AsyncGenerator[ChargeNotification | Exception, None]:
        content = await file.read()
        text_io = StringIO(content.decode("utf-8"))
        csv_reader = csv.DictReader(text_io)
    
        for _, row in enumerate(csv_reader, start=1):
            try:
                processed_row = {
                    "name": row["name"],
                    "government_id": row["governmentId"],
                    "email": row["email"],
                    "debt_amount": Decimal(row["debtAmount"]),
                    "debt_due_date": datetime.strptime(row["debtDueDate"], "%Y-%m-%d").date(),
                    "debt_id": UUID(row["debtId"].strip())
                }
                charge = ChargeNotification(**processed_row)
                yield charge
            except Exception as e:
                yield e


class ProcessorFactory:
    @staticmethod
    def get_processor(file_type: str) -> FileProcessor:
        processors = {"csv": CSVProcessor}
        
        processor_class = processors.get(file_type.lower())
        if not processor_class:
            raise ValueError(f"Unsupported file type: {file_type}")
            
        return processor_class()
