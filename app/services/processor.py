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


class FileProcessor(ABC):
    @abstractmethod
    async def process(self, file: UploadFile) -> Dict[str, Any]:
        pass


class CSVProcessor(FileProcessor):
    def __init__(self):
        self.BATCH_SIZE = int(os.getenv("CSV_BATCH_SIZE", 1000))

    async def process(self, file: UploadFile) -> Dict[str, Any]:
        stats = {
            "total_rows": 0,
            "processed_rows": 0,
            "failed_rows": 0,
            "errors": []
        }
        
        async for result in self._process_stream(file):
            stats["total_rows"] += 1
            if isinstance(result, ChargeNotification):
                stats["processed_rows"] += 1
            else:
                stats["failed_rows"] += 1
                stats["errors"].append({
                    "row": stats["total_rows"],
                    "error": str(result)
                })
        
        return stats

    async def _process_stream(
        self, file: UploadFile
    ) -> AsyncGenerator[ChargeNotification | Exception, None]:
        content = await file.read()
        text_io = StringIO(content.decode("utf-8"))
        csv_reader = csv.DictReader(text_io)
        
        row_count = 0
        for row_num, row in enumerate(csv_reader, start=1):
            row_count += 1
            print(f"Processing row {row_count}")
            print(f"Row data: {row}")
            try:
                processed_row = {
                    "name": row["name"],
                    "government_id": row["government_id"],
                    "email": row["email"],
                    "debt_amount": Decimal(row["debt_amount"]),
                    "debt_due_date": datetime.strptime(row["debt_due_date"], "%Y-%m-%d").date(),
                    "debt_id": UUID(row["debt_id"].strip())
                }
                
                charge = ChargeNotification(**processed_row)
                yield charge
                print(f"Successfully processed row {row_count}")
                
            except Exception as e:
                print(f"Failed to process row {row_count}: {str(e)}")
                yield e


class ProcessorFactory:
    @staticmethod
    def get_processor(file_type: str) -> FileProcessor:
        processors = {"csv": CSVProcessor}
        
        processor_class = processors.get(file_type.lower())
        if not processor_class:
            raise ValueError(f"Unsupported file type: {file_type}")
            
        return processor_class()
