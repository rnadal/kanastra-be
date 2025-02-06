from abc import ABC, abstractmethod
from fastapi import UploadFile
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
        stats = {"total_rows": 0, "processed_rows": 0, "failed_rows": 0, "errors": []}

        async for result in self._process_stream(file):
            stats["total_rows"] += 1
            if isinstance(result, ChargeNotification):
                stats["processed_rows"] += 1
            else:
                stats["failed_rows"] += 1
                stats["errors"].append({"row": stats["total_rows"], "error": str(result)})

        return stats

    async def _process_stream(
        self, file: UploadFile
    ) -> AsyncGenerator[ChargeNotification | Exception, None]:
        text_io = StringIO(await file.read().decode("utf-8"))
        csv_reader = csv.DictReader(text_io)

        buffer = []
        for row_num, row in enumerate(csv_reader, start=1):
            try:
                row["debt_amount"] = Decimal(row["debt_amount"])
                row["debt_due_date"] = datetime.strptime(row["debt_due_date"], "%Y-%m-%d").date()
                row["debt_id"] = UUID(row["debt_id"])

                charge = ChargeNotification(**row)
                yield charge

            except Exception as e:
                yield e

            buffer.append(row)
            if len(buffer) >= self.BATCH_SIZE:
                buffer = []


class ProcessorFactory:
    @staticmethod
    def get_processor(file_type: str) -> FileProcessor:
        processors = {"csv": CSVProcessor}

        processor_class = processors.get(file_type.lower())
        if not processor_class:
            raise ValueError(f"Unsupported file type: {file_type}")

        return processor_class()
