import pytest
from fastapi import UploadFile
from app.services.processor import ProcessorFactory, CSVProcessor
from app.schemas.charge_notification import ChargeNotification
from decimal import Decimal
from datetime import date
from uuid import UUID
import io

def create_mock_file(content: str) -> UploadFile:
    return UploadFile(
        filename="test.csv",
        file=io.BytesIO(content.encode('utf-8')),
        headers={"Content-Type": "text/csv"}
    )

@pytest.fixture
def valid_csv_content():
    return (
        "name,government_id,email,debt_amount,debt_due_date,debt_id\n"
        "John Doe,11111111111,john@example.com,1000.00,2023-01-01,550e8400-e29b-41d4-a716-446655440000"
    )

@pytest.fixture
def invalid_csv_content():
    return (
        "name,government_id,email,debt_amount,debt_due_date,debt_id\n"
        "John Doe,invalid_id,invalid_email,invalid_amount,invalid_date,invalid_uuid"
    )

class TestProcessorFactory:
    def test_get_processor_csv(self):
        processor = ProcessorFactory.get_processor("csv")
        assert isinstance(processor, CSVProcessor)

    def test_get_processor_unsupported(self):
        with pytest.raises(ValueError, match="Unsupported file type: txt"):
            ProcessorFactory.get_processor("txt")

class TestCSVProcessor:
    @pytest.mark.asyncio
    async def test_process_valid_csv(self, valid_csv_content):
        processor = CSVProcessor()
        file = create_mock_file(valid_csv_content)
        print(file)
        
        result = await processor.process(file)
        
        assert result["total_rows"] == 1
        assert result["processed_rows"] == 1
        assert result["failed_rows"] == 0
        assert len(result["errors"]) == 0

    @pytest.mark.asyncio
    async def test_process_invalid_csv(self, invalid_csv_content):
        processor = CSVProcessor()
        file = create_mock_file(invalid_csv_content)
        
        result = await processor.process(file)
        
        assert result["total_rows"] == 1
        assert result["processed_rows"] == 0
        assert result["failed_rows"] == 1
        assert len(result["errors"]) == 1
        assert result["errors"][0]["row"] == 1

    @pytest.mark.asyncio
    async def test_process_empty_csv(self):
        processor = CSVProcessor()
        file = create_mock_file("name,government_id,email,debt_amount,debt_due_date,debt_id\n")
        
        result = await processor.process(file)
        
        assert result["total_rows"] == 0
        assert result["processed_rows"] == 0
        assert result["failed_rows"] == 0
        assert len(result["errors"]) == 0

    @pytest.mark.asyncio
    async def test_process_multiple_rows(self):
        content = (
            "name,government_id,email,debt_amount,debt_due_date,debt_id\n"
            "John Doe,11111111111,john@example.com,1000.00,2023-01-01,550e8400-e29b-41d4-a716-446655440000\n"
            "Jane Doe,22222222222,jane@example.com,2000.00,2023-01-02,650e8400-e29b-41d4-a716-446655440000\n"
            "Invalid,invalid,,invalid,invalid,invalid"
        )
        
        processor = CSVProcessor()
        file = create_mock_file(content)
        
        result = await processor.process(file)
        
        assert result["total_rows"] == 3
        assert result["processed_rows"] == 2
        assert result["failed_rows"] == 1
        assert len(result["errors"]) == 1

    @pytest.mark.asyncio
    async def test_process_stream_valid_row(self, valid_csv_content):
        processor = CSVProcessor()
        file = create_mock_file(valid_csv_content)
        
        async for result in processor._process_stream(file):
            assert isinstance(result, ChargeNotification)
            assert isinstance(result.debt_amount, Decimal)
            assert isinstance(result.debt_due_date, date)
            assert isinstance(result.debt_id, UUID)

    @pytest.mark.asyncio
    async def test_batch_processing(self):
        row = "John Doe,11111111111,john@example.com,1000.00,2023-01-01,550e8400-e29b-41d4-a716-446655440000\n"
        rows = [row] * (CSVProcessor().BATCH_SIZE + 1)
        content = "name,government_id,email,debt_amount,debt_due_date,debt_id\n" + "".join(rows)
        
        processor = CSVProcessor()
        file = create_mock_file(content)
        
        result = await processor.process(file)
        
        expected_rows = CSVProcessor().BATCH_SIZE + 1
        assert result["total_rows"] == expected_rows
        assert result["processed_rows"] == expected_rows
        assert result["failed_rows"] == 0 