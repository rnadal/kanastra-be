import pytest
from fastapi.testclient import TestClient
from app.main import app
import io

client = TestClient(app)

def create_test_csv(content: str, filename: str = "test.csv"):
    return {
        "file": (filename, content, "text/csv")
    }

def test_process_valid_csv():
    content = (
        "name,government_id,email,debt_amount,debt_due_date,debt_id\n"
        "John Doe,11111111111,john@example.com,1000.00,2023-01-01,550e8400-e29b-41d4-a716-446655440000"
    )
    
    response = client.post("/process-file/", files=create_test_csv(content))
    
    assert response.status_code == 200
    result = response.json()
    assert result["result"]["processed_rows"] == 1
    assert result["result"]["failed_rows"] == 0

def test_process_invalid_file_type():
    response = client.post("/process-file/", files=create_test_csv("content", "test.txt"))
    
    assert response.status_code == 400
    assert "Unsupported file type" in response.json()["detail"]

def test_process_invalid_csv_content():
    content = (
        "name,government_id,email,debt_amount,debt_due_date,debt_id\n"
        "Invalid,invalid,,invalid,invalid,invalid"
    )
    
    response = client.post("/process-file/", files=create_test_csv(content))
    
    assert response.status_code == 200
    result = response.json()
    assert result["result"]["processed_rows"] == 0
    assert result["result"]["failed_rows"] == 1
    assert len(result["result"]["errors"]) == 1

def test_process_empty_csv():
    content = "name,government_id,email,debt_amount,debt_due_date,debt_id\n"
    
    response = client.post("/process-file/", files=create_test_csv(content))
    
    assert response.status_code == 200
    result = response.json()
    assert result["result"]["total_rows"] == 0 