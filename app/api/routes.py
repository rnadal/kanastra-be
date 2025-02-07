from fastapi import APIRouter, UploadFile, HTTPException
from app.services.processor import ProcessorFactory
from app.services.payment_notification import PaymentNotificationService

router = APIRouter()


@router.post("/process-file/")
async def process_file(file: UploadFile):
    file_type = file.filename.split(".")[-1] if "." in file.filename else ""

    try:
        processor = ProcessorFactory.get_processor(file_type)
        result = await processor.process(file)
        
        successful_charges = result.get("charges", [])
        if successful_charges:
            notifier = PaymentNotificationService()
            notifier.process_payments(successful_charges)
        
        return {"message": f"{file_type.upper()} processed successfully", "result": result}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
