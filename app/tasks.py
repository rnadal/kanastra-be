import logging
from celery import Celery
from app.db import SessionLocal
from app.models import ChargeRow, ChargeStatus
from app.services.payment_notifier import EmailNotifier
from app.services.payment_file import PDFGenerator

logger = logging.getLogger(__name__)

celery_app = Celery(
    "worker",
    broker="redis://redis:6379/0"
)

@celery_app.task(bind=True, max_retries=3)
def process_charge(self, charge_id):
    session = SessionLocal()
    try:
        charge = session.query(ChargeRow).filter(ChargeRow.id == charge_id).first()
        if not charge:
            logger.error(f"Charge with id {charge_id} not found")
            return

        if charge.status != ChargeStatus.PENDING:
            logger.info(f"Charge {charge_id} already processed with status {charge.status}")
            return

        charge.status = ChargeStatus.PROCESSING
        session.commit()

        pdf_generator = PDFGenerator()
        email_notifier = EmailNotifier()
        pdf_ref = pdf_generator.generate_pdf(charge)
        email_notifier.notify(pdf_ref, charge.email)

        charge.status = ChargeStatus.PROCESSED
        session.commit()

        logger.info(f"Successfully processed charge {charge_id}")
    except Exception as exc:
        session.rollback()
        charge.status = ChargeStatus.FAILED
        charge.error = str(exc)
        session.commit()
        logger.error(f"Failed to process charge {charge_id}: {exc}")
        raise self.retry(exc=exc, countdown=60)
    finally:
        session.close() 