import logging
from typing import List
from app.schemas.charge_notification import ChargeNotification
from app.services.payment_file import PDFGenerator
from app.services.payment_notifier import EmailNotifier

logger = logging.getLogger(__name__)

class PaymentNotificationService:
    def __init__(self):
        self.pdf_generator = PDFGenerator()
        self.email_notifier = EmailNotifier()

    def process_payments(self, charges: List[ChargeNotification]) -> None:
        for charge in charges:
            try:
                pdf_ref = self.pdf_generator.generate_pdf(charge)
                self.email_notifier.notify(pdf_ref, charge.email)
                logger.info(
                    "Processed payment notification for '%s' with PDF '%s'", 
                    charge.email, pdf_ref
                )
            except Exception as e:
                logger.error("Error processing payment notification for %s: %s", charge.email, e) 