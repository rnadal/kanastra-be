from app.schemas.charge_notification import ChargeNotification
from app.services.payment_notifier import logger


class PDFGenerator:
    """
    Stub class for generating a PDF for a payment.
    Instead of actually generating a PDF, this logs the action
    and returns a simulated PDF filename.
    """
    def generate_pdf(self, charge: ChargeNotification) -> str:
        # Use a unique identifier from the charge to create a filename
        pdf_filename = f"payment_{charge.debt_id}.pdf"
        logger.info(
            "Generating PDF for charge notification for email '%s' with debt_id '%s'. PDF filename: %s",
            charge.email, charge.debt_id, pdf_filename
        )
        return pdf_filename