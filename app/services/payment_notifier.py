import logging
from app.schemas.charge_notification import ChargeNotification

logger = logging.getLogger(__name__)

class PDFGenerator:
    """
    Stub class for generating a PDF for a 'boleto'.
    In a real implementation, this would generate a PDF file that is emailed to the client
    """

    def generate_pdf(self, charge: ChargeNotification) -> str:
        """
        "Generate" a PDF for the given charge notification.
        Instead of implementing actual PDF generation, log the action and return a fictitious file name.

        :param charge: ChargeNotification instance.
        :return: A reference (filename) to the generated PDF.
        """
        pdf_filename = f"payment_{charge.debt_id}.pdf"
        logger.info("Generating PDF for charge notification for email '%s'. PDF filename: %s",
                    charge.email, pdf_filename)
        # In a real implementation, code to generate the "boleto" pdf would go here.
        return pdf_filename


class EmailNotifier:
    """
    Stub class for notifying clients via email.
    In a real implementation, this would send an email (with the PDF attached).
    """
    def notify(self, pdf_reference: str, email: str) -> bool:
        """
        "Send" an email notification.
        Instead of sending an actual email, it just logs that the email would be sent.

        :param pdf_reference: A reference to the generated PDF (for example, a filename or URL).
        :param email: The recipient's email address.
        :return: Whether email could be sent or not
        """
        logger.info("Sending email to '%s' with PDF attachment reference: %s", email, pdf_reference)
        # In a real implementation, code to send the email would go here.
        return True
