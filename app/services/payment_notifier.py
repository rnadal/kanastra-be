import logging

logger = logging.getLogger(__name__)

class EmailNotifier:
    """
    Stub class for sending email notifications.
    Instead of sending an actual email, it logs that an email
    would have been sent along with the PDF reference.
    """
    def notify(self, pdf_reference: str, email: str) -> bool:
        logger.info(
            "Sending email to '%s' with PDF attachment reference: %s",
            email, pdf_reference
        )
        # Simulate a successful send
        return True
