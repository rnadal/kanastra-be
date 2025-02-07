from app.tasks import process_charge
import logging

logger = logging.getLogger(__name__)

class PaymentNotificationService:
    def process_payments(self, charges) -> None:
        for charge in charges:
            try:
                process_charge.delay(str(charge.id))
                logger.info("Enqueued payment notification task for charge id: %s", charge.id)
            except Exception as e:
                logger.error("Failed to enqueue task for charge %s: %s", charge.id, e)