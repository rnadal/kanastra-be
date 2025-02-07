import logging
from app.schemas.charge_notification import ChargeNotification
from app.services.PDFGenerator import PDFGenerator
from app.services.payment_notifier import EmailNotifier

def test_pdf_generation_and_email_notification(caplog):
    caplog.set_level(logging.INFO)
    
    charge = ChargeNotification(
        name="John Doe",
        government_id="11111111111",
        email="johndoe@example.com",
        debt_amount="1000.00",
        debt_due_date="2023-01-01",
        debt_id="550e8400-e29b-41d4-a716-446655440000"
    )
    
    pdf_generator = PDFGenerator()
    email_notifier = EmailNotifier()
    
    pdf_ref = pdf_generator.generate_pdf(charge)
    email_notifier.notify(pdf_ref, charge.email)
    
    assert f"Generating PDF for charge notification for email '{charge.email}'" in caplog.text
    assert f"Sending email to '{charge.email}' with PDF attachment reference: {pdf_ref}" in caplog.text 