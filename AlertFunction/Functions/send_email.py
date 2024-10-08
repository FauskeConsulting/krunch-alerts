import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import logging
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

EMAIL_ADDRESS = os.environ.get('EMAIL_ADDRESS',None)
EMAIL_PASSWORD = os.environ.get('EMAIL_PASSWORD')
SMTP_SERVER = os.environ.get('SMTP_SERVER')
SMTP_PORT = os.environ.get('SMTP_PORT')
RECIPIENT_EMAILS = os.environ.get('RECIPIENT_EMAILS').split(',')

def send_email(subject, body, to_emails):
    from_email = EMAIL_ADDRESS
    password = EMAIL_PASSWORD

    msg = MIMEMultipart()
    msg['From'] = from_email
    msg['To'] = ', '.join(to_emails)
    msg['Subject'] = subject

    msg.attach(MIMEText(body, 'html'))

    try:
        server = smtplib.SMTP(SMTP_SERVER, 587)
        server.starttls()
        server.login(from_email, password)
        text = msg.as_string()
        server.sendmail(from_email, to_emails, text)
        server.quit()
        logging.info(f"Email sent to {RECIPIENT_EMAILS} successfully")
    except Exception as e:
        logging.error(f"Failed to send email: {e}")