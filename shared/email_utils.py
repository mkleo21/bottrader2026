import logging
import os
from azure.communication.email import EmailClient

# Configuration retrieved from environment variables
CONNECTION_STRING = os.getenv("EMAIL_CONNECTION_STRING")
SENDER_ADDRESS = os.getenv("EMAIL_SENDER")
RECIPIENT_ADDRESS = os.getenv("EMAIL_RECIPIENT")

def send_email_alert(subject, body, config_key):
    """
    Sends an email alert using Azure Communication Services.
    Only sends if the environment variable Email_<config_key> is 'true' (case-insensitive) or not set.
    """
    # Check if email is enabled for this specific key
    env_key = f"Email_{config_key}"
    is_enabled = os.getenv(env_key, "true").lower() == "true"
    
    if not is_enabled:
        logging.info(f"Email alert suppressed by configuration: {env_key}")
        return

    try:
        client = EmailClient.from_connection_string(CONNECTION_STRING)
        
        message = {
            "senderAddress": SENDER_ADDRESS,
            "recipients": {
                "to": [{"address": RECIPIENT_ADDRESS}]
            },
            "content": {
                "subject": subject,
                "plainText": body
            }
        }
        
        poller = client.begin_send(message)
        logging.info(f"Email alert sent. Subject: {subject}")
        # Not waiting for poller result to avoid blocking the function flow excessively
    except Exception as e:
        logging.error(f"Failed to send email alert: {e}")
