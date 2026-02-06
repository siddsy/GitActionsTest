import smtplib
from email.message import EmailMessage
import os

EMAIL_FROM = os.environ["EMAIL_FROM"]
EMAIL_TO = "dunderhenlin@gmail.com"
EMAIL_PASS = os.environ["EMAIL_PASS"]

msg = EmailMessage()
msg["Subject"] = "Hourly test from GitHub Actions"
msg["From"] = EMAIL_FROM
msg["To"] = EMAIL_TO
msg.set_content("This email confirms the hourly script is running.")

with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
    server.login(EMAIL_FROM, EMAIL_PASS)
    server.send_message(msg)

print("Email sent")
