import ssl
import smtplib
from email.message import EmailMessage

def send_email():
    sender = 'vuducnama6@gmail.com'
    password = 'cnxsnrschvmoaghb'
    receiver = 'namvd.dev@gmail.com'
    subject = 'Job Market Report'
    body = 'report on the 13/4/2025'

    em = EmailMessage()
    em['From'] = sender
    em['To'] = receiver
    em['Subject'] = subject
    em.set_content(body)

    context = ssl.create_default_context()

    with smtplib.SMTP_SSL('smtp.gmail.com', 465, context=context) as smtp:
        smtp.login(sender, password)
        smtp.sendmail(sender, receiver, em.as_string())

send_email()