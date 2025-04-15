import ssl
import smtplib
from email.message import EmailMessage
from dotenv import load_dotenv
import time
import os
import mysql.connector
import pandas as pd
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
load_dotenv()

def fetch_from_mysql():
    conn = mysql.connector.connect(
        host="host.docker.internal",
        user="root",
        password="root"
    )
    cur = conn.cursor()

    cur.execute(
    """
        select count(*)
        from jobs.jobs
        where query_day = %s
    """, ((time.strftime("%Y-%m-%d")),)
    )
    total_jobs = cur.fetchone()

    cur.execute(
        """
            select source, position, company, address, max_salary, experience
            from jobs.jobs
            where query_day = %s
            order by max_salary desc
            limit 10
        """, ((time.strftime("%Y-%m-%d")),)
    )
    top_10_jobs = cur.fetchall()

    return total_jobs, top_10_jobs

def send_email():
    sender = 'vuducnama6@gmail.com'
    # password = os.getenv('APP_PASSWORD')
    password = 'cnxsnrschvmoaghb'
    receiver = 'namvd.dev@gmail.com'
    subject = 'Job Market Report'
    total_jobs, top_10_jobs = fetch_from_mysql()
    df = pd.DataFrame(top_10_jobs, columns=['source', 'position', 'company', 'address', 'max_salary', 'experience'])
    html = df.to_html(index=False, justify='left', border=1)

    body = f"""
        <html>
            <body>
                <h2>Job Market Report - {time.strftime('%Y-%m-%d')}</h2>
                <p>Total jobs: <strong>{total_jobs}</strong></p>
                <p>Top 10 Jobs with Highest Salaries:</p>
                {html}
                <br>
            </body>
        </html>
    """

    em = MIMEMultipart("alternative")
    em['From'] = sender
    em['To'] = receiver
    em['Subject'] = subject
    em.attach(MIMEText(body, "html"))

    context = ssl.create_default_context()

    with smtplib.SMTP_SSL('smtp.gmail.com', 465, context=context) as smtp:
        smtp.login(sender, password)
        smtp.sendmail(sender, receiver, em.as_string())

send_email()