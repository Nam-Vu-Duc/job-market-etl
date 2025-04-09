FROM apache/airflow:2.7.1-python3.9

USER root
RUN apt-get update && apt-get install -y gcc python3-dev

# Install Chrome (required for Selenium)
RUN wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add - \
    && echo "deb http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google-chrome.list \
    && apt-get update \
    && apt-get install -y google-chrome-stable \
    && rm -rf /var/lib/apt/lists/*

# Install ChromeDriver (required for Selenium)
RUN CHROMEDRIVER_VERSION=$(curl -sS chromedriver.storage.googleapis.com/LATEST_RELEASE) \
    && wget -O /tmp/chromedriver.zip https://chromedriver.storage.googleapis.com/${CHROMEDRIVER_VERSION}/chromedriver_linux64.zip \
    && unzip /tmp/chromedriver.zip -d /usr/local/bin/ \
    && rm /tmp/chromedriver.zip \
    && chmod +x /usr/local/bin/chromedriver

# Switch back to the airflow user
USER airflow

# Install Python dependencies
RUN pip install --no-cache-dir \
    selenium \
    undetected-chromedriver \
    mysql-connector-python \
    psycopg2-binary \
    pandas \
    pyspark \
    unidecode \