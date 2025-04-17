FROM apache/airflow:2.10.5-python3.9

# Switch to root user to install system dependencies
USER root

# Adding Spark + Java
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk curl && \
    mkdir -p /opt && \
    curl -fL https://dlcdn.apache.org/spark/spark-3.5.5/spark-3.5.5-bin-hadoop3.tgz | tar -zx -C /opt && \
    ln -s /opt/spark-3.5.5-bin-hadoop3 /opt/spark

# Set JAVA_HOME environment variable for Spark
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin
ENV SPARK_HOME=/opt/spark

# Install Chrome and ChromeDriver dependencies
RUN apt-get update && apt-get install -y \
    wget \
    unzip \
    curl \
    libglib2.0-0 \
    libnss3 \
    libgconf-2-4 \
    libfontconfig1 \
    libx11-6 \
    libx11-xcb1 \
    libxi6 \
    libxcomposite1 \
    libxtst6 \
    libxdamage1 \
    libxrandr2 \
    libasound2 \
    libatk1.0-0 \
    libpango-1.0-0 \
    libcairo2 \
    libgbm1 \
    fonts-liberation \
    && apt-get clean

# Install Chrome
RUN wget -q -O - https://dl.google.com/linux/linux_signing_key.pub | apt-key add - \
    && echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google-chrome.list \
    && apt-get update \
    && apt-get install -y google-chrome-stable \
    && apt-get clean

# Set up folders with permissions
RUN mkdir -p /tmp/uc_driver /tmp/uc_user_data && \
    chmod -R 777 /tmp/uc_driver /tmp/uc_user_data

# Get the current Chrome version
# Get Chrome version
RUN CHROME_VERSION=$(google-chrome --version | grep -oP '\d+\.\d+\.\d+') && \
    echo "Detected Chrome version: $CHROME_VERSION" && \
    CHROMEDRIVER_URL=$(curl -sS https://googlechromelabs.github.io/chrome-for-testing/last-known-good-versions-with-downloads.json | \
      python3 -c "import sys, json; data=json.load(sys.stdin); print([b['downloads']['chromedriver'][0]['url'] for b in data['channels'].values() if b['version'].startswith('$CHROME_VERSION')][0])") && \
    echo "Downloading ChromeDriver from: $CHROMEDRIVER_URL" && \
    wget -O /tmp/chromedriver.zip "$CHROMEDRIVER_URL" && \
    unzip /tmp/chromedriver.zip -d /usr/local/bin/ && \
    mv /usr/local/bin/chromedriver-linux64/chromedriver /tmp/chromedriver && \
    chmod +x /tmp/chromedriver && \
    chmod 777 /tmp/chromedriver && \
    rm -rf /tmp/chromedriver.zip /usr/local/bin/chromedriver-linux64

# Ensure airflow owns its local folder
RUN mkdir -p /home/airflow/.local/share/undetected_chromedriver && \
    chown -R airflow: /home/airflow/.local

# Switch back to the airflow user
USER airflow

# Copy the requirements.txt file into the container
COPY requirements.txt /requirements.txt

# Upgrade pip and install packages
RUN pip install --upgrade pip \
    && pip install --no-cache-dir -r /requirements.txt