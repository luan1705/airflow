FROM apache/airflow:2.9.1-python3.10

USER root

# Cài đặt Chrome
RUN apt-get update && apt-get install -y wget unzip gnupg curl \
    && wget -q -O - https://dl.google.com/linux/linux_signing_key.pub | apt-key add - \
    && echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" > /etc/apt/sources.list.d/google-chrome.list \
    && apt-get update && apt-get install -y google-chrome-stable

# Cài ChromeDriver
RUN CHROME_DRIVER_VERSION=$(curl -sS https://chromedriver.storage.googleapis.com/LATEST_RELEASE) && \
    wget -q https://chromedriver.storage.googleapis.com/${CHROME_DRIVER_VERSION}/chromedriver_linux64.zip && \
    unzip chromedriver_linux64.zip && \
    mv chromedriver /usr/bin/chromedriver && \
    chmod +x /usr/bin/chromedriver && \
    rm chromedriver_linux64.zip

# 👉 Bắt buộc phải trở về user airflow trước khi cài pip
USER airflow

# Cài selenium (phải dùng user airflow)
RUN pip install selenium
RUN pip install webdriver-manager
RUN pip install VNSFintech
RUN pip install openpyxl