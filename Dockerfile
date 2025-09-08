FROM apache/airflow:2.9.2-python3.10

# 👉 cần root để cài apt packages
USER root

# Install dependencies + Google Chrome
RUN apt-get update && apt-get install -y \
    wget unzip curl gnupg2 ca-certificates fonts-liberation \
    libnss3 libxss1 libasound2 libatk1.0-0 libcups2 libx11-xcb1 \
    libxcomposite1 libxcursor1 libxdamage1 libxi6 libxtst6 \
    libxrandr2 libpangocairo-1.0-0 libpango-1.0-0 libgtk-3-0 \
    libglib2.0-0 libgbm1 libatk-bridge2.0-0 libdrm2 xdg-utils \
    xvfb && \
    rm -rf /var/lib/apt/lists/*

# Tải & cài Google Chrome
RUN wget -q -O - https://dl.google.com/linux/linux_signing_key.pub | apt-key add - && \
    echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" > /etc/apt/sources.list.d/google-chrome.list && \
    apt-get update && apt-get install -y google-chrome-stable && \
    rm -rf /var/lib/apt/lists/*

# Cài ChromeDriver với phương pháp ổn định hơn
RUN CHROME_VERSION=$(google-chrome --version | cut -d ' ' -f3 | cut -d '.' -f1-3) && \
    echo "Chrome version: $CHROME_VERSION" && \
    # Lấy version ChromeDriver tương thích
    CHROMEDRIVER_VERSION=$(curl -s "https://googlechromelabs.github.io/chrome-for-testing/LATEST_RELEASE_$CHROME_VERSION") && \
    echo "ChromeDriver version: $CHROMEDRIVER_VERSION" && \
    # Tải ChromeDriver
    wget -O /tmp/chromedriver.zip "https://storage.googleapis.com/chrome-for-testing-public/$CHROMEDRIVER_VERSION/linux64/chromedriver-linux64.zip" && \
    unzip /tmp/chromedriver.zip -d /tmp && \
    mv /tmp/chromedriver-linux64/chromedriver /usr/local/bin/chromedriver && \
    chmod +x /usr/local/bin/chromedriver && \
    rm -rf /tmp/* || \
    # Fallback: sử dụng version stable nếu lỗi
    (echo "Fallback to stable version" && \
     STABLE_VERSION=$(curl -s https://googlechromelabs.github.io/chrome-for-testing/LATEST_RELEASE_STABLE) && \
     wget -O /tmp/chromedriver.zip "https://storage.googleapis.com/chrome-for-testing-public/$STABLE_VERSION/linux64/chromedriver-linux64.zip" && \
     unzip /tmp/chromedriver.zip -d /tmp && \
     mv /tmp/chromedriver-linux64/chromedriver /usr/local/bin/chromedriver && \
     chmod +x /usr/local/bin/chromedriver && \
     rm -rf /tmp/*)

# Tạo script wrapper cho xvfb (virtual display)
RUN echo '#!/bin/bash\nXvfb :99 -screen 0 1024x768x24 > /dev/null 2>&1 &\nexport DISPLAY=:99\nexec "$@"' > /usr/local/bin/xvfb-run-custom && \
    chmod +x /usr/local/bin/xvfb-run-custom

# 👉 trả lại quyền cho airflow
USER airflow

# Cài thêm Python packages sau khi chuyển user
RUN pip install --no-cache-dir selenium==4.24.0 beautifulsoup4 pandas psycopg2-binary sqlalchemy

# Set environment variables
ENV DISPLAY=:99
ENV CHROME_BIN=/usr/bin/google-chrome
ENV CHROMEDRIVER_PATH=/usr/local/bin/chromedriver