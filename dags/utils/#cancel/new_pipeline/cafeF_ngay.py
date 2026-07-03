from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from bs4 import BeautifulSoup
import pandas as pd
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
from datetime import datetime, timedelta
import logging

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s - %(message)s',
)
log = logging.getLogger(__name__)

def cafef():
    driver = None
    try:
        # Chrome Options
        Chrome_option = Options()
        Chrome_option.add_argument('--headless=new')
        Chrome_option.add_argument('--disable-gpu')
        Chrome_option.add_argument('--no-sandbox')
        Chrome_option.add_argument('--disable-dev-shm-usage')
        Chrome_option.add_argument('--disable-software-rasterizer')
        Chrome_option.add_argument('--disable-extensions')
        Chrome_option.add_argument('--remote-debugging-port=9222')
        Chrome_option.add_argument('--window-size=1920,1080')  # Thêm dấu --
        Chrome_option.add_argument('--disable-blink-features=AutomationControlled')
        Chrome_option.add_experimental_option("excludeSwitches", ["enable-automation"])
        Chrome_option.add_experimental_option('useAutomationExtension', False)
        
        # Đường dẫn binary (thử cả 2 đường dẫn)
        try:
            Chrome_option.binary_location = "/usr/bin/google-chrome"
        except:
            Chrome_option.binary_location = "/opt/google/chrome/chrome"
        
        url = 'https://cafef.vn/thi-truong-chung-khoan.chn'
        
        # Khởi tạo driver với error handling
        try:
            driver = webdriver.Chrome(service=Service("/usr/local/bin/chromedriver"), options=Chrome_option)
        except Exception as e1:
            log.warning(f"Thử chromedriver path khác: {e1}")
            try:
                driver = webdriver.Chrome(service=Service("/usr/bin/chromedriver"), options=Chrome_option)
            except Exception as e2:
                log.error(f"Không thể khởi tạo driver: {e2}")
                return pd.DataFrame()  # Return empty DataFrame
        
        # Set timeouts
        driver.set_page_load_timeout(60)
        driver.implicitly_wait(10)
        
        # Load page
        log.info(f"Đang truy cập: {url}")
        driver.get(url)
        
        # Đợi page load
        wait = WebDriverWait(driver, 20)
        wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        
        # Cuộn và click "Xem thêm" với error handling
        max_cycles = 5  # Giảm số lần thử
        for cycle in range(max_cycles):
            try:
                log.info(f"Chu kỳ cuộn {cycle + 1}/{max_cycles}")
                
                # Cuộn xuống
                for scroll in range(3):  # Giảm số lần cuộn
                    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    time.sleep(1)
                
                # Tìm và click nút "Xem thêm"
                try:
                    button = wait.until(EC.element_to_be_clickable((By.CLASS_NAME, "btn-viewmore")))
                    driver.execute_script("arguments[0].click();", button)  # JS click thay vì .click()
                    log.info("✅ Đã click nút 'Xem thêm'")
                    time.sleep(3)  # Đợi load content
                except Exception as e:
                    log.info(f"Không tìm thấy nút 'Xem thêm' ở chu kỳ {cycle + 1}: {e}")
                    break
                    
            except Exception as e:
                log.error(f"Lỗi trong chu kỳ {cycle + 1}: {e}")
                break
        
        # Parse HTML
        log.info("Đang parse HTML...")
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        
        # Tìm container chính
        source = soup.find('div', class_='list-news-main top5_news loadedStock')
        
        if not source:
            log.warning("Không tìm thấy container chính")
            # Thử tìm với class khác
            source = soup.find('div', class_='list-news-main')
            if not source:
                log.error("Không tìm thấy bất kỳ container nào")
                return pd.DataFrame()
        
        # Extract data
        baiviet = source.find_all('a', class_='avatar img-resize')
        log.info(f"Tìm thấy {len(baiviet)} bài viết")
        
        if not baiviet:
            log.warning("Không tìm thấy bài viết nào")
            return pd.DataFrame()
        
        # Extract titles và links
        titles = []
        links = []
        
        for a in baiviet:
            title = a.get('title')
            href = a.get('href')
            
            if title and href:
                titles.append(title.strip())
                # Ensure full URL
                if href.startswith('/'):
                    links.append(f"https://cafef.vn{href}")
                else:
                    links.append(href)
        
        # Extract dates
        date_elements = source.find_all('span', class_=['time time-ago hidden', 'time time-ago'])
        dates = []
        
        for span in date_elements:
            date_title = span.get('title')
            if date_title:
                dates.append(date_title.strip())
        
        log.info(f"Titles: {len(titles)}, Links: {len(links)}, Dates: {len(dates)}")
        
        # Ensure all lists have same length
        min_length = min(len(titles), len(links), len(dates)) if dates else min(len(titles), len(links))
        
        if min_length == 0:
            log.warning("Không có dữ liệu để tạo DataFrame")
            return pd.DataFrame()
        
        # Create DataFrame
        df_data = {
            'Titles': titles[:min_length],
            'Link': links[:min_length],
            'Source': 'Cafef.vn'
        }
        
        # Add dates if available
        if dates:
            df_data['Date'] = dates[:min_length]
        else:
            df_data['Date'] = datetime.now().strftime('%d/%m/%Y %H:%M:%S')
        
        df = pd.DataFrame(df_data)
        
        # Process dates
        try:
            # Thử convert date với nhiều format
            df['Date'] = pd.to_datetime(df['Date'], dayfirst=True, errors='coerce')
            
            # Nếu có NaT, thay bằng ngày hiện tại
            df['Date'] = df['Date'].fillna(pd.Timestamp.now())
            
            # Convert to date only
            df['Date'] = df['Date'].dt.date
            df['Date'] = pd.to_datetime(df['Date'], dayfirst=True)
            
        except Exception as e:
            log.error(f"Lỗi xử lý date: {e}")
            df['Date'] = pd.Timestamp.now()
        
        # Filter by date (yesterday only)
        today = datetime.today()
        yesterday = (datetime(today.year, today.month, today.day) - timedelta(days=1))
        
        # Filter data
        cafef_filtered = df[
            (df['Date'] >= yesterday) & 
            (df['Date'] < today)
        ]
        
        log.info(f"✅ Cào được {len(df)} bài viết tổng, {len(cafef_filtered)} bài ngày hôm qua")
        
        return cafef_filtered
        
    except Exception as e:
        log.error(f"❌ Lỗi tổng thể trong cafef(): {e}")
        return pd.DataFrame()
        
    finally:
        # Đảm bảo driver được đóng
        if driver:
            try:
                driver.quit()
                log.info("✅ Đã đóng driver")
            except:
                pass

