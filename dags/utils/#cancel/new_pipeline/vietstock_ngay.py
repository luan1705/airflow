from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
import pandas as pd
from bs4 import BeautifulSoup
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
from datetime import datetime, timedelta
import logging
import re

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s - %(message)s',
)
log = logging.getLogger(__name__)

def vietstock():
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
        
        # Đường dẫn binary
        try:
            Chrome_option.binary_location = "/usr/bin/google-chrome"
        except:
            Chrome_option.binary_location = "/opt/google/chrome/chrome"
        
        url = 'https://vietstock.vn/chung-khoan.htm'
        
        # Khởi tạo driver với error handling
        try:
            driver = webdriver.Chrome(service=Service("/usr/local/bin/chromedriver"), options=Chrome_option)
        except Exception as e1:
            log.warning(f"Thử chromedriver path khác: {e1}")
            try:
                driver = webdriver.Chrome(service=Service("/usr/bin/chromedriver"), options=Chrome_option)
            except Exception as e2:
                log.error(f"Không thể khởi tạo driver: {e2}")
                return pd.DataFrame()
        
        # Set timeouts
        driver.set_page_load_timeout(60)  # Giảm từ 600
        driver.set_script_timeout(60)     # Giảm từ 600
        driver.implicitly_wait(10)
        
        # Load page
        log.info(f"Đang truy cập: {url}")
        driver.get(url)
        
        # Đợi page load
        wait = WebDriverWait(driver, 20)
        wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        
        # Collections để lưu data
        titles = []
        links = []
        dates = []
        
        max_clicks = 5  # Giảm từ 10
        
        for click_num in range(max_clicks):
            try:
                log.info(f"Đang xử lý trang {click_num + 1}/{max_clicks}")
                
                # Parse current page
                soup = BeautifulSoup(driver.page_source, 'html.parser')
                source = soup.find('div', class_='business padding20 border-radious5')
                
                if not source:
                    log.warning(f"Không tìm thấy container chính ở trang {click_num + 1}")
                    # Thử tìm container khác
                    source = soup.find('div', class_='business')
                    if not source:
                        log.error("Không tìm thấy container nào")
                        break
                
                # Extract articles
                baiviet = source.find_all('a', class_='fontbold')
                log.info(f"Tìm thấy {len(baiviet)} bài viết ở trang {click_num + 1}")
                
                # Process articles
                page_titles = []
                page_links = []
                
                for article in baiviet:
                    href = article.get('href')
                    title = article.get('title')
                    
                    # Skip external links
                    if href and href.startswith('http://'):
                        continue
                    
                    if title and href:
                        page_titles.append(title.strip())
                        
                        # Ensure full URL
                        if href.startswith('/'):
                            page_links.append(f"https://vietstock.vn{href}")
                        else:
                            page_links.append(href)
                
                # Extract dates
                ngay_elements = source.find_all('div', class_='meta3')
                page_dates = []
                
                for meta_div in ngay_elements:
                    a_tags = meta_div.find_all('a')
                    if len(a_tags) >= 2:
                        second_a = a_tags[1]
                        href = second_a.get('href', '')
                        
                        # Skip external links
                        if href.startswith('http://'):
                            continue
                        
                        date_text = second_a.get_text(strip=True)
                        if date_text:
                            page_dates.append(date_text)
                
                # Ensure equal lengths
                min_length = min(len(page_titles), len(page_links), len(page_dates)) if page_dates else min(len(page_titles), len(page_links))
                
                if min_length > 0:
                    titles.extend(page_titles[:min_length])
                    links.extend(page_links[:min_length])
                    
                    if page_dates:
                        dates.extend(page_dates[:min_length])
                    else:
                        # Fallback date
                        dates.extend([datetime.now().strftime('%d/%m')] * min_length)
                
                log.info(f"Đã thu thập {min_length} bài viết từ trang {click_num + 1}")
                
                # Scroll và click next page
                if click_num < max_clicks - 1:  # Không click ở trang cuối
                    try:
                        # Scroll
                        driver.execute_script("window.scrollTo(0, document.body.scrollHeight - 2200);")
                        time.sleep(2)
                        
                        # Find and click next button
                        next_button = wait.until(
                            EC.element_to_be_clickable((By.CSS_SELECTOR, 'li.pagination-page.next a'))
                        )
                        driver.execute_script("arguments[0].click();", next_button)
                        log.info("✅ Đã click next page")
                        
                        # Đợi page load
                        time.sleep(5)
                        
                        # Verify page changed
                        wait.until(EC.staleness_of(soup))
                        
                    except Exception as e:
                        log.info(f"Không thể click next page ở trang {click_num + 1}: {e}")
                        break
                
            except Exception as e:
                log.error(f"Lỗi ở trang {click_num + 1}: {e}")
                break
        
        log.info(f"Tổng cộng thu thập: Titles={len(titles)}, Links={len(links)}, Dates={len(dates)}")
        
        # Create DataFrame
        if not titles:
            log.warning("Không có dữ liệu để tạo DataFrame")
            return pd.DataFrame()
        
        # Ensure all arrays have same length
        min_length = min(len(titles), len(links), len(dates))
        
        df = pd.DataFrame({
            'Titles': titles[:min_length],
            'Date': dates[:min_length],
            'Link': links[:min_length],
            'Source': 'vietstock.vn'
        })
        
        log.info(f"Tạo DataFrame với {len(df)} dòng")
        
        # Process dates
        try:
            # Process relative dates ("X phút trước", "X giờ trước")
            def process_date(date_str):
                if "trước" in str(date_str):
                    return datetime.today().date()
                else:
                    # Add current year if missing
                    if re.match(r'^\d{1,2}/\d{1,2}$', str(date_str)):
                        return f"{date_str}/2025"
                    return date_str
            
            df['Date'] = df['Date'].apply(process_date)
            
            # Clean date strings (remove time info)
            df['Date'] = df['Date'].astype(str).str.replace(r'\s.+', '/2025', regex=True)
            
            # Convert to datetime
            df['Date'] = pd.to_datetime(df['Date'], dayfirst=True, errors='coerce')
            
            # Fill NaT with today
            df['Date'] = df['Date'].fillna(pd.Timestamp.now())
            
            # Convert to date and back to datetime for filtering
            df['Date'] = df['Date'].dt.date
            df['Date'] = pd.to_datetime(df['Date'], dayfirst=True)
            
        except Exception as e:
            log.error(f"Lỗi xử lý date: {e}")
            df['Date'] = pd.Timestamp.now()
        
        # Filter by yesterday
        today = datetime.today()
        yesterday = datetime(today.year, today.month, today.day) - timedelta(days=1)
        
        vietstock_filtered = df[
            (df['Date'] >= yesterday) & 
            (df['Date'] < today)
        ]
        
        log.info(f"✅ Cào được {len(df)} bài viết tổng, {len(vietstock_filtered)} bài ngày hôm qua")
        
        return vietstock_filtered
        
    except Exception as e:
        log.error(f"❌ Lỗi tổng thể trong vietstock(): {e}")
        return pd.DataFrame()
        
    finally:
        # Ensure driver is closed
        if driver:
            try:
                driver.quit()
                log.info("✅ Đã đóng driver")
            except:
                pass
