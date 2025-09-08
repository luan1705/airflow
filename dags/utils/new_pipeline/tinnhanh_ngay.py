from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
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

def tinnhanh():
    driver = None
    try:
        # Cấu hình Chrome headless
        Chrome_option = Options()
        Chrome_option.add_argument('--headless=new')
        Chrome_option.add_argument('--disable-gpu')
        Chrome_option.add_argument('--no-sandbox')
        Chrome_option.add_argument('--disable-dev-shm-usage')
        Chrome_option.add_argument('--disable-software-rasterizer')
        Chrome_option.add_argument('--disable-extensions')
        Chrome_option.add_argument('--remote-debugging-port=9222')
        Chrome_option.add_argument('--window-size=1920,1080')  # Sửa format
        Chrome_option.add_argument('--disable-blink-features=AutomationControlled')
        Chrome_option.add_experimental_option("excludeSwitches", ["enable-automation"])
        Chrome_option.add_experimental_option('useAutomationExtension', False)
        
        # Thử cả 2 đường dẫn binary
        try:
            Chrome_option.binary_location = "/usr/bin/google-chrome"
        except:
            Chrome_option.binary_location = "/opt/google/chrome/chrome"

        # URL
        url = 'https://www.tinnhanhchungkhoan.vn/chung-khoan/'
        
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

        # Set timeouts (giảm timeout cho phù hợp)
        driver.set_page_load_timeout(60)
        driver.implicitly_wait(10)
        
        # Load page
        log.info(f"Đang truy cập: {url}")
        driver.get(url)
        
        # Đợi page load
        wait = WebDriverWait(driver, 20)
        wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        
        # Cuộn xuống và nhấn "Xem thêm"
        max_clicks = 5  # Giảm số lần click
        successful_clicks = 0
        
        for i in range(max_clicks):
            try:
                log.info(f"Đang thử click 'Xem thêm' lần {i+1}/{max_clicks}")
                
                # Tìm nút "Xem thêm" với nhiều selector
                xem_them_button = None
                selectors = [
                    '//button[contains(text(),"Xem thêm")]',
                    '//a[contains(text(),"Xem thêm")]',
                    '//div[contains(text(),"Xem thêm")]',
                    '//*[contains(text(),"Xem thêm")]'
                ]
                
                for selector in selectors:
                    try:
                        xem_them_button = driver.find_element(By.XPATH, selector)
                        if xem_them_button:
                            break
                    except:
                        continue
                
                if not xem_them_button:
                    log.warning(f"Không tìm thấy nút 'Xem thêm' ở lần {i+1}")
                    break
                
                # Scroll tới nút
                driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", xem_them_button)
                time.sleep(1)
                
                # Click nút bằng JavaScript để tránh lỗi
                driver.execute_script("arguments[0].click();", xem_them_button)
                
                log.info(f"✅ Click 'Xem thêm' lần {i+1} thành công")
                successful_clicks += 1
                time.sleep(3)  # Đợi nội dung mới load
                
            except Exception as e:
                log.warning(f"Không thể click 'Xem thêm' lần {i+1}: {e}")
                break
        
        log.info(f"Đã click thành công {successful_clicks}/{max_clicks} lần")
        
        # Lấy dữ liệu với BeautifulSoup
        log.info("Đang parse HTML...")
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        
        # Tìm container chính
        source = soup.find('div', class_='box-content content-list')
        
        if not source:
            log.warning("Không tìm thấy container chính, thử tìm alternatives...")
            # Thử tìm với class khác
            alternatives = [
                'content-list',
                'box-content',
                'list-news'
            ]
            for alt_class in alternatives:
                source = soup.find('div', class_=alt_class)
                if source:
                    log.info(f"Tìm thấy container với class: {alt_class}")
                    break
            
            if not source:
                log.error("Không tìm thấy container nào")
                return pd.DataFrame()
        
        # Extract headlines
        headline = source.find_all(['h2', 'h3'], class_='story__heading')
        log.info(f"Tìm thấy {len(headline)} headlines")
        
        if not headline:
            log.warning("Không tìm thấy headlines, thử selector khác...")
            # Thử các selector khác
            alternative_selectors = [
                {'tag': ['h2', 'h3'], 'class_': 'heading'},
                {'tag': ['h2', 'h3'], 'class_': 'title'},
                {'tag': 'h2'},
                {'tag': 'h3'}
            ]
            
            for selector in alternative_selectors:
                if 'class_' in selector:
                    headline = source.find_all(selector['tag'], class_=selector['class_'])
                else:
                    headline = source.find_all(selector['tag'])
                
                if headline:
                    log.info(f"Tìm thấy {len(headline)} headlines với selector alternative")
                    break
        
        if not headline:
            log.error("Không tìm thấy headlines nào")
            return pd.DataFrame()
        
        # Extract data
        baiviet = [h.find('a') for h in headline if h.find('a')]
        log.info(f"Tìm thấy {len(baiviet)} bài viết có link")
        
        # Extract dates
        ngay = source.find_all('time')
        log.info(f"Tìm thấy {len(ngay)} elements thời gian")
        
        # Process titles và links
        all_titles = []
        all_links = []
        
        for a in baiviet:
            if not a:
                continue
                
            # Get title
            if a.get('title'):
                title = a.get('title').strip()
            else:
                title = a.text.strip()
            
            # Get link
            href = a.get('href')
            if href:
                if href.startswith('/'):
                    # Relative URL
                    if href.startswith('/chung-khoan'):
                        link = f'https://www.tinnhanhchungkhoan.vn{href}'
                    else:
                        link = f'https://www.tinnhanhchungkhoan.vn/chung-khoan{href}'
                else:
                    # Absolute URL
                    link = href
            else:
                continue  # Skip if no href
            
            if title and link:
                all_titles.append(title)
                all_links.append(link)
        
        # Process dates
        all_ngay = [x.text.strip() for x in ngay if x.text.strip()]
        
        log.info(f"Processed: {len(all_titles)} titles, {len(all_links)} links, {len(all_ngay)} dates")
        
        # Ensure equal lengths
        min_length = min(len(all_titles), len(all_links))
        if all_ngay:
            min_length = min(min_length, len(all_ngay))
        
        if min_length == 0:
            log.warning("Không có dữ liệu để tạo DataFrame")
            return pd.DataFrame()
        
        # Create DataFrame
        df_data = {
            'Titles': all_titles[:min_length],
            'Link': all_links[:min_length],
            'Source': 'tinnhanhchungkhoan.vn'
        }
        
        # Add dates
        if all_ngay and len(all_ngay) >= min_length:
            df_data['Date'] = all_ngay[:min_length]
        else:
            # Fallback to current date
            df_data['Date'] = datetime.now().strftime('%d/%m/%Y')
        
        df = pd.DataFrame(df_data)
        
        # Process dates
        try:
            df['Date'] = pd.to_datetime(df['Date'], dayfirst=True, errors='coerce')
            df['Date'] = df['Date'].fillna(pd.Timestamp.now())
            df['Date'] = df['Date'].dt.date
            df['Date'] = pd.to_datetime(df['Date'], dayfirst=True)
        except Exception as e:
            log.error(f"Lỗi xử lý date: {e}")
            df['Date'] = pd.Timestamp.now()
        
        # Filter by yesterday
        today = datetime.today()
        yesterday = datetime(today.year, today.month, today.day) - timedelta(days=1)
        
        tn = df[(df['Date'] >= yesterday) & (df['Date'] < today)]
        
        log.info(f"✅ Cào được {len(df)} bài viết tổng, {len(tn)} bài ngày hôm qua")
        return tn
        
    except Exception as e:
        log.error(f"❌ Lỗi tổng thể trong tinnhanh(): {e}")
        return pd.DataFrame()
        
    finally:
        # Đảm bảo driver được đóng
        if driver:
            try:
                driver.quit()
                log.info("✅ Đã đóng driver")
            except:
                pass
