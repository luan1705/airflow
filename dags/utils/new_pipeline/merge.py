import pandas as pd
import asyncio
from concurrent.futures import ThreadPoolExecutor
import logging
import time
from typing import List, Callable, Tuple, Optional
import sys
import traceback

# Import your scraper functions
from utils.new_pipeline.vietstock_ngay import vietstock
from utils.new_pipeline.tinnhanh_ngay import tinnhanh  
from utils.new_pipeline.cafeF_ngay import cafef

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s - %(message)s',
)
log = logging.getLogger(__name__)

def safe_scraper_wrapper(scraper_func: Callable) -> pd.DataFrame:
    """
    Wrapper function để bắt lỗi và đảm bảo return DataFrame
    """
    start_time = time.time()
    try:
        log.info(f"🚀 Bắt đầu {scraper_func.__name__}")
        result = scraper_func()
        
        if isinstance(result, pd.DataFrame):
            if not result.empty:
                execution_time = round(time.time() - start_time, 2)
                log.info(f"✅ {scraper_func.__name__}: {len(result)} bài viết trong {execution_time}s")
                return result
            else:
                log.warning(f"⚠️ {scraper_func.__name__}: DataFrame trống")
                return pd.DataFrame()
        else:
            log.error(f"❌ {scraper_func.__name__}: Không trả về DataFrame")
            return pd.DataFrame()
            
    except Exception as e:
        execution_time = round(time.time() - start_time, 2)
        log.error(f"❌ {scraper_func.__name__} lỗi sau {execution_time}s: {e}")
        log.error(f"Chi tiết lỗi: {traceback.format_exc()}")
        return pd.DataFrame()

async def run_in_thread(func: Callable, executor: ThreadPoolExecutor) -> pd.DataFrame:
    """
    Chạy scraper function trong thread pool
    """
    loop = asyncio.get_running_loop()
    try:
        result = await loop.run_in_executor(executor, safe_scraper_wrapper, func)
        return result
    except Exception as e:
        log.error(f"❌ Thread execution error cho {func.__name__}: {e}")
        return pd.DataFrame()

async def tintuc_async() -> Tuple[pd.DataFrame, dict]:
    """
    Chạy tất cả scrapers async với error handling và stats
    """
    log.info("🎯 Bắt đầu async news scraping pipeline")
    
    # Định nghĩa scrapers
    scrapers = [
        (vietstock, "VietStock"),
        (tinnhanh, "TinNhanh"),
        (cafef, "CafeF")
    ]
    
    results = []
    stats = {}
    start_time = time.time()
    
    # Sử dụng ThreadPoolExecutor với max_workers phù hợp
    max_workers = min(len(scrapers), 3)  # Không quá 3 workers
    
    try:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Tạo tasks
            tasks = []
            for scraper_func, scraper_name in scrapers:
                task = run_in_thread(scraper_func, executor)
                tasks.append((task, scraper_name))
            
            # Chạy tất cả tasks
            log.info(f"⚡ Chạy {len(tasks)} scrapers song song...")
            
            # Gather với timeout
            try:
                task_results = await asyncio.wait_for(
                    asyncio.gather(*[task for task, _ in tasks], return_exceptions=True),
                    timeout=300  # 5 phút timeout
                )
            except asyncio.TimeoutError:
                log.error("❌ Async tasks timeout sau 5 phút")
                task_results = [pd.DataFrame() for _ in tasks]
            
            # Process results
            for (task, scraper_name), result in zip(tasks, task_results):
                if isinstance(result, Exception):
                    log.error(f"❌ {scraper_name}: Exception - {result}")
                    stats[scraper_name] = {
                        'status': 'error',
                        'articles': 0,
                        'error': str(result)
                    }
                elif isinstance(result, pd.DataFrame):
                    if not result.empty:
                        results.append(result)
                        stats[scraper_name] = {
                            'status': 'success', 
                            'articles': len(result)
                        }
                        log.info(f"✅ {scraper_name}: {len(result)} bài viết")
                    else:
                        stats[scraper_name] = {
                            'status': 'no_data',
                            'articles': 0
                        }
                        log.warning(f"⚠️ {scraper_name}: Không có dữ liệu")
                else:
                    log.error(f"❌ {scraper_name}: Unexpected result type - {type(result)}")
                    stats[scraper_name] = {
                        'status': 'error',
                        'articles': 0,
                        'error': f'Unexpected result type: {type(result)}'
                    }
    
    except Exception as e:
        log.error(f"❌ Lỗi trong ThreadPoolExecutor: {e}")
        for _, scraper_name in scrapers:
            if scraper_name not in stats:
                stats[scraper_name] = {
                    'status': 'error',
                    'articles': 0,
                    'error': str(e)
                }
    
    # Combine results
    total_time = round(time.time() - start_time, 2)
    
    if results:
        try:
            df_all = pd.concat(results, ignore_index=True)
            
            # Remove duplicates if any
            initial_count = len(df_all)
            if 'Link' in df_all.columns:
                df_all = df_all.drop_duplicates(subset=['Link'], keep='first')
                final_count = len(df_all)
                if initial_count != final_count:
                    log.info(f"🔄 Removed {initial_count - final_count} duplicates")
            
            log.info(f"🎉 Pipeline hoàn thành: {len(df_all)} bài viết trong {total_time}s")
            
            # Print detailed stats
            log.info("📊 Chi tiết kết quả:")
            total_articles = 0
            successful_scrapers = 0
            
            for scraper_name, stat in stats.items():
                status = stat['status']
                articles = stat['articles']
                
                if status == 'success':
                    log.info(f"   ✅ {scraper_name}: {articles} bài viết")
                    total_articles += articles
                    successful_scrapers += 1
                elif status == 'no_data':
                    log.info(f"   ⚠️ {scraper_name}: Không có dữ liệu")
                else:
                    error = stat.get('error', 'Unknown error')
                    log.info(f"   ❌ {scraper_name}: Lỗi - {error}")
            
            stats['summary'] = {
                'total_articles': len(df_all),
                'total_time': total_time,
                'successful_scrapers': successful_scrapers,
                'total_scrapers': len(scrapers)
            }
            
            return df_all, stats
            
        except Exception as e:
            log.error(f"❌ Lỗi khi concat DataFrames: {e}")
            return pd.DataFrame(), stats
    else:
        log.error(f"❌ Không có dữ liệu sau {total_time}s")
        stats['summary'] = {
            'total_articles': 0,
            'total_time': total_time,
            'successful_scrapers': 0,
            'total_scrapers': len(scrapers)
        }
        return pd.DataFrame(), stats

def tintuc() -> pd.DataFrame:
    """
    Wrapper đồng bộ để gọi từ Airflow tasks
    """
    try:
        # Kiểm tra event loop
        try:
            loop = asyncio.get_running_loop()
            log.warning("⚠️ Event loop đã tồn tại, tạo new event loop")
            # Nếu đã có loop, tạo loop mới trong thread
            import threading
            result = []
            exception = []
            
            def run_in_new_thread():
                try:
                    new_loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(new_loop)
                    df, stats = new_loop.run_until_complete(tintuc_async())
                    result.append(df)
                except Exception as e:
                    exception.append(e)
                finally:
                    new_loop.close()
            
            thread = threading.Thread(target=run_in_new_thread)
            thread.start()
            thread.join(timeout=400)  # 6 phút 40 giây timeout
            
            if thread.is_alive():
                log.error("❌ Thread timeout")
                return pd.DataFrame()
            
            if exception:
                raise exception[0]
            
            return result[0] if result else pd.DataFrame()
            
        except RuntimeError:
            # Không có event loop, chạy bình thường
            df, stats = asyncio.run(tintuc_async())
            return df
            
    except Exception as e:
        log.error(f"❌ Lỗi trong tintuc(): {e}")
        log.error(f"Chi tiết lỗi: {traceback.format_exc()}")
        return pd.DataFrame()

async def main():
    """
    Function để test async pipeline
    """
    df, stats = await tintuc_async()
    print(f"\n🎯 Kết quả: {len(df)} bài viết")
    print(f"📊 Stats: {stats}")
    
    if not df.empty:
        print(f"\n📝 Sample data:")
        print(df.head())
        print(f"\n📋 Columns: {df.columns.tolist()}")
    
    return df, stats

