from sqlalchemy import create_engine, text
import concurrent.futures
import pandas as pd
import logging
from .List.symbol_list import total_list

logging.basicConfig(
    level=logging.INFO,                 # cấp log: DEBUG / INFO / WARNING / ERROR
    format="%(asctime)s [%(levelname)s] %(message)s",  # format log
    handlers=[
        logging.StreamHandler()         # in ra console
        # có thể thêm FileHandler nếu muốn ghi log ra file
    ]
)
enginedb=create_engine('postgresql://vnsfintech:%40Vns123456@videv.cloud:5432/vnsfintech')
enginedbnews=create_engine("postgresql+psycopg2://vnsfintech:Vns_123456@videv.cloud:5433/vnsfintech")

def break_out_break_down(symbol):
    try:
        with enginedb.begin() as conn:
            # Đọc dữ liệu
            dffull = pd.read_sql(f'SELECT * FROM "history_tradingview"."{symbol}_1D"', con=conn)
            today=pd.Timestamp.today().normalize()
            row_now = dffull[dffull['time'] == today]
            if not row_now.empty:
                # price_now = row_now['close'].iloc[0]
                df_caculated = dffull[dffull['time'] < today]
            else:
                # price_now = dffull['close'].iloc[-1]
                df_caculated = dffull.iloc[:-2]
            
            price_topfull=df_caculated["close"].max()
            price_bottomfull=df_caculated["close"].min()

            # break_out_full = price_now > price_topfull
            # break_down_full = price_now < price_bottomfull

            df52w = df_caculated.tail(252)
            price_top52w = df52w["close"].max()
            price_bottom52w = df52w["close"].min()

            # break_out_52w = price_now > price_top52w
            # break_down_52w = price_now < price_bottom52w

            result = {
                'symbol': symbol,
                # 'price_now': float(price_now),
                'price_topfull': float(price_topfull),
                'price_bottomfull': float(price_bottomfull),
                # 'break_out_full': bool(break_out_full),
                # 'break_down_full': bool(break_down_full),
                'price_top52w': float(price_top52w),
                'price_bottom52w': float(price_bottom52w),
                # 'break_out_52w': bool(break_out_52w),
                # 'break_down_52w': bool(break_down_52w)
            }

            try:
                with enginedbnews.begin() as connew:
                    connew.execute(text("""
                        INSERT INTO "status"."break"
                        ("symbol",  
                         "price_topfull", "price_bottomfull",
                         "price_top52w", "price_bottom52w")
                        VALUES (:symbol, :price_topfull, :price_bottomfull,
                                :price_top52w, :price_bottom52w)
                        ON CONFLICT ("symbol") DO UPDATE SET
                                     "price_topfull"    = EXCLUDED."price_topfull",
                                     "price_bottomfull" = EXCLUDED."price_bottomfull",
                                     "price_top52w"     = EXCLUDED."price_top52w",
                                     "price_bottom52w"  = EXCLUDED."price_bottom52w";
                        """), result)
            except Exception as e:
                logging.error(f"Upsert break_out_break_down lỗi {symbol}: {e}")

    except Exception as e:
        logging.error(f'Lỗi khi kết nối DB hoặc đọc dữ liệu {symbol} : {e}')
    return

def break_all_symbol(symbol_list):
    messages = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(break_out_break_down, symbol) for symbol in symbol_list]
        for future in concurrent.futures.as_completed(futures):
            messages.append(future.result())
    return messages

def break_all():
    result = []
    result += break_all_symbol(total_list)
    return result