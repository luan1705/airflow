from sqlalchemy import create_engine, text
import concurrent.futures
import pandas as pd
import logging
from utils.create_list.symbol_list import total_list

logging.basicConfig(
    level=logging.INFO,                 # cấp log: DEBUG / INFO / WARNING / ERROR
    format="%(asctime)s [%(levelname)s] %(message)s",  # format log
    handlers=[
        logging.StreamHandler()         # in ra console
        # có thể thêm FileHandler nếu muốn ghi log ra file
    ]
)
# enginedb=create_engine('postgresql://vnsfintech:%40Vns123456@tanhungsoft.com:5432/vnsfintech')
enginedbnews=create_engine("postgresql+psycopg2://vnsfintech:Vns_123456@tanhungsoft.com:5433/vnsfintech")


def break_out_break_down(symbol):
    try:
        with enginedbnews.begin() as conn:
            dffull = pd.read_sql(f'SELECT * FROM "ohlcv"."{symbol}_1D" ORDER BY time ASC', con=conn)
            dffull["time"] = pd.to_datetime(dffull["time"], utc=True)
            dffull["time"] = dffull["time"].dt.tz_convert("Asia/Ho_Chi_Minh")
            today = pd.Timestamp.now(tz="Asia/Ho_Chi_Minh").normalize()
            row_now = dffull[dffull['time'] == today]
            if not row_now.empty:
                df_calc = dffull[dffull['time'] < today]
            else:
                df_calc = dffull.iloc[:-1]

            def top_bottom(df):
                return float(df["close"].max()), float(df["close"].min())

            top_all,    bot_all    = top_bottom(df_calc)
            top_3y,     bot_3y     = top_bottom(df_calc.tail(252 * 3))
            top_1y,     bot_1y     = top_bottom(df_calc.tail(252))
            top_1m,     bot_1m     = top_bottom(df_calc.tail(21))
            top_1w,     bot_1w     = top_bottom(df_calc.tail(5))

            result = {
                'symbol':       symbol,
                'price_top_all': top_all,  'price_bot_all': bot_all,
                'price_top_3y':  top_3y,   'price_bot_3y':  bot_3y,
                'price_top_1y':  top_1y,   'price_bot_1y':  bot_1y,
                'price_top_1m':  top_1m,   'price_bot_1m':  bot_1m,
                'price_top_1w':  top_1w,   'price_bot_1w':  bot_1w,
            }

            try:
                with enginedbnews.begin() as connew:
                    connew.execute(text("""
                        INSERT INTO "status"."break"
                        ("symbol",
                         "price_top_all", "price_bot_all",
                         "price_top_3y",  "price_bot_3y",
                         "price_top_1y",  "price_bot_1y",
                         "price_top_1m",  "price_bot_1m",
                         "price_top_1w",  "price_bot_1w")
                        VALUES
                        (:symbol,
                         :price_top_all, :price_bot_all,
                         :price_top_3y,  :price_bot_3y,
                         :price_top_1y,  :price_bot_1y,
                         :price_top_1m,  :price_bot_1m,
                         :price_top_1w,  :price_bot_1w)
                        ON CONFLICT ("symbol") DO UPDATE SET
                            "price_top_all" = EXCLUDED."price_top_all",
                            "price_bot_all" = EXCLUDED."price_bot_all",
                            "price_top_3y"  = EXCLUDED."price_top_3y",
                            "price_bot_3y"  = EXCLUDED."price_bot_3y",
                            "price_top_1y"  = EXCLUDED."price_top_1y",
                            "price_bot_1y"  = EXCLUDED."price_bot_1y",
                            "price_top_1m"  = EXCLUDED."price_top_1m",
                            "price_bot_1m"  = EXCLUDED."price_bot_1m",
                            "price_top_1w"  = EXCLUDED."price_top_1w",
                            "price_bot_1w"  = EXCLUDED."price_bot_1w";
                    """), result)
            except Exception as e:
                logging.error(f"Upsert break lỗi {symbol}: {e}")

    except Exception as e:
        logging.error(f'Lỗi khi kết nối DB hoặc đọc dữ liệu {symbol}: {e}')


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