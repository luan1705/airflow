import requests
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import concurrent.futures
import logging
from utils.create_list.symbol_list import CW

# =========================
# LOGGING
# =========================
log = logging.getLogger(__name__)

# =========================
# DB
# =========================
engine = create_engine(
    "postgresql+psycopg2://vnsfintech:Vns_123456@tanhungsoft.com:5433/vnsfintech"
)

# =========================
# API
# =========================
headers = {
    'content-type': 'application/json',
    'referer': 'https://iboard.ssi.com.vn/',
    'user-agent': 'Mozilla/5.0'
}

# =========================
# CREATE TABLE
# =========================
def create_table():

    create_table_sql = '''
        CREATE TABLE IF NOT EXISTS info.warrant (
            symbol TEXT PRIMARY KEY,
            issuer TEXT,
            warrant_type TEXT,
            exercise_style TEXT,
            exercise_price DOUBLE PRECISION,
            exercise_ratio TEXT,
            underlying_symbol TEXT,
            underlying_price DOUBLE PRECISION,
            status TEXT,
            intrinsic_value DOUBLE PRECISION,
            break_even_price DOUBLE PRECISION,
            maturity_date DATE,
            last_trading_date DATE,
            days_to_maturity INTEGER,
            listed_share BIGINT
        );
    '''

    with engine.begin() as conn:
        conn.execute(text(create_table_sql))

# =========================
# GET WARRANT INFO
# =========================
def get_warrant_info(symbol):

    symbol = symbol.upper()

    url = f'https://iboard-query.ssi.com.vn/stock/{symbol}'

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()

        raw = response.json()['data']

        if not raw:
            return pd.DataFrame()

        df = pd.json_normalize(raw)

        result = pd.DataFrame({

            'symbol': df['stockSymbol'],

            'issuer': df['issuerName'],

            'warrant_type': df['coveredWarrantType'].replace({
                'C': 'Call',
                'P': 'Put'
            }),

            'exercise_style': np.select(
                [
                    df['companyNameEn'].fillna('').str.contains('/EU/'),
                    df['companyNameEn'].fillna('').str.contains('/AM/')
                ],
                [
                    'European',
                    'American'
                ],
                default=''
            ),

            'exercise_price': round(df['exercisePrice'] / 1000, 2),

            'exercise_ratio': df['exerciseRatio'],

            'underlying_symbol': df['underlyingSymbol'],

            'underlying_price': df['underlyingStockPrice'] / 1000,

            'status': df['status'],

            'intrinsic_value': round(df['intrinsic'] / 1000, 3),

            'break_even_price': round(df['breakEvenPoint'] / 1000, 2),

            'maturity_date': pd.to_datetime(
                df['maturityDate'],
                format='%d/%m/%Y',
                errors='coerce'
            ),

            'last_trading_date': pd.to_datetime(
                df['lastTradingDate'],
                format='%Y%m%d',
                errors='coerce'
            ),

            'days_to_maturity': df['dateMaturity'],

            'listed_share': df['listedShare']

        })

        return result

    except Exception as e:
        log.error(f"❌ {symbol}: {e}")
        return pd.DataFrame()

# =========================
# SAVE 1 SYMBOL
# =========================
def save_pg(symbol):

    try:

        df = get_warrant_info(symbol)

        if df.empty:
            return f"⚠ Không có dữ liệu {symbol}"

        upsert_sql = """
            INSERT INTO info.warrant (
                symbol,
                issuer,
                warrant_type,
                exercise_style,
                exercise_price,
                exercise_ratio,
                underlying_symbol,
                underlying_price,
                status,
                intrinsic_value,
                break_even_price,
                maturity_date,
                last_trading_date,
                days_to_maturity,
                listed_share
            )
            VALUES (
                :symbol,
                :issuer,
                :warrant_type,
                :exercise_style,
                :exercise_price,
                :exercise_ratio,
                :underlying_symbol,
                :underlying_price,
                :status,
                :intrinsic_value,
                :break_even_price,
                :maturity_date,
                :last_trading_date,
                :days_to_maturity,
                :listed_share
            )
            ON CONFLICT (symbol)
            DO UPDATE SET
                issuer = EXCLUDED.issuer,
                warrant_type = EXCLUDED.warrant_type,
                exercise_style = EXCLUDED.exercise_style,
                exercise_price = EXCLUDED.exercise_price,
                exercise_ratio = EXCLUDED.exercise_ratio,
                underlying_symbol = EXCLUDED.underlying_symbol,
                underlying_price = EXCLUDED.underlying_price,
                status = EXCLUDED.status,
                intrinsic_value = EXCLUDED.intrinsic_value,
                break_even_price = EXCLUDED.break_even_price,
                maturity_date = EXCLUDED.maturity_date,
                last_trading_date = EXCLUDED.last_trading_date,
                days_to_maturity = EXCLUDED.days_to_maturity,
                listed_share = EXCLUDED.listed_share
        """

        with engine.begin() as conn:

            conn.execute(
                text(upsert_sql),
                df.to_dict(orient='records')
            )

        return f"✔ {symbol}"

    except Exception as e:

        return f"❌ {symbol}: {str(e)}"
    
# =========================
# MULTITHREAD
# =========================
def update_all_symbol(symbols):

    results = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as ex:

        futures = {
            ex.submit(save_pg, sym): sym
            for sym in symbols
        }

        for future in concurrent.futures.as_completed(futures):
            results.append(future.result())

    return results

# =========================
# MAIN
# =========================
def save_all_pg():

    create_table()

    symbols = CW

    log.info(f"📌 Total CW symbols: {len(symbols)}")

    result = update_all_symbol(symbols)

    errors = [
        msg for msg in result
        if msg.startswith("❌") or msg.startswith("⚠")
    ]

    log.info(f"✅ Total processed: {len(result)}")
    log.info(f"❌ Total errors: {len(errors)}")

    if errors:
        for err in errors:
            log.warning(err)

    log.info("🎉 Done.")

    return result