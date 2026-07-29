from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool
import pandas as pd
import concurrent.futures
import logging

log = logging.getLogger(__name__)

engine = create_engine(
    "postgresql+psycopg2://root:Dnl_123456@tanhungsoft.com:5432/dnl",
    poolclass=NullPool
)


def calc_pepb(symbol: str):
    try:
        symbol = symbol.upper()

        lnst = pd.read_sql(f"""
            SELECT "yearReport", "lengthReport", "isa20"
            FROM income_statement."{symbol}"
            WHERE "lengthReport" != 5
            ORDER BY "yearReport", "lengthReport"
        """, engine)

        if lnst.empty:
            log.warning(f"⚠️ {symbol}: không có LNST")
            return

        lnst['yearReport'] = pd.to_numeric(
            lnst['yearReport'],
            errors='coerce'
        )
        lnst['lengthReport'] = pd.to_numeric(
            lnst['lengthReport'],
            errors='coerce'
        )
        lnst['isa20'] = pd.to_numeric(
            lnst['isa20'],
            errors='coerce'
        )

        # Thiếu năm/quý thì không thể merge.
        # isa20 lỗi vẫn giữ lại để kết quả tính thành NULL.
        lnst = lnst.dropna(
            subset=['yearReport', 'lengthReport']
        )

        if lnst.empty:
            log.warning(f"⚠️ {symbol}: không có năm/quý hợp lệ")
            return

        lnst = lnst.sort_values(
            ['yearReport', 'lengthReport']
        ).reset_index(drop=True)

        # Thiếu bất kỳ giá trị nào trong 4 quý thì TTM = NaN.
        lnst['lnst_ttm'] = lnst['isa20'].rolling(
            window=4,
            min_periods=4
        ).sum()

        idx = pd.read_sql(f"""
            SELECT
                "yearReport",
                "quarter" AS "lengthReport",
                "numberOfSharesMktCap"
            FROM index."{symbol}"
            WHERE "ratioType" = 'RATIO_TTM'
              AND "quarter" != 5
            ORDER BY "yearReport", "quarter"
        """, engine)

        idx['yearReport'] = pd.to_numeric(
            idx['yearReport'],
            errors='coerce'
        )
        idx['lengthReport'] = pd.to_numeric(
            idx['lengthReport'],
            errors='coerce'
        )
        idx['numberOfSharesMktCap'] = pd.to_numeric(
            idx['numberOfSharesMktCap'],
            errors='coerce'
        )

        # Thiếu năm/quý thì không thể merge.
        # Số cổ phiếu lỗi vẫn giữ lại để kết quả tính thành NULL.
        idx = idx.dropna(
            subset=['yearReport', 'lengthReport']
        )

        bs = pd.read_sql(f"""
            SELECT "yearReport", "lengthReport", "bsa78"
            FROM balance_sheet."{symbol}"
            WHERE "lengthReport" != 5
            ORDER BY "yearReport", "lengthReport"
        """, engine)

        bs['yearReport'] = pd.to_numeric(
            bs['yearReport'],
            errors='coerce'
        )
        bs['lengthReport'] = pd.to_numeric(
            bs['lengthReport'],
            errors='coerce'
        )
        bs['bsa78'] = pd.to_numeric(
            bs['bsa78'],
            errors='coerce'
        )

        # Thiếu năm/quý thì không thể merge.
        # bsa78 lỗi vẫn giữ lại để BVPS/PB thành NULL.
        bs = bs.dropna(
            subset=['yearReport', 'lengthReport']
        )

        quarterly = (
            lnst.merge(
                idx,
                on=['yearReport', 'lengthReport'],
                how='left'
            )
            .merge(
                bs,
                on=['yearReport', 'lengthReport'],
                how='left'
            )
        )

        if quarterly.empty:
            log.warning(f"⚠️ {symbol}: không có dữ liệu quý")
            return

        # Không chia cho 0.
        # Đổi số cổ phiếu bằng 0 thành NaN để kết quả lưu NULL.
        quarterly.loc[
            quarterly['numberOfSharesMktCap'] == 0,
            'numberOfSharesMktCap'
        ] = float('nan')

        quarterly['eps_ttm'] = (
            quarterly['lnst_ttm']
            / quarterly['numberOfSharesMktCap']
        )

        quarterly['bvps'] = (
            quarterly['bsa78']
            / quarterly['numberOfSharesMktCap']
        )

        quarterly = quarterly[
            [
                'yearReport',
                'lengthReport',
                'eps_ttm',
                'bvps'
            ]
        ].copy()

        quarterly['yearReport'] = quarterly['yearReport'].astype(float)
        quarterly['lengthReport'] = quarterly['lengthReport'].astype(float)

        quarterly = quarterly.sort_values(
            ['yearReport', 'lengthReport']
        ).reset_index(drop=True)

        latest_year = quarterly.iloc[-1]['yearReport']
        latest_quarter = quarterly.iloc[-1]['lengthReport']

        close = pd.read_sql(f"""
            SELECT
                time AT TIME ZONE '+07' AS date,
                close
            FROM ohlcv."{symbol}_1D"
            ORDER BY time
        """, engine)

        if close.empty:
            log.warning(f"⚠️ {symbol}: không có close")
            return

        close['close'] = pd.to_numeric(
            close['close'],
            errors='coerce'
        )

        close['date'] = pd.to_datetime(
            close['date'],
            errors='coerce'
        )

        close['date'] = (
            close['date']
            .dt.normalize()
            .dt.tz_localize(None)
        )

        # Không có ngày thì không thể insert vì time là primary key.
        # Close lỗi vẫn giữ để PE/PB thành NULL.
        close = close.dropna(subset=['date'])

        if close.empty:
            log.warning(f"⚠️ {symbol}: không có ngày hợp lệ")
            return

        close['yearReport'] = close['date'].dt.year.astype(float)
        close['lengthReport'] = close['date'].dt.quarter.astype(float)

        # Ngày thuộc quý chưa có BCTC thì dùng quý mới nhất hiện có.
        mask = (
            (close['yearReport'] > latest_year)
            |
            (
                (close['yearReport'] == latest_year)
                & (close['lengthReport'] > latest_quarter)
            )
        )

        close.loc[mask, 'yearReport'] = latest_year
        close.loc[mask, 'lengthReport'] = latest_quarter

        df = close.merge(
            quarterly,
            on=['yearReport', 'lengthReport'],
            how='left'
        )

        # EPS hoặc BVPS bằng 0 thì PE/PB phải là NULL,
        # tránh tạo inf khi chia.
        df.loc[
            df['eps_ttm'] == 0,
            'eps_ttm'
        ] = float('nan')

        df.loc[
            df['bvps'] == 0,
            'bvps'
        ] = float('nan')

        df['pe'] = (
            (df['close'] * 1000)
            / df['eps_ttm']
        ).round(2)

        df['pb'] = (
            (df['close'] * 1000)
            / df['bvps']
        ).round(2)

        df['bvps'] = df['bvps'].round(2)
        df['symbol'] = symbol

        df = df.rename(columns={
            'yearReport': 'calYear',
            'lengthReport': 'calQuarter'
        })

        df = df[
            [
                'symbol',
                'date',
                'pe',
                'pb',
                'bvps',
                'calYear',
                'calQuarter'
            ]
        ]

        if df.empty:
            log.warning(f"⚠️ {symbol}: không tính được PE/PB")
            return

        # Chuyển toàn bộ inf/-inf thành NaN,
        # sau đó phần insert sẽ đổi NaN thành NULL.
        df = df.replace(
            [float('inf'), float('-inf')],
            float('nan')
        )

        with engine.begin() as conn:
            conn.execute(text("""
                CREATE SCHEMA IF NOT EXISTS asset_pepb_history
            """))

            conn.execute(text(f"""
                CREATE TABLE IF NOT EXISTS asset_pepb_history."{symbol}" (
                    symbol          TEXT,
                    time            DATE PRIMARY KEY,
                    pe              DOUBLE PRECISION,
                    pb              DOUBLE PRECISION,
                    bvps            DOUBLE PRECISION,
                    "calYear"       DOUBLE PRECISION,
                    "calQuarter"    DOUBLE PRECISION
                )
            """))

            for _, row in df.iterrows():
                row_dict = {
                    key: None if pd.isna(value) else value
                    for key, value in row.to_dict().items()
                }

                conn.execute(text(f"""
                    INSERT INTO asset_pepb_history."{symbol}" (
                        symbol,
                        time,
                        pe,
                        pb,
                        bvps,
                        "calYear",
                        "calQuarter"
                    )
                    VALUES (
                        :symbol,
                        :date,
                        :pe,
                        :pb,
                        :bvps,
                        :calYear,
                        :calQuarter
                    )
                    ON CONFLICT (time) DO UPDATE SET
                        symbol = EXCLUDED.symbol,
                        pe = EXCLUDED.pe,
                        pb = EXCLUDED.pb,
                        bvps = EXCLUDED.bvps,
                        "calYear" = EXCLUDED."calYear",
                        "calQuarter" = EXCLUDED."calQuarter"
                """), row_dict)

        print(f"✅ {symbol}: {len(df)} dòng")

    except Exception:
        log.exception(f"❌ {symbol}")


def asset_pepb_history():
    symbols = pd.read_sql(
        text("""
            SELECT symbol
            FROM info.asset
            WHERE exchange IN ('HOSE', 'HNX', 'UPCOM')
        """),
        engine
    )['symbol'].dropna().astype(str).str.upper().unique().tolist()

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=5
    ) as executor:
        list(executor.map(calc_pepb, symbols))

    print("Hoàn tất!")