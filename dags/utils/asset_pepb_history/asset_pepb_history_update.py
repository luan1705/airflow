from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool
import pandas as pd
import concurrent.futures
import logging
from utils.exchange_history.breadth.update import pepb_breadth_update

log = logging.getLogger(__name__)

engine = create_engine(
    "postgresql+psycopg2://root:Dnl_123456@tanhungsoft.com:5432/dnl",
    poolclass=NullPool
)


def calc_pepb_quarter(
    symbol: str,
    target_year: float,
    target_quarter: float
) -> bool:
    try:
        symbol = str(symbol).upper()

        # ==================================================
        # LNST và LNST TTM
        # ==================================================
        lnst = pd.read_sql(f"""
            SELECT "yearReport", "lengthReport", "isa20"
            FROM income_statement."{symbol}"
            WHERE "lengthReport" != 5
            ORDER BY "yearReport", "lengthReport"
        """, engine)

        if lnst.empty:
            log.warning(f"⚠️ {symbol}: không có dữ liệu LNST")
            return False

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

        # Chỉ loại dòng thiếu năm/quý vì không thể xác định kỳ báo cáo.
        # isa20 lỗi vẫn được giữ để kết quả PE thành NULL.
        lnst = lnst.dropna(
            subset=['yearReport', 'lengthReport']
        )

        if lnst.empty:
            log.warning(f"⚠️ {symbol}: không có năm/quý hợp lệ")
            return False

        lnst = lnst.sort_values(
            ['yearReport', 'lengthReport']
        ).reset_index(drop=True)

        # Thiếu bất kỳ giá trị nào trong 4 quý thì lnst_ttm = NaN.
        lnst['lnst_ttm'] = lnst['isa20'].rolling(
            window=4,
            min_periods=4
        ).sum()

        row_q = lnst[
            (lnst['yearReport'] == float(target_year))
            & (lnst['lengthReport'] == float(target_quarter))
        ]

        if row_q.empty:
            log.warning(
                f"⚠️ {symbol}: không tìm thấy "
                f"Q{int(target_quarter)}/{int(target_year)}"
            )
            return False

        lnst_ttm = pd.to_numeric(
            pd.Series([row_q.iloc[-1]['lnst_ttm']]),
            errors='coerce'
        ).iloc[0]

        # ==================================================
        # Số lượng cổ phiếu
        # ==================================================
        idx = pd.read_sql(
            f"""
                SELECT "numberOfSharesMktCap"
                FROM index."{symbol}"
                WHERE "ratioType" = 'RATIO_TTM'
                  AND "lengthReport" = :quarter
                  AND "yearReport" = :year
                LIMIT 1
            """,
            engine,
            params={
                'quarter': target_quarter,
                'year': target_year
            }
        )

        if idx.empty:
            shares = float('nan')
        else:
            shares = pd.to_numeric(
                pd.Series([idx.iloc[0]['numberOfSharesMktCap']]),
                errors='coerce'
            ).iloc[0]

        # Không cho phép chia cho 0.
        if pd.notna(shares) and shares == 0:
            shares = float('nan')

        # ==================================================
        # Vốn chủ sở hữu
        # ==================================================
        bs = pd.read_sql(
            f"""
                SELECT "bsa78"
                FROM balance_sheet."{symbol}"
                WHERE "lengthReport" = :quarter
                  AND "yearReport" = :year
                LIMIT 1
            """,
            engine,
            params={
                'quarter': target_quarter,
                'year': target_year
            }
        )

        if bs.empty:
            bsa78 = float('nan')
        else:
            bsa78 = pd.to_numeric(
                pd.Series([bs.iloc[0]['bsa78']]),
                errors='coerce'
            ).iloc[0]

        # Dữ liệu lỗi/thiếu thì phép tính cho ra NaN.
        eps_ttm = (
            lnst_ttm / shares
            if pd.notna(lnst_ttm) and pd.notna(shares)
            else float('nan')
        )

        bvps = (
            bsa78 / shares
            if pd.notna(bsa78) and pd.notna(shares)
            else float('nan')
        )

        # EPS hoặc BVPS bằng 0 thì PE/PB không tính được.
        if pd.notna(eps_ttm) and eps_ttm == 0:
            eps_ttm = float('nan')

        if pd.notna(bvps) and bvps == 0:
            bvps = float('nan')

        # ==================================================
        # Giá đóng cửa từ đầu quý báo cáo đến hiện tại
        # ==================================================
        q_start = pd.Timestamp(
            year=int(target_year),
            month=int((target_quarter - 1) * 3 + 1),
            day=1
        )

        today = pd.Timestamp.today().normalize()

        close = pd.read_sql(
            f"""
                SELECT
                    time AT TIME ZONE '+07' AS date,
                    close
                FROM ohlcv."{symbol}_1D"
                WHERE (time AT TIME ZONE '+07')::date
                      BETWEEN :start AND :end
                ORDER BY time
            """,
            engine,
            params={
                'start': q_start.date(),
                'end': today.date()
            }
        )

        if close.empty:
            log.warning(f"⚠️ {symbol}: không có dữ liệu close")
            return False

        close['date'] = pd.to_datetime(
            close['date'],
            errors='coerce'
        )

        close['date'] = (
            close['date']
            .dt.normalize()
            .dt.tz_localize(None)
        )

        close['close'] = pd.to_numeric(
            close['close'],
            errors='coerce'
        )

        # Không có ngày thì không thể insert vì time là primary key.
        # Close lỗi vẫn giữ, PE/PB sẽ là NULL.
        close = close.dropna(subset=['date'])

        if close.empty:
            log.warning(f"⚠️ {symbol}: không có ngày hợp lệ")
            return False

        close['pe'] = (
            (close['close'] * 1000) / eps_ttm
            if pd.notna(eps_ttm)
            else float('nan')
        )

        close['pb'] = (
            (close['close'] * 1000) / bvps
            if pd.notna(bvps)
            else float('nan')
        )

        close['pe'] = close['pe'].round(2)
        close['pb'] = close['pb'].round(2)

        close['bvps'] = (
            round(float(bvps), 2)
            if pd.notna(bvps)
            else float('nan')
        )

        # Đổi Infinity thành NaN để ghi PostgreSQL thành NULL.
        close = close.replace(
            [float('inf'), float('-inf')],
            float('nan')
        )

        # ==================================================
        # Ghi database
        # ==================================================
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

            insert_sql = text(f"""
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
                    symbol       = EXCLUDED.symbol,
                    pe           = EXCLUDED.pe,
                    pb           = EXCLUDED.pb,
                    bvps         = EXCLUDED.bvps,
                    "calYear"    = EXCLUDED."calYear",
                    "calQuarter" = EXCLUDED."calQuarter"
            """)

            for _, row in close.iterrows():
                values = {
                    'symbol': symbol,
                    'date': (
                        None
                        if pd.isna(row['date'])
                        else row['date']
                    ),
                    'pe': (
                        None
                        if pd.isna(row['pe'])
                        else float(row['pe'])
                    ),
                    'pb': (
                        None
                        if pd.isna(row['pb'])
                        else float(row['pb'])
                    ),
                    'bvps': (
                        None
                        if pd.isna(row['bvps'])
                        else float(row['bvps'])
                    ),
                    'calYear': float(target_year),
                    'calQuarter': float(target_quarter)
                }

                conn.execute(insert_sql, values)

        log.info(
            f"✅ {symbol}: "
            f"Q{int(target_quarter)}/{int(target_year)} — "
            f"{len(close)} dòng"
        )

        return True

    except Exception:
        log.exception(f"❌ {symbol}: lỗi cập nhật PE/PB")
        return False


def _check_and_update(symbol: str) -> str | None:
    try:
        symbol = str(symbol).upper()

        # Quý mới nhất trong income_statement
        is_latest = pd.read_sql(f"""
            SELECT "yearReport", "lengthReport"
            FROM income_statement."{symbol}"
            WHERE "lengthReport" != 5
            ORDER BY "yearReport" DESC, "lengthReport" DESC
            LIMIT 1
        """, engine)

        if is_latest.empty:
            return None

        latest_year = pd.to_numeric(
            pd.Series([is_latest.iloc[0]['yearReport']]),
            errors='coerce'
        ).iloc[0]

        latest_quarter = pd.to_numeric(
            pd.Series([is_latest.iloc[0]['lengthReport']]),
            errors='coerce'
        ).iloc[0]

        if pd.isna(latest_year) or pd.isna(latest_quarter):
            log.warning(f"⚠️ {symbol}: năm/quý mới nhất không hợp lệ")
            return None

        # Quý mới nhất đang có trong asset_pepb_history
        try:
            pepb_latest = pd.read_sql(f"""
                SELECT "calYear", "calQuarter"
                FROM asset_pepb_history."{symbol}"
                ORDER BY time DESC
                LIMIT 1
            """, engine)

            if pepb_latest.empty:
                pepb_year = 0.0
                pepb_quarter = 0.0
            else:
                pepb_year = pd.to_numeric(
                    pd.Series([pepb_latest.iloc[0]['calYear']]),
                    errors='coerce'
                ).iloc[0]

                pepb_quarter = pd.to_numeric(
                    pd.Series([pepb_latest.iloc[0]['calQuarter']]),
                    errors='coerce'
                ).iloc[0]

                if pd.isna(pepb_year):
                    pepb_year = 0.0

                if pd.isna(pepb_quarter):
                    pepb_quarter = 0.0

        except Exception:
            # Bảng chưa tồn tại thì xem như chưa có dữ liệu.
            pepb_year = 0.0
            pepb_quarter = 0.0

        has_new_report = (
            latest_year > pepb_year
            or (
                latest_year == pepb_year
                and latest_quarter > pepb_quarter
            )
        )

        if not has_new_report:
            return None

        log.info(
            f"🔄 {symbol}: có BCTC mới "
            f"Q{int(latest_quarter)}/{int(latest_year)}"
        )

        success = calc_pepb_quarter(
            symbol,
            float(latest_year),
            float(latest_quarter)
        )

        return symbol if success else None

    except Exception:
        log.exception(f"❌ {symbol}: lỗi kiểm tra BCTC")
        return None


def asset_pepb_history_update():
    symbols = pd.read_sql(
        text("""
            SELECT symbol
            FROM info.asset
            WHERE exchange IN ('HOSE', 'HNX', 'UPCOM')
        """),
        engine
    )['symbol']

    symbols = (
        symbols
        .dropna()
        .astype(str)
        .str.upper()
        .unique()
        .tolist()
    )

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=5
    ) as executor:
        results = list(
            executor.map(_check_and_update, symbols)
        )

    # Chỉ giữ symbol cập nhật thành công.
    updated_symbols = [
        symbol
        for symbol in results
        if symbol is not None
    ]

    if updated_symbols:
        print(
            f"Có {len(updated_symbols)} symbol BCTC mới "
            f"→ chạy pepb_breadth_update"
        )
        pepb_breadth_update()
    else:
        print("Không có BCTC mới → bỏ qua")

    print("Hoàn tất!")