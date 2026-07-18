import requests
import json
import pandas as pd
from sqlalchemy import create_engine,text
from psycopg2.extras import execute_values
import concurrent.futures
from utils.create_list.symbol_list import HOSE, HNX, UPCOM, DERIVATIVES, CW, HNXBOND, ETFHOSE, indices, custom_list
import logging

# Thiết lập logging 
log=logging.getLogger(__name__)

engine = create_engine(
    "postgresql+psycopg2://root:Dnl_123456@tanhungsoft.com:5432/dnl"
)
def chucvu(symbol):
    table = f'leader."{symbol}"'

    # kiểm tra bảng có tồn tại không
    check_sql = text("""
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'leader'
            AND table_name = :tbl
        );
    """)

    with engine.connect() as conn:
        exists = conn.execute(check_sql, {"tbl": symbol}).scalar()

    if not exists:
        return pd.DataFrame(columns=["name", "relatedPersonPosition"])

    query = text(f"""
        SELECT "name", "positionName" AS "relatedPersonPosition"
        FROM leader."{symbol}"
    """)

    return pd.read_sql(query, engine)

def insider_trans(symbol):
    symbol = symbol.upper()
    url = f"https://iq.vietcap.com.vn/api/iq-insight-service/v1/company/{symbol}/insider-transaction?page=0&size=1000"
    headers = {
        "Referer": "https://trading.vietcap.com.vn/",
        "User-Agent": "Mozilla/5.0"
    }

    try:
        df = pd.json_normalize(
            requests.get(url, headers=headers).json()["data"]["content"]
        )
    except:
        print(f"❌ Không có dữ liệu '{symbol}'")
        return pd.DataFrame()

    if df.empty:
        return df

    # ──────────────────────────────────────────────
    # Chuẩn hóa dữ liệu
    # ──────────────────────────────────────────────
    # df.insert(0, "ticker", symbol)
    # bảo đảm tồn tại cột
    if "traderNameVi" not in df.columns:
        df["traderNameVi"] = None
    if "traderOrganNameVi" not in df.columns:
        df["traderOrganNameVi"] = None

    # gộp tên người + tổ chức
    df["traderNameVi"] = df["traderNameVi"].combine_first(df["traderOrganNameVi"])

    for col in ["traderPositionVi", "relativeNameVi", "roleNameVi"]:
        if col not in df.columns:
            df[col] = None
        df[col] = df[col].replace({"-": None, "": None})

    num_cols = ["shareRegister","shareBeforeTrade","shareAcquire","shareAfterTrade"]
    df[num_cols] = df[num_cols].apply(pd.to_numeric, errors="coerce").astype(float)

    drop_cols = [
        "id","organCode","eventNameEn","eventCode","traderPersonId",
        "sourceUrlVi","sourceUrlEn","tradeStatusEn",
        "traderNameEn","traderPositionEn","actionTypeCode","actionTypeEn",
        "relativeNameEn","icbCodeLv1","traderOrganNameEn",
        "displayDate1","displayDate2","roleNameEn","traderOrganNameVi"
    ]
    df = df.drop(columns=drop_cols, errors="ignore")

    df["eventNameVi"] = df["eventNameVi"].str.replace(r"^Giao dịch nội bộ: ", "", regex=True)

    date_cols = ["startDate","endDate","publicDate"]
    df[date_cols] = df[date_cols].apply(
        lambda col: pd.to_datetime(col, errors="coerce").dt.strftime("%Y-%m-%d")
    )

    df["ownershipAfterTradePct"] = (df["ownershipAfterTrade"] * 100).round(2)

    # ──────────────────────────────────────────────
    # Merge chức vụ từ bảng leader.{symbol}
    # ──────────────────────────────────────────────
    cv = chucvu(symbol)[["name", "relatedPersonPosition"]]
    df = df.merge(cv, left_on="relativeNameVi", right_on="name", how="left")
    df = df.drop(columns=["name"])

    not_related = df["eventNameVi"] != "Giao dịch người liên quan"
    df.loc[not_related, ["relativeNameVi", "relatedPersonPosition", "roleNameVi"]] = None

    # ──────────────────────────────────────────────
    # Chọn cột cuối & rename
    # ──────────────────────────────────────────────
    df = df[[
        "ticker", "eventNameVi", "traderNameVi", "traderPositionVi",
        "actionTypeVi", "tradeStatusVi",
        "shareRegister", "shareBeforeTrade", "shareAcquire", "shareAfterTrade",
        "ownershipAfterTradePct", "startDate", "endDate", "publicDate",
        "relativeNameVi", "relatedPersonPosition", "roleNameVi"
    ]]

    df = df.rename(columns={
        "ticker": "symbol",
        "eventNameVi": "transactionType",
        "traderNameVi": "trader",
        "traderPositionVi": "position",
        "actionTypeVi": "buySellType",
        "tradeStatusVi": "status",
        "relativeNameVi": "relatedPerson",
        "roleNameVi": "relationship",
    })

    return df

def save_pg(symbol):
    try:
        symbol = symbol.upper()
        create_schema_sql = 'CREATE SCHEMA IF NOT EXISTS insider_transaction;'
        create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS insider_transaction."{symbol}" (
                "symbol" TEXT,
                "transactionType" TEXT,
                "trader" TEXT,
                "position" TEXT,
                "buySellType" TEXT,
                "status" TEXT,
                "shareRegister" DOUBLE PRECISION,
                "shareBeforeTrade" DOUBLE PRECISION,
                "shareAcquire" DOUBLE PRECISION,
                "shareAfterTrade" DOUBLE PRECISION,
                "ownershipAfterTradePct" DOUBLE PRECISION,
                "startDate" DATE,
                "endDate" DATE,
                "publicDate" DATE,
                "relatedPerson" TEXT,
                "relatedPersonPosition" TEXT,
                "relationship" TEXT
            );
        """

        with engine.begin() as conn:
            conn.execute(text(create_schema_sql))
            conn.execute(text(create_table_sql))

        df = insider_trans(symbol)

        if df.empty:
            return f"⚠ Không có dữ liệu insider {symbol}"

        with engine.begin() as conn:

            df.to_sql(
                name=f'{symbol}',
                schema="insider_transaction",
                con=conn,
                if_exists="replace",
                index=False,
                method="multi",
                chunksize=500
            )

        return f"✔ {symbol}: {len(df)} rows inserted"
    except Exception as e:
        return f"❌ Lỗi {symbol}: {e}"

def update_all_symbol(symbol_list):
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as ex:
        futures = {ex.submit(save_pg, sym): sym for sym in symbol_list}
        for future in concurrent.futures.as_completed(futures):
            results.append(future.result())
    return results

def save_all_pg():
    result = []
    result += update_all_symbol(HOSE)
    result += update_all_symbol(HNX)
    result += update_all_symbol(UPCOM)
    # result += update_all_symbol(DERIVATIVES)
    # result += update_all_symbol(CW)
    # result += update_all_symbol(HNXBOND)
    # result += update_all_symbol(ETFHOSE)
    # result += update_all_symbol(indices)
    # result += update_all_symbol(custom_list)
    
    errors = [msg for msg in result if msg.startswith("❌") or msg.startswith("⚠️")]

    log.info(f"✅ Tổng số mã xử lý: {len(result)}")
    log.info(f"❌ Tổng số lỗi: {len(errors)}")

    if errors:
        log.warning("📛 Chi tiết các mã bị lỗi:")
        for err in errors:
            log.warning(err)

    # if len(errors) >= 5:
    #     raise Exception("Task thất bại vì có lỗi:\n" + "\n".join(errors))

    log.info("🎉 Hoàn thành cập nhật tất cả mã.")
    return result
