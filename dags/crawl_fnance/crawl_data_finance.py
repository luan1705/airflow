import requests
import time
import psycopg2
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# ==== Danh sách HOSE ====
hose = [ "AAA", "AAM", "AAT", "ABR", "ABS", "ABT", "ACB", "ACC", "ACG", "ACL", "ADG", "ADP", "ADS", "AGG", "AGM", "AGR", "ANV", "APG", "APH", "ASG", "ASM", "ASP", "AST", "BAF", "BBC", "BCE", "BCG", "BCM", "BFC", "BHN", "BIC", "BID", "BKG", "BMC", "BMI", "BMP", "BRC", "BSI", "BSR", "BTP", "BTT", "BVH", "BWE", "C32", "C47", "CCI", "CCL", "CDC", "CHP", "CIG", "CII", "CKG", "CLC", "CLL", "CLW", "CMG", "CMV", "CMX", "CNG", "COM", "CRC", "CRE", "CSM", "CSV", "CTD", "CTF", "CTG", "CTI", "CTR", "CTS", "CVT", "D2D", "DAH", "DAT", "DBC", "DBD", "DBT", "DC4", "DCL", "DCM", "DGC", "DGW", "DHA", "DHC", "DHG", "DHM", "DIG", "DLG", "DMC", "DPG", "DPM", "DPR", "DQC", "DRC", "DRH", "DRL", "DSC", "DSE", "DSN", "DTA", "DTL", "DTT", "DVP", "DXG", "DXS", "DXV", "E1VFVN30", "EIB", "ELC", "EVE", "EVF", "EVG", "FCM", "FCN", "FDC", "FIR", "FIT", "FMC", "FPT", "FRT", "FTS", "FUCTVGF3", "FUCTVGF4", "FUCTVGF5", "FUCVREIT", "FUEABVND", "FUEBFVND", "FUEDCMID", "FUEFCV50", "FUEIP100", "FUEKIV30", "FUEKIVFS", "FUEKIVND", "FUEMAV30", "FUEMAVND", "FUESSV30", "FUESSV50", "FUESSVFL", "FUETCC50", "FUEVFVND", "FUEVN100", "GAS", "GDT", "GEE", "GEG", "GEX", "GIL", "GMD", "GMH", "GSP", "GTA", "GVR", "HAG", "HAH", "HAP", "HAR", "HAS", "HAX", "HCD", "HCM", "HDB", "HDC", "HDG", "HHP", "HHS", "HHV", "HID", "HII", "HMC", "HNA", "HPG", "HPX", "HQC", "HRC", "HSG", "HSL", "HT1", "HTG", "HTI", "HTL", "HTN", "HTV", "HU1", "HUB", "HVH", "HVN", "HVX", "ICT", "IDI", "IJC", "ILB", "IMP", "ITC", "ITD", "JVC", "KBC", "KDC", "KDH", "KHG", "KHP", "KMR", "KOS", "KPF", "KSB", "L10", "LAF", "LBM", "LCG", "LDG", "LEC", "LGC", "LGL", "LHG", "LIX", "LM8", "LPB", "LSS", "MBB", "MCM", "MCP", "MDG", "MHC", "MIG", "MSB", "MSH", "MSN", "MWG", "NAB", "NAF", "NAV", "NBB", "NCT", "NHA", "NHH", "NHT", "NKG", "NLG", "NNC", "NO1", "NSC", "NT2", "NTL", "NVL", "NVT", "OCB", "OGC", "OPC", "ORS", "PAC", "PAN", "PC1", "PDN", "PDR", "PET", "PGC", "PGD", "PGI", "PGV", "PHC", "PHR", "PIT", "PJT", "PLP", "PLX", "PMG", "PNC", "PNJ", "POW", "PPC", "PSH", "PTB", "PTC", "PTL", "PVD", "PVP", "PVT", "QCG", "QNP", "RAL", "RDP", "REE", "RYG", "S4A", "SAB", "SAM", "SAV", "SBA", "SBG", "SBT", "SBV", "SC5", "SCR", "SCS", "SFC", "SFG", "SFI", "SGN", "SGR", "SGT", "SHA", "SHB", "SHI", "SHP", "SIP", "SJD", "SJS", "SKG", "SMA", "SMB", "SMC", "SPM", "SRC", "SRF", "SSB", "SSC", "SSI", "ST8", "STB", "STG", "STK", "SVC", "SVD", "SVI", "SVT", "SZC", "SZL", "TBC", "TCB", "TCD", "TCH", "TCI", "TCL", "TCM", "TCO", "TCR", "TCT", "TDC", "TDG", "TDH", "TDM", "TDP", "TDW", "TEG", "THG", "TIP", "TIX", "TLD", "TLG", "TLH", "TMP", "TMS", "TMT", "TN1", "TNC", "TNH", "TNI", "TNT", "TPB", "TPC", "TRA", "TRC", "TSC", "TTA", "TTE", "TTF", "TV2", "TVB", "TVS", "TVT", "TYA", "UIC", "VAF", "VCA", "VCB", "VCF", "VCG", "VCI", "VDP", "VDS", "VFG", "VGC", "VHC", "VHM", "VIB", "VIC", "VID", "VIP", "VIX", "VJC", "VMD", "VND", "VNE", "VNG", "VNL", "VNM", "VNS", "VOS", "VPB", "VPD", "VPG", "VPH", "VPI", "VPS", "VRC", "VRE", "VSC", "VSH", "VSI", "VTB", "VTO", "VTP", "YBM", "YEG" ]

# ==== Cấu hình ====
from_date = "20060101"
to_date = datetime.today().strftime("%Y%m%d")
event_codes = "DIV,ISS"
page_size = 200

# ==== DB Connection ====
conn = psycopg2.connect(
    dbname="vnsfintech",
    user="vnsfintech",
    password="@Vns123456",
    host="videv.cloud",
    port=5432
)
cur = conn.cursor()

# ==== Headers ====
headers = {
    "Accept": "application/json",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
    "Host": "iq.vietcap.com.vn",
    "Origin": "https://trading.vietcap.com.vn",
    "Referer": "https://trading.vietcap.com.vn/",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}

# ==== API Functions ====
def fetch_events(ticker, page):
    url = f"https://iq.vietcap.com.vn/api/iq-insight-service/v1/events?ticker={ticker}&fromDate={from_date}&toDate={to_date}&eventCode={event_codes}&page={page}&size={page_size}"
    resp = requests.get(url, headers=headers, timeout=10)
    resp.raise_for_status()
    return resp.json().get("data", {}).get("content", [])

def fetch_event_detail(event_id):
    url = f"https://iq.vietcap.com.vn/api/iq-insight-service/v1/events/{event_id}"
    resp = requests.get(url, headers=headers, timeout=10)
    resp.raise_for_status()
    return resp.json().get("data", {})

def fetch_news(ticker, page):
    url = f"https://iq.vietcap.com.vn/api/iq-insight-service/v1/news?ticker={ticker}&fromDate={from_date}&toDate={to_date}&languageId=1&page={page}&size={page_size}"
    resp = requests.get(url, headers=headers, timeout=10)
    resp.raise_for_status()
    return resp.json().get("data", {}).get("content", [])

def fetch_news_detail(news_id):
    url = f"https://iq.vietcap.com.vn/api/iq-insight-service/v1/news/{news_id}"
    resp = requests.get(url, headers=headers, timeout=10)
    resp.raise_for_status()
    data = resp.json().get("data", {})
    if not data:
        return None
    # Bỏ hết URL
    data["newsFullContent"] = ""
    data["newsImageUrl"] = ""
    data["newsSmallImageUrl"] = ""
    data["newsSourceLink"] = ""
    return data

# ==== Hàm xử lý từng ticker (1 thread) ====
def process_ticker(ticker):
    conn = psycopg2.connect(
        dbname="vnsfintech",
        user="vnsfintech",
        password="@Vns123456",
        host="videv.cloud",
        port=5432
    )
    cur = conn.cursor()

    print(f"🚀 Thread xử lý: {ticker}")

    # Lấy ngày mới nhất
    cur.execute("SELECT MAX(displayDate1) FROM vnstock.events WHERE ticker=%s", (ticker,))
    latest_event_date = cur.fetchone()[0]

    cur.execute("SELECT MAX(publicDate) FROM vnstock.news WHERE ticker=%s", (ticker,))
    latest_news_date = cur.fetchone()[0]

    # ==== Crawl Events ====
    page = 0
    while True:
        events = fetch_events(ticker, page)
        if not events:
            break

        stop = False
        for item in events:
            if latest_event_date and item.get("displayDate1") <= latest_event_date:
                stop = True
                break

            cur.execute("""
                INSERT INTO vnstock.events (ticker, eventCode, eventTitleVi, eventTitleEn, displayDate1, recordDate, exrightDate, listingDate, exerciseRatio)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT DO NOTHING
            """, (
                ticker,
                item.get("eventCode"),
                item.get("eventTitleVi"),
                item.get("eventTitleEn"),
                item.get("displayDate1"),
                item.get("recordDate"),
                item.get("exrightDate"),
                item.get("listingDate"),
                item.get("exerciseRatio"),
            ))

            event_detail = fetch_event_detail(item.get("id"))
            if event_detail:
                cur.execute("""
                    INSERT INTO vnstock.event_details (
                        id, organNameEn, organNameVi, displayDate1, displayDate2, ticker, eventCode, publicDate,
                        eventTitleVi, eventTitleEn, recordDate, exrightDate, exerciseRatio, dividendYear,
                        dividendStageNameVi, dividendStageNameEn, listingDate, issueMethodCode, issueMethodNameVi,
                        issueMethodNameEn, issueVolumn, totalValue, issueStatusCode, issueStatusVi, issueStatusEn, icbCodeLv1
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (id) DO NOTHING
                """, (
                    event_detail.get("id"),
                    event_detail.get("organNameEn"),
                    event_detail.get("organNameVi"),
                    event_detail.get("displayDate1"),
                    event_detail.get("displayDate2"),
                    event_detail.get("ticker"),
                    event_detail.get("eventCode"),
                    event_detail.get("publicDate"),
                    event_detail.get("eventTitleVi"),
                    event_detail.get("eventTitleEn"),
                    event_detail.get("recordDate"),
                    event_detail.get("exrightDate"),
                    event_detail.get("exerciseRatio"),
                    event_detail.get("dividendYear"),
                    event_detail.get("dividendStageNameVi"),
                    event_detail.get("dividendStageNameEn"),
                    event_detail.get("listingDate"),
                    event_detail.get("issueMethodCode"),
                    event_detail.get("issueMethodNameVi"),
                    event_detail.get("issueMethodNameEn"),
                    event_detail.get("issueVolumn"),
                    event_detail.get("totalValue"),
                    event_detail.get("issueStatusCode"),
                    event_detail.get("issueStatusVi"),
                    event_detail.get("issueStatusEn"),
                    event_detail.get("icbCodeLv1")
                ))

        conn.commit()
        if stop:
            break
        page += 1

    # ==== Crawl News ====
    page = 0
    while True:
        news_list = fetch_news(ticker, page)
        if not news_list:
            break

        stop = False
        for item in news_list:
            if latest_news_date and item.get("publicDate") <= latest_news_date:
                stop = True
                break

            cur.execute("""
                INSERT INTO vnstock.news (id, newsId, language, ticker, newsTitle, newsSubTitle, newsShortContent, newsFullContent, newsImageUrl, newsSource, newsSourceLink, publicDate)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (id) DO NOTHING
            """, (
                str(item.get("id")),
                str(item.get("newsId")),
                item.get("language"),
                ticker,
                item.get("newsTitle"),
                item.get("newsSubTitle"),
                item.get("newsShortContent"),
                item.get("newsFullContent"),
                item.get("newsImageUrl"),
                item.get("newsSource"),
                item.get("newsSourceLink"),
                item.get("publicDate"),
            ))

            news_detail = fetch_news_detail(item.get("id"))
            if news_detail:
                cur.execute("""
                    INSERT INTO vnstock.news_details (
                        id, newsId, language, organCode, newsCategoryCode, icbCode, comGroupCode, expertId,
                        ticker, newsTitle, friendlyTitle, newsSubTitle, friendlySubTitle,
                        newsShortContent, newsFullContent, newsImageUrl, newsSmallImageUrl,
                        newsSource, newsAuthor, newsKeyword, friendlyKeyword, publicDate,
                        isTranfer, createBy, updateBy, status, createDate, updateDate, sourceCode
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (id) DO NOTHING
                """, (
                    news_detail.get("id"),
                    news_detail.get("newsId"),
                    news_detail.get("language"),
                    news_detail.get("organCode"),
                    news_detail.get("newsCategoryCode"),
                    news_detail.get("icbCode"),
                    news_detail.get("comGroupCode"),
                    news_detail.get("expertId"),
                    news_detail.get("ticker"),
                    news_detail.get("newsTitle"),
                    news_detail.get("friendlyTitle"),
                    news_detail.get("newsSubTitle"),
                    news_detail.get("friendlySubTitle"),
                    news_detail.get("newsShortContent"),
                    news_detail.get("newsFullContent"),
                    news_detail.get("newsImageUrl"),
                    news_detail.get("newsSmallImageUrl"),
                    news_detail.get("newsSource"),
                    news_detail.get("newsAuthor"),
                    news_detail.get("newsKeyword"),
                    news_detail.get("friendlyKeyword"),
                    news_detail.get("publicDate"),
                    news_detail.get("isTranfer"),
                    news_detail.get("createBy"),
                    news_detail.get("updateBy"),
                    news_detail.get("status"),
                    news_detail.get("createDate"),
                    news_detail.get("updateDate"),
                    news_detail.get("sourceCode"),
                ))

        conn.commit()
        if stop:
            break
        page += 1

    cur.close()
    conn.close()
    print(f"✅ Hoàn tất {ticker}")

# ==== Run đa luồng ====
with ThreadPoolExecutor(max_workers=5) as executor:  # chỉnh số luồng theo nhu cầu
    futures = [executor.submit(process_ticker, ticker) for ticker in hose]

    for f in as_completed(futures):
        try:
            f.result()
        except Exception as e:
            print("❌ Lỗi:", e)

print("🎯 Toàn bộ xong")
