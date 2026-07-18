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

# ─── METRIC DEFINITIONS ───────────────────────────────────────────────────────

METRICS = {
    'phi_tai_chinh': {
        'fa_chart_asset': [
            ('cash',                 'Tiền & tương đương tiền'),
            ('receivables',          'Phải thu'),
            ('inventory',            'Tồn kho'),
            ('fixedAssets',          'Tài sản cố định'),
            ('constructionInProgress','Xây dựng dở dang'),
            ('financialInvestments', 'Đầu tư tài chính'),
            ('otherAssets',          'Tài sản khác'),
            ('cashToTotalAssets',    'Tiền/TTS'),
            ('totalAssets',          'Tổng tài sản'),
        ],
        'fa_chart_capital': [
            ('debt',             'Nợ vay'),
            ('tradePayables',    'Nợ chiếm dụng'),
            ('retainedEarnings', 'Lợi nhuận chưa phân phối'),
            ('minorityInterest', 'Lợi ích cổ đông không kiểm soát'),
            ('charterCapital',   'Vốn điều lệ'),
            ('otherCapital',     'Nguồn vốn khác'),
            ('totalCapital',     'Tổng nguồn vốn'),
        ],
        'fa_chart_profit': [
            ('operatingProfit', 'Lợi nhuận kinh doanh'),
            ('financialProfit', 'Lợi nhuận tài chính'),
            ('otherProfit',     'Lợi nhuận khác'),
            ('netProfit',       'LNST công ty mẹ'),
        ],
        'fa_chart_cashflow': [
            ('operatingCF', 'Từ hoạt động kinh doanh'),
            ('investingCF', 'Từ hoạt động đầu tư'),
            ('financingCF', 'Từ hoạt động tài chính'),
            ('endingCash',  'Tiền và tương đương tiền cuối kỳ'),
        ],
        'fa_chart_cost': [
            ('cogs',            'Giá vốn hàng bán'),
            ('sellingExpense',  'Chi phí bán hàng'),
            ('adminExpense',    'Chi phí quản lý'),
            ('interestExpense', 'Chi phí lãi vay'),
            ('ebitdaCoverage',  'EBITDA/Lãi vay'),
            ('depreciation',    'Khấu hao TSCĐ'),
        ],
        'fa_chart_growth': [
            ('netProfit4Q', 'Lợi nhuận sau thuế 4QGN'),
            ('revenue4Q',   'Doanh thu 4QGN'),
        ],
        'fa_chart_efficiency': [
            ('roa',          'ROA (%)'),
            ('roe',          'ROE (%)'),
            ('grossMargin4Q','Biên lợi nhuận gộp 4QGN'),
            ('netMargin4Q',  'Biên lợi nhuận ròng 4QGN'),
        ],
        'fa_chart_leverage': [
            ('debtToEquity',   'Nợ vay/VCSH'),
            ('debtToAssets',   'Nợ/Nguồn vốn'),
            ('roa',            'ROA'),
            ('ebitdaCoverage', 'EBITDA/Lãi vay'),
        ],
    },
    'ngan_hang': {
        'fa_chart_asset': [
            ('cash',               'Tiền mặt, vàng bạc, đá quý'),
            ('depositAtSBV',       'Tiền gửi tại NHNN'),
            ('depositAtOtherBank', 'Tiền gửi & cho vay các TCTD khác'),
            ('securities',         'Chứng khoán kinh doanh & đầu tư'),
            ('customerLoans',      'Cho vay khách hàng'),
            ('fixedAssets',        'Tài sản cố định'),
            ('otherAssets',        'Tài sản khác'),
            ('creditGrowthYTD',    'Tăng trưởng tín dụng (%YTD)'),
            ('totalAssets',        'Tổng tài sản'),
        ],
        'fa_chart_capital': [
            ('govAndSBVDebt',    'Nợ chính phủ & NHNN'),
            ('interBankDeposit', 'Tiền gửi & vay của các TCTD khác'),
            ('customerDeposit',  'Tiền gửi của khách hàng'),
            ('issuedPapers',     'Phát hành giấy tờ có giá'),
            ('charterCapital',   'Vốn điều lệ'),
            ('otherCapital',     'Nguồn vốn khác'),
            ('depositGrowthYTD', 'Tăng trưởng huy động (%YTD)'),
            ('totalCapital',     'Tổng nguồn vốn'),
        ],
        'fa_chart_profit': [
            ('netInterestIncome', 'Thu nhập lãi thuần'),
            ('serviceIncome',     'Lãi/lỗ từ hoạt động dịch vụ'),
            ('otherIncome',       'Lãi/lỗ từ hoạt động khác'),
            ('netProfit',         'Lợi nhuận sau thuế'),
        ],
        'fa_chart_cost': [
            ('operatingExpense', 'Chi phí hoạt động'),
            ('provisionExpense', 'Chi phí dự phòng rủi ro tín dụng'),
            ('cir',              'CIR'),
        ],
        'fa_chart_efficiency': [
            ('nim', 'NIM'),
            ('cof', 'COF'),
            ('yea', 'YEA'),
        ],
        'fa_chart_growth': [
            ('toi4Q',       'Tổng thu nhập hoạt động 4QGN (TOI)'),
            ('netProfit4Q', 'LNST NH mẹ 4QGN'),
        ],
        'fa_chart_loan_quality': [
            ('loanLossProvision',  'Dự phòng cho vay khách hàng'),
            ('subStandardDebt',    'Nợ dưới tiêu chuẩn'),
            ('specialMentionDebt', 'Nợ cần chú ý'),
            ('doubtfulDebt',       'Nợ nghi ngờ'),
            ('lossDebt',           'Nợ xấu có khả năng mất vốn'),
            ('vamcBonds',          'Trái phiếu VAMC'),
            ('standardDebtRatio',  'Tỷ lệ nợ đạt chuẩn'),
        ],
        'fa_chart_npl': [
            ('npl',  'Tỷ lệ nợ xấu - NPL'),
            ('npl2', 'Tỷ lệ nợ không đủ chuẩn - NPL2'),
            ('llr',  'Tỷ lệ bao phủ nợ xấu - LLR'),
        ],
        'fa_chart_loan_structure': [
            ('longTermLoans',   'Cho vay dài hạn'),
            ('mediumTermLoans', 'Cho vay trung hạn'),
            ('shortTermLoans',  'Cho vay ngắn hạn'),
            ('ldr',             'Tỷ lệ LDR'),
        ],
        'fa_chart_deposit_structure': [
            ('demandDeposit', 'Tiền gửi không kỳ hạn'),
            ('termDeposit',   'Tiền gửi có kỳ hạn'),
            ('savingDeposit', 'Tiền gửi tiết kiệm'),
            ('otherDeposit',  'Tiền gửi khác'),
            ('casaRatio',     'Tỷ lệ CASA'),
        ],
    },
    'chung_khoan': {
        'fa_chart_asset': [
            ('cash',               'Tiền mặt'),
            ('fvtpl',              'FVTPL'),
            ('htm',                'HTM'),
            ('afs',                'AFS'),
            ('loans',              'Cho vay'),
            ('receivables',        'Phải thu'),
            ('fixedAssets',        'TSCĐ, BĐS, DDDH'),
            ('otherAssets',        'Tài sản khác'),
            ('proprietaryToAssets','Tự doanh/Tổng tài sản'),
        ],
        'fa_chart_capital': [
            ('debt',             'Nợ vay'),
            ('tradePayables',    'Nợ chiếm dụng'),
            ('retainedEarnings', 'Lợi nhuận chưa phân phối'),
            ('charterCapital',   'Vốn điều lệ'),
            ('assetRevaluation', 'Chênh lệch đánh giá tài sản'),
            ('otherCapital',     'Nguồn vốn khác'),
            ('debtToCapital',    'Nợ vay/Tổng nguồn vốn'),
        ],
        'fa_chart_profit': [
            ('fvtplProfit',     'FVTPL'),
            ('htmProfit',       'HTM'),
            ('afsProfit',       'AFS'),
            ('brokerageProfit', 'Môi giới'),
            ('loanProfit',      'Cho vay'),
            ('netProfit',       'LNST chủ sở hữu'),
            ('otherServices',   'Dịch vụ khác'),
        ],
        'fa_chart_growth': [
            ('revenue4Q',   'Doanh thu hoạt động 4QGN'),
            ('netProfit4Q', 'Lợi nhuận sau thuế 4QGN'),
        ],
        'fa_chart_efficiency': [
            ('roa', 'ROA (%)'),
            ('roe', 'ROE (%)'),
        ],
        'fa_chart_brokerage': [
            ('brokerageProfit', 'Lợi nhuận mảng môi giới'),
            ('brokerageMargin', 'Biên lợi nhuận gộp mảng môi giới'),
        ],
        'fa_chart_margin': [
            ('marginLoans',    'Margin (Cho vay)'),
            ('equity',         'Vốn chủ sở hữu'),
            ('marginToEquity', 'Margin/VCSH'),
        ],
        'fa_chart_proprietary': [
            ('listedStocks',     'Cổ phiếu niêm yết'),
            ('unlistedStocks',   'Cổ phiếu chưa niêm yết'),
            ('fundCertificates', 'Chứng chỉ quỹ'),
            ('bonds',            'Trái phiếu'),
            ('deposits',         'Tiền gửi và chứng chỉ tiền gửi'),
        ],
        'fa_chart_fvtpl': [
            ('listedStocks',     'Cổ phiếu niêm yết'),
            ('unlistedStocks',   'Cổ phiếu chưa niêm yết'),
            ('fundCertificates', 'Chứng chỉ quỹ'),
            ('bonds',            'Trái phiếu'),
            ('deposits',         'Tiền gửi và chứng chỉ tiền gửi'),
        ],
        'fa_chart_afs': [
            ('listedStocks',     'Cổ phiếu niêm yết'),
            ('unlistedStocks',   'Cổ phiếu chưa niêm yết'),
            ('fundCertificates', 'Chứng chỉ quỹ'),
            ('bonds',            'Trái phiếu'),
            ('deposits',         'Tiền gửi và chứng chỉ tiền gửi'),
        ],
        'fa_chart_htm': [
            ('listedStocks',     'Cổ phiếu niêm yết'),
            ('unlistedStocks',   'Cổ phiếu chưa niêm yết'),
            ('fundCertificates', 'Chứng chỉ quỹ'),
            ('bonds',            'Trái phiếu'),
            ('deposits',         'Tiền gửi và chứng chỉ tiền gửi'),
        ],
    },
    'bao_hiem': {
        'fa_chart_asset': [
            ('cash',                'Tiền & tương đương tiền'),
            ('shortTermInvestments','Đầu tư tài chính ngắn hạn'),
            ('receivables',         'Phải thu'),
            ('reinsuranceAssets',   'Tài sản tái bảo hiểm'),
            ('longTermInvestments', 'Đầu tư tài chính dài hạn'),
            ('fixedAssets',         'Tài sản cố định'),
            ('otherAssets',         'Tài sản khác'),
            ('investmentToAssets',  'Đầu tư tài chính/TTS'),
            ('totalAssets',         'Tổng tài sản'),
        ],
        'fa_chart_capital': [
            ('insuranceReserves', 'Dự phòng nghiệp vụ'),
            ('debt',              'Nợ vay'),
            ('tradePayables',     'Nợ chiếm dụng / Phải trả'),
            ('charterCapital',    'Vốn điều lệ'),
            ('retainedEarnings',  'Lợi nhuận chưa phân phối'),
            ('otherCapital',      'Nguồn vốn khác'),
            ('totalCapital',      'Tổng nguồn vốn'),
        ],
        'fa_chart_profit': [
            ('underwritingProfit', 'Lợi nhuận hoạt động kinh doanh bảo hiểm'),
            ('investmentProfit',   'Lợi nhuận hoạt động tài chính'),
            ('otherProfit',        'Lợi nhuận khác'),
            ('netProfit',          'LNST công ty mẹ'),
        ],
        'fa_chart_cost': [
            ('claimsExpense',       'Chi bồi thường thuần'),
            ('claimsReserveChange', 'Tăng/giảm dự phòng bồi thường'),
            ('commissionExpense',   'Chi hoa hồng bảo hiểm'),
            ('otherBizExpense',     'Chi phí hoạt động KD bảo hiểm khác'),
            ('adminExpense',        'Chi phí quản lý doanh nghiệp'),
        ],
        'fa_chart_efficiency': [
            ('lossRatio',       'Tỷ lệ bồi thường (Loss ratio)'),
            ('expenseRatio',    'Tỷ lệ chi phí (Expense ratio)'),
            ('combinedRatio',   'Tỷ lệ kết hợp (Combined ratio)'),
            ('retentionRatio',  'Tỷ lệ giữ lại (Retention ratio)'),
            ('investmentYield', 'Lợi suất đầu tư (Investment yield)'),
            ('roa',             'ROA (%)'),
            ('roe',             'ROE (%)'),
            ('netMargin4Q',     'Biên lợi nhuận ròng 4QGN'),
        ],
        'fa_chart_growth': [
            ('premiumRevenue4Q', 'Doanh thu phí bảo hiểm 4QGN'),
            ('netProfit4Q',      'Lợi nhuận sau thuế 4QGN'),
        ],
        'fa_chart_leverage': [
            ('debtToAssets',    'Nợ/Nguồn vốn'),
            ('reservesToEquity','Dự phòng nghiệp vụ/VCSH'),
            ('roa',             'ROA'),
        ],
        'fa_chart_reserve_structure': [
            ('upr',                'Dự phòng phí chưa được hưởng (UPR)'),
            ('claimsReserve',      'Dự phòng bồi thường'),
            ('catastropheReserve', 'Dự phòng dao động lớn'),
            ('mathReserve',        'Dự phòng toán học'),
            ('bonusReserve',       'Dự phòng chia lãi & đảm bảo cân đối'),
            ('otherReserve',       'Dự phòng nghiệp vụ khác'),
        ],
        'fa_chart_revenue_structure': [
            ('grossPremium',       'Doanh thu phí bảo hiểm gốc'),
            ('reinsurancePremium', 'Doanh thu phí nhận tái bảo hiểm'),
            ('cedingPremium',      'Phí nhượng tái bảo hiểm'),
            ('reserveChange',      'Tăng/giảm dự phòng phí'),
            ('netPremium',         'Doanh thu phí bảo hiểm thuần'),
            ('financialIncome',    'Doanh thu hoạt động tài chính'),
        ],
        'fa_chart_investment_portfolio': [
            ('htm',            'Đầu tư nắm giữ đến ngày đáo hạn (HTM)'),
            ('stInvestments',  'Đầu tư chứng khoán ngắn hạn'),
            ('ltDeposits',     'Tiền gửi có kỳ hạn tại các TCTD (dài hạn)'),
            ('ltBonds',        'Đầu tư trái phiếu (dài hạn)'),
            ('otherInvest',    'Đầu tư khác'),
            ('totalPortfolio', 'Tổng danh mục đầu tư'),
        ],
    },
}

# SCHEMA_TITLES = {
#     # Chung (4 ngành)
#     'fa_chart_asset':                'Cơ cấu tài sản',
#     'fa_chart_capital':              'Cơ cấu nguồn vốn',
#     'fa_chart_profit':               'Cơ cấu lợi nhuận',
#     'fa_chart_cashflow':             'Lưu chuyển tiền tệ',
#     'fa_chart_cost':                 'Cơ cấu chi phí',
#     'fa_chart_growth':               'Diễn biến tăng trưởng',
#     'fa_chart_efficiency':           'Hiệu quả hoạt động',
#     'fa_chart_leverage':             'Tình trạng sử dụng đòn bẩy',

#     # Ngân hàng
#     'fa_chart_loan_quality':         'Chất lượng cho vay',
#     'fa_chart_npl':                  'Tỷ lệ nợ xấu và bao phủ nợ xấu',
#     'fa_chart_loan_structure':       'Cơ cấu cho vay theo thời gian',
#     'fa_chart_deposit_structure':    'Cơ cấu tiền gửi',

#     # Chứng khoán
#     'fa_chart_brokerage':            'Mảng môi giới',
#     'fa_chart_margin':               'Cho vay ký quỹ',
#     'fa_chart_proprietary':          'Cơ cấu tự doanh',
#     'fa_chart_fvtpl':                'Cơ cấu FVTPL',
#     'fa_chart_afs':                  'Cơ cấu AFS',
#     'fa_chart_htm':                  'Cơ cấu HTM',

#     # Bảo hiểm
#     'fa_chart_reserve_structure':    'Cơ cấu dự phòng nghiệp vụ',
#     'fa_chart_revenue_structure':    'Cơ cấu doanh thu',
#     'fa_chart_investment_portfolio': 'Danh mục đầu tư',
# }


# ─── HELPERS ──────────────────────────────────────────────────────────────────

def safe_get(df, col):
    return df[col] if col in df.columns else pd.Series(0, index=df.index)


def upsert_df(df, schema, symbol, engine):
    if df.empty:
        return
    df = df.copy()
    df = df.where(pd.notnull(df), None)
    cols = df.columns.tolist()
    with engine.begin() as conn:
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {schema}."{symbol}" (
                "yearReport"   DOUBLE PRECISION,
                "lengthReport" DOUBLE PRECISION,
                PRIMARY KEY ("yearReport", "lengthReport")
            )
        """))
        for col in cols:
            if col not in ('yearReport', 'lengthReport'):
                conn.execute(text(f"""
                    ALTER TABLE {schema}."{symbol}"
                    ADD COLUMN IF NOT EXISTS "{col}" DOUBLE PRECISION
                """))
        for _, row in df.iterrows():
            row_dict = {k: (None if pd.isna(v) else v) for k, v in row.to_dict().items()}
            placeholders = ', '.join([f':{c}' for c in cols])
            col_list     = ', '.join([f'"{c}"' for c in cols])
            updates      = ', '.join([f'"{c}" = EXCLUDED."{c}"' for c in cols if c not in ('yearReport', 'lengthReport')])
            conn.execute(text(f"""
                INSERT INTO {schema}."{symbol}" ({col_list})
                VALUES ({placeholders})
                ON CONFLICT ("yearReport", "lengthReport") DO UPDATE SET {updates}
            """), row_dict)


# def upsert_df_daily(df, schema, symbol, engine):
#     """Upsert daily data với primary key là date"""
#     if df.empty:
#         return
#     df = df.copy()
#     df = df.where(pd.notnull(df), None)
#     cols = df.columns.tolist()
#     with engine.begin() as conn:
#         conn.execute(text(f"""
#             CREATE TABLE IF NOT EXISTS {schema}."{symbol}" (
#                 date DATE PRIMARY KEY
#             )
#         """))
#         for col in cols:
#             if col != 'date':
#                 conn.execute(text(f"""
#                     ALTER TABLE {schema}."{symbol}"
#                     ADD COLUMN IF NOT EXISTS "{col}" DOUBLE PRECISION
#                 """))
#         for _, row in df.iterrows():
#             row_dict = {k: (None if pd.isna(v) else v) for k, v in row.to_dict().items()}
#             placeholders = ', '.join([f':{c}' for c in cols])
#             col_list     = ', '.join([f'"{c}"' for c in cols])
#             updates      = ', '.join([f'"{c}" = EXCLUDED."{c}"' for c in cols if c != 'date'])
#             conn.execute(text(f"""
#                 INSERT INTO {schema}."{symbol}" ({col_list})
#                 VALUES ({placeholders})
#                 ON CONFLICT (date) DO UPDATE SET {updates}
#             """), row_dict)


def upsert_metric(schema, symbol, metrics, engine):
    table = f'{symbol}_METRIC'
    with engine.begin() as conn:
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {schema}."{table}" (
                "columnName" TEXT PRIMARY KEY,
                "titleVi"    TEXT,
                "titleEn"    TEXT
            )
        """))
        for col_name, title_vi in metrics:
            conn.execute(text(f"""
                INSERT INTO {schema}."{table}" ("columnName", "titleVi", "titleEn")
                VALUES (:col, :vi, :en)
                ON CONFLICT ("columnName") DO UPDATE SET
                    "titleVi" = EXCLUDED."titleVi",
                    "titleEn" = EXCLUDED."titleEn"
            """), {'col': col_name, 'vi': title_vi, 'en': col_name})


def read_bs(symbol, cols):
    try:
        return pd.read_sql(f"""
            SELECT "yearReport", "lengthReport", {', '.join([f'"{c}"' for c in cols])}
            FROM balance_sheet."{symbol}"
            ORDER BY "yearReport", "lengthReport"
        """, engine)
    except:
        return pd.DataFrame()


def read_is(symbol, cols):
    try:
        return pd.read_sql(f"""
            SELECT "yearReport", "lengthReport", {', '.join([f'"{c}"' for c in cols])}
            FROM income_statement."{symbol}"
            ORDER BY "yearReport", "lengthReport"
        """, engine)
    except:
        return pd.DataFrame()


def read_cf(symbol, cols):
    try:
        return pd.read_sql(f"""
            SELECT "yearReport", "lengthReport", {', '.join([f'"{c}"' for c in cols])}
            FROM cash_flow."{symbol}"
            ORDER BY "yearReport", "lengthReport"
        """, engine)
    except:
        return pd.DataFrame()


def read_note(symbol, cols):
    try:
        return pd.read_sql(f"""
            SELECT "yearReport", "lengthReport", {', '.join([f'"{c}"' for c in cols])}
            FROM note."{symbol}"
            ORDER BY "yearReport", "lengthReport"
        """, engine)
    except:
        return pd.DataFrame()


# def read_ohlcv(symbol):
#     try:
#         return pd.read_sql(f"""
#             SELECT (time AT TIME ZONE '+07')::date AS date, close
#             FROM ohlcv."{symbol}_1D"
#             ORDER BY time
#         """, engine)
#     except:
#         return pd.DataFrame()


# ─── PHI TÀI CHÍNH ────────────────────────────────────────────────────────────

def calc_fa_phi_tai_chinh(symbol):
    try:
        bs  = read_bs(symbol, ['bsa2','bsa5','bsa8','bsa15','bsa24','bsa29','bsa43',
                                'bsa53','bsa54','bsa56','bsa57','bsa58','bsa60','bsa68',
                                'bsa71','bsa78','bsa80','bsa90','bsa96','bsa163','bsa170','bsa210'])
        is_ = read_is(symbol, ['isa3','isa4','isa5','isa6','isa7','isa8','isa9','isa10',
                                'isa11','isa14','isa20','isa22'])
        cf  = read_cf(symbol, ['cfa2','cfa18','cfa26','cfa34','cfa38'])
        # ohlcv = read_ohlcv(symbol)

        if bs.empty or is_.empty:
            return

        df = bs.merge(is_, on=['yearReport','lengthReport'], how='outer') \
               .merge(cf,  on=['yearReport','lengthReport'], how='outer') \
               .sort_values(['yearReport','lengthReport']).reset_index(drop=True)

        keys = ['yearReport', 'lengthReport']

        # fa_chart_asset
        asset = df[keys].copy()
        asset['cash']                    = safe_get(df,'bsa2')
        asset['receivables']             = safe_get(df,'bsa8') + safe_get(df,'bsa24')
        asset['inventory']               = safe_get(df,'bsa15')
        asset['fixedAssets']             = safe_get(df,'bsa29')
        asset['constructionInProgress']  = safe_get(df,'bsa163')
        asset['financialInvestments']    = safe_get(df,'bsa5') + safe_get(df,'bsa43')
        asset['otherAssets']             = safe_get(df,'bsa53') - safe_get(df,'bsa2') - safe_get(df,'bsa8') - safe_get(df,'bsa24') - safe_get(df,'bsa15') - safe_get(df,'bsa29') - safe_get(df,'bsa163') - safe_get(df,'bsa5') - safe_get(df,'bsa43')
        asset['cashToTotalAssets']       = safe_get(df,'bsa2') / safe_get(df,'bsa53').replace(0, float('nan'))
        asset['totalAssets']             = safe_get(df,'bsa53')
        upsert_df(asset, 'fa_chart_asset', symbol, engine)
        upsert_metric('fa_chart_asset', symbol, METRICS['phi_tai_chinh']['fa_chart_asset'], engine)

        # fa_chart_capital
        capital = df[keys].copy()
        capital['debt']             = safe_get(df,'bsa56') + safe_get(df,'bsa71')
        capital['tradePayables']    = safe_get(df,'bsa57') + safe_get(df,'bsa58') + safe_get(df,'bsa60') + safe_get(df,'bsa68') + safe_get(df,'bsa170')
        capital['retainedEarnings'] = safe_get(df,'bsa90')
        capital['minorityInterest'] = safe_get(df,'bsa210')
        capital['charterCapital']   = safe_get(df,'bsa80')
        capital['otherCapital']     = safe_get(df,'bsa96') - capital['debt'] - capital['tradePayables'] - safe_get(df,'bsa90') - safe_get(df,'bsa210') - safe_get(df,'bsa80')
        capital['totalCapital']     = safe_get(df,'bsa96')
        upsert_df(capital, 'fa_chart_capital', symbol, engine)
        upsert_metric('fa_chart_capital', symbol, METRICS['phi_tai_chinh']['fa_chart_capital'], engine)

        # fa_chart_profit
        profit = df[keys].copy()
        profit['operatingProfit'] = safe_get(df,'isa5') + safe_get(df,'isa9') + safe_get(df,'isa10')
        profit['financialProfit'] = safe_get(df,'isa6') + safe_get(df,'isa7')
        profit['otherProfit']     = safe_get(df,'isa14')
        profit['netProfit']       = safe_get(df,'isa22')
        upsert_df(profit, 'fa_chart_profit', symbol, engine)
        upsert_metric('fa_chart_profit', symbol, METRICS['phi_tai_chinh']['fa_chart_profit'], engine)

        # fa_chart_cashflow
        cashflow = df[keys].copy()
        cashflow['operatingCF'] = safe_get(df,'cfa18')
        cashflow['investingCF'] = safe_get(df,'cfa26')
        cashflow['financingCF'] = safe_get(df,'cfa34')
        cashflow['endingCash']  = safe_get(df,'cfa38')
        upsert_df(cashflow, 'fa_chart_cashflow', symbol, engine)
        upsert_metric('fa_chart_cashflow', symbol, METRICS['phi_tai_chinh']['fa_chart_cashflow'], engine)

        # fa_chart_cost
        cost = df[keys].copy()
        cost['cogs']            = safe_get(df,'isa4').abs()
        cost['sellingExpense']  = safe_get(df,'isa9').abs()
        cost['adminExpense']    = safe_get(df,'isa10').abs()
        cost['interestExpense'] = safe_get(df,'isa8').abs()
        cost['ebitdaCoverage']  = safe_get(df,'isa11') / safe_get(df,'isa8').abs().replace(0, float('nan'))
        cost['depreciation']    = safe_get(df,'cfa2').abs()
        upsert_df(cost, 'fa_chart_cost', symbol, engine)
        upsert_metric('fa_chart_cost', symbol, METRICS['phi_tai_chinh']['fa_chart_cost'], engine)

        # TTM
        ttm    = df[df['lengthReport'].between(1,4)].copy()
        ln_4q  = safe_get(ttm,'isa20').rolling(4).sum()
        dt_4q  = safe_get(ttm,'isa3').rolling(4).sum()
        gop_4q = safe_get(ttm,'isa5').rolling(4).sum()
        avg_ta = safe_get(ttm,'bsa53').rolling(2).mean()
        avg_eq = safe_get(ttm,'bsa78').rolling(2).mean()

        # fa_chart_growth
        growth = ttm[keys].copy()
        growth['netProfit4Q'] = ln_4q.values
        growth['revenue4Q']   = dt_4q.values
        growth = growth.dropna(subset=['netProfit4Q','revenue4Q'], how='all')
        upsert_df(growth, 'fa_chart_growth', symbol, engine)
        upsert_metric('fa_chart_growth', symbol, METRICS['phi_tai_chinh']['fa_chart_growth'], engine)

        # fa_chart_efficiency
        eff = ttm[keys].copy()
        eff['roa']           = ln_4q / avg_ta.replace(0, float('nan'))
        eff['roe']           = ln_4q / avg_eq.replace(0, float('nan'))
        eff['grossMargin4Q'] = gop_4q / dt_4q.replace(0, float('nan'))
        eff['netMargin4Q']   = ln_4q  / dt_4q.replace(0, float('nan'))
        eff = eff.dropna(subset=['roa','roe'], how='all')
        upsert_df(eff, 'fa_chart_efficiency', symbol, engine)
        upsert_metric('fa_chart_efficiency', symbol, METRICS['phi_tai_chinh']['fa_chart_efficiency'], engine)

        # fa_chart_leverage
        lev = ttm[keys].copy()
        lev['debtToEquity']   = (safe_get(df,'bsa56') + safe_get(df,'bsa71')) / safe_get(df,'bsa78').replace(0, float('nan'))
        lev['debtToAssets']   = safe_get(df,'bsa54') / safe_get(df,'bsa96').replace(0, float('nan'))
        lev['roa']            = ln_4q / avg_ta.replace(0, float('nan'))
        lev['ebitdaCoverage'] = safe_get(ttm,'isa11') / safe_get(ttm,'isa8').abs().replace(0, float('nan'))
        upsert_df(lev, 'fa_chart_leverage', symbol, engine)
        upsert_metric('fa_chart_leverage', symbol, METRICS['phi_tai_chinh']['fa_chart_leverage'], engine)

        log.info(f"✅ {symbol} (phi tài chính)")
    except Exception as e:
        log.error(f"❌ {symbol}: {e}")


# ─── NGÂN HÀNG ────────────────────────────────────────────────────────────────

def calc_fa_ngan_hang(symbol):
    try:
        bs    = read_bs(symbol, ['bsa2','bsa29','bsa53','bsa80','bsa96',
                                  'bsb97','bsb98','bsb99','bsb103','bsb105','bsb106',
                                  'bsb111','bsb112','bsb113','bsb116'])
        is_   = read_is(symbol, ['isa20','isa22','isb25','isb26','isb27','isb30','isb38','isb39','isb41'])
        note  = read_note(symbol, ['nob39','nob40','nob41','nob42','nob43','nob44',
                                    'nob46','nob47','nob48','nob65','nob66','nob67','nob68','nob201'])
        # ohlcv = read_ohlcv(symbol)

        if bs.empty or is_.empty:
            return

        df = bs.merge(is_, on=['yearReport','lengthReport'], how='outer') \
               .sort_values(['yearReport','lengthReport']).reset_index(drop=True)

        df_note = bs.merge(note, on=['yearReport','lengthReport'], how='left') \
                    .sort_values(['yearReport','lengthReport']).reset_index(drop=True) if not note.empty else pd.DataFrame()

        keys = ['yearReport', 'lengthReport']

        def ytd_growth(series):
            return series / series.shift(4) - 1

        # fa_chart_asset
        asset = df[keys].copy()
        asset['cash']               = safe_get(df,'bsa2')
        asset['depositAtSBV']       = safe_get(df,'bsb97')
        asset['depositAtOtherBank'] = safe_get(df,'bsb98')
        asset['securities']         = safe_get(df,'bsb99') + safe_get(df,'bsb106')
        asset['customerLoans']      = safe_get(df,'bsb103')
        asset['fixedAssets']        = safe_get(df,'bsa29')
        asset['otherAssets']        = safe_get(df,'bsa53') - safe_get(df,'bsa2') - safe_get(df,'bsb97') - safe_get(df,'bsb98') - (safe_get(df,'bsb99') + safe_get(df,'bsb106')) - safe_get(df,'bsb103') - safe_get(df,'bsa29')
        asset['creditGrowthYTD']    = ytd_growth(safe_get(df,'bsb103'))
        asset['totalAssets']        = safe_get(df,'bsa53')
        upsert_df(asset, 'fa_chart_asset', symbol, engine)
        upsert_metric('fa_chart_asset', symbol, METRICS['ngan_hang']['fa_chart_asset'], engine)

        # fa_chart_capital
        capital = df[keys].copy()
        capital['govAndSBVDebt']    = safe_get(df,'bsb111')
        capital['interBankDeposit'] = safe_get(df,'bsb112')
        capital['customerDeposit']  = safe_get(df,'bsb113')
        capital['issuedPapers']     = safe_get(df,'bsb116')
        capital['charterCapital']   = safe_get(df,'bsa80')
        capital['otherCapital']     = safe_get(df,'bsa96') - safe_get(df,'bsb111') - safe_get(df,'bsb112') - safe_get(df,'bsb113') - safe_get(df,'bsb116') - safe_get(df,'bsa80')
        capital['depositGrowthYTD'] = ytd_growth(safe_get(df,'bsb112') + safe_get(df,'bsb113'))
        capital['totalCapital']     = safe_get(df,'bsa96')
        upsert_df(capital, 'fa_chart_capital', symbol, engine)
        upsert_metric('fa_chart_capital', symbol, METRICS['ngan_hang']['fa_chart_capital'], engine)

        # fa_chart_profit
        profit = df[keys].copy()
        profit['netInterestIncome'] = safe_get(df,'isb27')
        profit['serviceIncome']     = safe_get(df,'isb30')
        profit['otherIncome']       = safe_get(df,'isb38') - safe_get(df,'isb27') - safe_get(df,'isb30')
        profit['netProfit']         = safe_get(df,'isa20')
        upsert_df(profit, 'fa_chart_profit', symbol, engine)
        upsert_metric('fa_chart_profit', symbol, METRICS['ngan_hang']['fa_chart_profit'], engine)

        # fa_chart_cost
        cost = df[keys].copy()
        cost['operatingExpense'] = safe_get(df,'isb39').abs()
        cost['provisionExpense'] = safe_get(df,'isb41').abs()
        cost['cir']              = safe_get(df,'isb39').abs() / safe_get(df,'isb38').replace(0, float('nan'))
        upsert_df(cost, 'fa_chart_cost', symbol, engine)
        upsert_metric('fa_chart_cost', symbol, METRICS['ngan_hang']['fa_chart_cost'], engine)

        # TTM
        ttm              = df[df['lengthReport'].between(1,4)].copy()
        earning_assets   = safe_get(df,'bsb97') + safe_get(df,'bsb98') + safe_get(df,'bsb99') + safe_get(df,'bsb106') + safe_get(df,'bsb103')
        interest_bearing = safe_get(df,'bsb111') + safe_get(df,'bsb112') + safe_get(df,'bsb113') + safe_get(df,'bsb116')

        # fa_chart_efficiency
        eff = ttm[keys].copy()
        eff['nim'] = safe_get(ttm,'isb27').rolling(4).sum() / earning_assets.rolling(2).mean().replace(0, float('nan'))
        eff['cof'] = safe_get(ttm,'isb26').abs().rolling(4).sum() / interest_bearing.rolling(2).mean().replace(0, float('nan'))
        eff['yea'] = safe_get(ttm,'isb25').rolling(4).sum() / earning_assets.rolling(2).mean().replace(0, float('nan'))
        eff = eff.dropna(subset=['nim'], how='all')
        upsert_df(eff, 'fa_chart_efficiency', symbol, engine)
        upsert_metric('fa_chart_efficiency', symbol, METRICS['ngan_hang']['fa_chart_efficiency'], engine)

        # fa_chart_growth
        growth = ttm[keys].copy()
        growth['toi4Q']       = safe_get(ttm,'isb38').rolling(4).sum().values
        growth['netProfit4Q'] = safe_get(ttm,'isa22').rolling(4).sum().values
        growth = growth.dropna(subset=['toi4Q'], how='all')
        upsert_df(growth, 'fa_chart_growth', symbol, engine)
        upsert_metric('fa_chart_growth', symbol, METRICS['ngan_hang']['fa_chart_growth'], engine)

        # fa_chart_loan_quality, npl, loan_structure, deposit_structure
        if not df_note.empty:
            lq = df_note[keys].copy()
            lq['loanLossProvision']  = safe_get(df,'bsb105').abs()
            lq['subStandardDebt']    = safe_get(df_note,'nob42')
            lq['specialMentionDebt'] = safe_get(df_note,'nob41')
            lq['doubtfulDebt']       = safe_get(df_note,'nob43')
            lq['lossDebt']           = safe_get(df_note,'nob44')
            lq['vamcBonds']          = safe_get(df_note,'nob201')
            lq['standardDebtRatio']  = safe_get(df_note,'nob40') / safe_get(df_note,'nob39').replace(0, float('nan'))
            upsert_df(lq, 'fa_chart_loan_quality', symbol, engine)
            upsert_metric('fa_chart_loan_quality', symbol, METRICS['ngan_hang']['fa_chart_loan_quality'], engine)

            bad_debt = safe_get(df_note,'nob42') + safe_get(df_note,'nob43') + safe_get(df_note,'nob44')
            total    = safe_get(df_note,'nob39')
            npl = df_note[keys].copy()
            npl['npl']  = bad_debt / total.replace(0, float('nan'))
            npl['npl2'] = (total - safe_get(df_note,'nob40')) / total.replace(0, float('nan'))
            npl['llr']  = safe_get(df,'bsb105').abs() / bad_debt.replace(0, float('nan'))
            upsert_df(npl, 'fa_chart_npl', symbol, engine)
            upsert_metric('fa_chart_npl', symbol, METRICS['ngan_hang']['fa_chart_npl'], engine)

            ls = df_note[keys].copy()
            ls['longTermLoans']   = safe_get(df_note,'nob48')
            ls['mediumTermLoans'] = safe_get(df_note,'nob47')
            ls['shortTermLoans']  = safe_get(df_note,'nob46')
            ls['ldr']             = safe_get(df,'bsb103') / safe_get(df,'bsb113').replace(0, float('nan'))
            upsert_df(ls, 'fa_chart_loan_structure', symbol, engine)
            upsert_metric('fa_chart_loan_structure', symbol, METRICS['ngan_hang']['fa_chart_loan_structure'], engine)

            ds = df_note[keys].copy()
            ds['demandDeposit'] = safe_get(df_note,'nob66')
            ds['termDeposit']   = safe_get(df_note,'nob67')
            ds['savingDeposit'] = safe_get(df_note,'nob68')
            ds['otherDeposit']  = safe_get(df_note,'nob65') - safe_get(df_note,'nob66') - safe_get(df_note,'nob67') - safe_get(df_note,'nob68')
            ds['casaRatio']     = safe_get(df_note,'nob66') / safe_get(df_note,'nob65').replace(0, float('nan'))
            upsert_df(ds, 'fa_chart_deposit_structure', symbol, engine)
            upsert_metric('fa_chart_deposit_structure', symbol, METRICS['ngan_hang']['fa_chart_deposit_structure'], engine)

        log.info(f"✅ {symbol} (ngân hàng)")
    except Exception as e:
        log.error(f"❌ {symbol}: {e}")


# ─── CHỨNG KHOÁN ──────────────────────────────────────────────────────────────

def calc_fa_chung_khoan(symbol):
    try:
        bs    = read_bs(symbol, ['bsa2','bsa6','bsa8','bsa29','bsa40','bsa53','bsa57',
                                  'bsa78','bsa80','bsa84','bsa90','bsa96','bsa163',
                                  'bsb108','bss135','bss215','bss216','bss238','bss247'])
        is_   = read_is(symbol, ['isa1','isa20','isa22',
                                  'iss42','iss44','iss47','iss115','iss119','iss120','iss121','iss123','iss133'])
        note  = read_note(symbol, ['nos149','nos150','nos151','nos152','nos453',
                                    'nos461','nos462','nos463','nos465','nos467',
                                    'nos475','nos476','nos477','nos478','nos479'])
        # ohlcv = read_ohlcv(symbol)

        if bs.empty or is_.empty:
            return

        df = bs.merge(is_, on=['yearReport','lengthReport'], how='outer') \
               .sort_values(['yearReport','lengthReport']).reset_index(drop=True)

        df_note = bs.merge(note, on=['yearReport','lengthReport'], how='left') \
                    .sort_values(['yearReport','lengthReport']).reset_index(drop=True) if not note.empty else pd.DataFrame()

        keys = ['yearReport', 'lengthReport']

        # fa_chart_asset
        asset = df[keys].copy()
        asset['cash']                = safe_get(df,'bsa2')
        asset['fvtpl']               = safe_get(df,'bsa6')
        asset['htm']                 = safe_get(df,'bsb108')
        asset['afs']                 = safe_get(df,'bss216')
        asset['loans']               = safe_get(df,'bss215')
        asset['receivables']         = safe_get(df,'bsa8')
        asset['fixedAssets']         = safe_get(df,'bsa29') + safe_get(df,'bsa40') + safe_get(df,'bsa163')
        asset['otherAssets']         = safe_get(df,'bsa53') - safe_get(df,'bsa2') - safe_get(df,'bsa6') - safe_get(df,'bsb108') - safe_get(df,'bss216') - safe_get(df,'bss215') - safe_get(df,'bsa8') - (safe_get(df,'bsa29') + safe_get(df,'bsa40') + safe_get(df,'bsa163'))
        asset['proprietaryToAssets'] = (safe_get(df,'bsa6') + safe_get(df,'bss216') + safe_get(df,'bsb108')) / safe_get(df,'bsa53').replace(0, float('nan'))
        upsert_df(asset, 'fa_chart_asset', symbol, engine)
        upsert_metric('fa_chart_asset', symbol, METRICS['chung_khoan']['fa_chart_asset'], engine)

        # fa_chart_capital
        capital = df[keys].copy()
        capital['debt']             = safe_get(df,'bss238') + safe_get(df,'bss247')
        capital['tradePayables']    = safe_get(df,'bss135') + safe_get(df,'bsa57')
        capital['retainedEarnings'] = safe_get(df,'bsa90')
        capital['charterCapital']   = safe_get(df,'bsa80')
        capital['assetRevaluation'] = safe_get(df,'bsa84')
        capital['otherCapital']     = safe_get(df,'bsa96') - capital['debt'] - capital['tradePayables'] - safe_get(df,'bsa90') - safe_get(df,'bsa80') - safe_get(df,'bsa84')
        capital['debtToCapital']    = capital['debt'] / safe_get(df,'bsa96').replace(0, float('nan'))
        upsert_df(capital, 'fa_chart_capital', symbol, engine)
        upsert_metric('fa_chart_capital', symbol, METRICS['chung_khoan']['fa_chart_capital'], engine)

        # fa_chart_profit
        profit = df[keys].copy()
        profit['fvtplProfit']     = safe_get(df,'iss115')
        profit['htmProfit']       = safe_get(df,'iss119')
        profit['afsProfit']       = safe_get(df,'iss121')
        profit['brokerageProfit'] = safe_get(df,'iss42') + safe_get(df,'iss133')
        profit['loanProfit']      = safe_get(df,'iss120')
        profit['netProfit']       = safe_get(df,'isa22')
        profit['otherServices']   = safe_get(df,'iss123') + safe_get(df,'iss44') + safe_get(df,'iss47')
        upsert_df(profit, 'fa_chart_profit', symbol, engine)
        upsert_metric('fa_chart_profit', symbol, METRICS['chung_khoan']['fa_chart_profit'], engine)

        # fa_chart_brokerage
        brok = df[keys].copy()
        brok['brokerageProfit'] = safe_get(df,'iss42') + safe_get(df,'iss133')
        brok['brokerageMargin'] = (safe_get(df,'iss42') + safe_get(df,'iss133')) / safe_get(df,'iss42').replace(0, float('nan'))
        upsert_df(brok, 'fa_chart_brokerage', symbol, engine)
        upsert_metric('fa_chart_brokerage', symbol, METRICS['chung_khoan']['fa_chart_brokerage'], engine)

        # fa_chart_margin
        margin = df[keys].copy()
        margin['marginLoans']    = safe_get(df,'bss215')
        margin['equity']         = safe_get(df,'bsa78')
        margin['marginToEquity'] = safe_get(df,'bss215') / safe_get(df,'bsa78').replace(0, float('nan'))
        upsert_df(margin, 'fa_chart_margin', symbol, engine)
        upsert_metric('fa_chart_margin', symbol, METRICS['chung_khoan']['fa_chart_margin'], engine)

        # fa_chart_proprietary, fvtpl, afs, htm từ note
        if not df_note.empty:
            prop = df_note[keys].copy()
            prop['listedStocks']     = safe_get(df_note,'nos149') + safe_get(df_note,'nos461') + safe_get(df_note,'nos475')
            prop['unlistedStocks']   = safe_get(df_note,'nos150') + safe_get(df_note,'nos462') + safe_get(df_note,'nos476')
            prop['fundCertificates'] = safe_get(df_note,'nos151') + safe_get(df_note,'nos463') + safe_get(df_note,'nos477')
            prop['bonds']            = safe_get(df_note,'nos152') + safe_get(df_note,'nos465') + safe_get(df_note,'nos478')
            prop['deposits']         = safe_get(df_note,'nos453') + safe_get(df_note,'nos467') + safe_get(df_note,'nos479')
            upsert_df(prop, 'fa_chart_proprietary', symbol, engine)
            upsert_metric('fa_chart_proprietary', symbol, METRICS['chung_khoan']['fa_chart_proprietary'], engine)

            fvtpl = df_note[keys].copy()
            fvtpl['listedStocks']     = safe_get(df_note,'nos149')
            fvtpl['unlistedStocks']   = safe_get(df_note,'nos150')
            fvtpl['fundCertificates'] = safe_get(df_note,'nos151')
            fvtpl['bonds']            = safe_get(df_note,'nos152')
            fvtpl['deposits']         = safe_get(df_note,'nos453')
            upsert_df(fvtpl, 'fa_chart_fvtpl', symbol, engine)
            upsert_metric('fa_chart_fvtpl', symbol, METRICS['chung_khoan']['fa_chart_fvtpl'], engine)

            afs = df_note[keys].copy()
            afs['listedStocks']     = safe_get(df_note,'nos461')
            afs['unlistedStocks']   = safe_get(df_note,'nos462')
            afs['fundCertificates'] = safe_get(df_note,'nos463')
            afs['bonds']            = safe_get(df_note,'nos465')
            afs['deposits']         = safe_get(df_note,'nos467')
            upsert_df(afs, 'fa_chart_afs', symbol, engine)
            upsert_metric('fa_chart_afs', symbol, METRICS['chung_khoan']['fa_chart_afs'], engine)

            htm = df_note[keys].copy()
            htm['listedStocks']     = safe_get(df_note,'nos475')
            htm['unlistedStocks']   = safe_get(df_note,'nos476')
            htm['fundCertificates'] = safe_get(df_note,'nos477')
            htm['bonds']            = safe_get(df_note,'nos478')
            htm['deposits']         = safe_get(df_note,'nos479')
            upsert_df(htm, 'fa_chart_htm', symbol, engine)
            upsert_metric('fa_chart_htm', symbol, METRICS['chung_khoan']['fa_chart_htm'], engine)

        # TTM
        ttm   = df[df['lengthReport'].between(1,4)].copy()
        ln_4q = safe_get(ttm,'isa20').rolling(4).sum()
        avg_ta = safe_get(ttm,'bsa53').rolling(2).mean()
        avg_eq = safe_get(ttm,'bsa78').rolling(2).mean()

        # fa_chart_growth
        growth = ttm[keys].copy()
        growth['revenue4Q']   = safe_get(ttm,'isa1').rolling(4).sum().values
        growth['netProfit4Q'] = ln_4q.values
        growth = growth.dropna(subset=['revenue4Q'], how='all')
        upsert_df(growth, 'fa_chart_growth', symbol, engine)
        upsert_metric('fa_chart_growth', symbol, METRICS['chung_khoan']['fa_chart_growth'], engine)

        # fa_chart_efficiency
        eff = ttm[keys].copy()
        eff['roa'] = ln_4q / avg_ta.replace(0, float('nan'))
        eff['roe'] = ln_4q / avg_eq.replace(0, float('nan'))
        eff = eff.dropna(subset=['roa'], how='all')
        upsert_df(eff, 'fa_chart_efficiency', symbol, engine)
        upsert_metric('fa_chart_efficiency', symbol, METRICS['chung_khoan']['fa_chart_efficiency'], engine)

        log.info(f"✅ {symbol} (chứng khoán)")
    except Exception as e:
        log.error(f"❌ {symbol}: {e}")


# ─── BẢO HIỂM ─────────────────────────────────────────────────────────────────

def calc_fa_bao_hiem(symbol):
    try:
        bs    = read_bs(symbol, ['bsa2','bsa5','bsa8','bsa24','bsa29','bsa43','bsa53',
                                  'bsa54','bsa56','bsa71','bsa78','bsa80','bsa90','bsa96',
                                  'bsi192','bsi202','bsi203','bsi204','bsi205','bsi206','bsi207','bsi208'])
        is_   = read_is(symbol, ['isa6','isa16','isa20','isa22',
                                  'isi51','isi52','isi54','isi64','isi71','isi73','isi75',
                                  'isi77','isi94','isi97','isi100','isi105'])
        note  = read_note(symbol, ['noi6','noi7','noi68','noi69','noi71','noi301'])
        # ohlcv = read_ohlcv(symbol)

        if bs.empty or is_.empty:
            return

        df = bs.merge(is_, on=['yearReport','lengthReport'], how='outer') \
               .sort_values(['yearReport','lengthReport']).reset_index(drop=True)

        df_note = bs.merge(note, on=['yearReport','lengthReport'], how='left') \
                    .sort_values(['yearReport','lengthReport']).reset_index(drop=True) if not note.empty else pd.DataFrame()

        keys = ['yearReport', 'lengthReport']

        # fa_chart_asset
        asset = df[keys].copy()
        asset['cash']                 = safe_get(df,'bsa2')
        asset['shortTermInvestments'] = safe_get(df,'bsa5')
        asset['receivables']          = safe_get(df,'bsa8') + safe_get(df,'bsa24')
        asset['reinsuranceAssets']    = safe_get(df,'bsi192')
        asset['longTermInvestments']  = safe_get(df,'bsa43')
        asset['fixedAssets']          = safe_get(df,'bsa29')
        asset['otherAssets']          = safe_get(df,'bsa53') - safe_get(df,'bsa2') - safe_get(df,'bsa5') - safe_get(df,'bsa8') - safe_get(df,'bsa24') - safe_get(df,'bsi192') - safe_get(df,'bsa43') - safe_get(df,'bsa29')
        asset['investmentToAssets']   = (safe_get(df,'bsa5') + safe_get(df,'bsa43')) / safe_get(df,'bsa53').replace(0, float('nan'))
        asset['totalAssets']          = safe_get(df,'bsa53')
        upsert_df(asset, 'fa_chart_asset', symbol, engine)
        upsert_metric('fa_chart_asset', symbol, METRICS['bao_hiem']['fa_chart_asset'], engine)

        # fa_chart_capital
        capital = df[keys].copy()
        capital['insuranceReserves'] = safe_get(df,'bsi202')
        capital['debt']              = safe_get(df,'bsa56') + safe_get(df,'bsa71')
        capital['tradePayables']     = safe_get(df,'bsa54') - safe_get(df,'bsi202') - capital['debt']
        capital['charterCapital']    = safe_get(df,'bsa80')
        capital['retainedEarnings']  = safe_get(df,'bsa90')
        capital['otherCapital']      = safe_get(df,'bsa96') - safe_get(df,'bsi202') - capital['debt'] - capital['tradePayables'] - safe_get(df,'bsa80') - safe_get(df,'bsa90')
        capital['totalCapital']      = safe_get(df,'bsa96')
        upsert_df(capital, 'fa_chart_capital', symbol, engine)
        upsert_metric('fa_chart_capital', symbol, METRICS['bao_hiem']['fa_chart_capital'], engine)

        # fa_chart_profit
        profit = df[keys].copy()
        profit['underwritingProfit'] = safe_get(df,'isi97')
        profit['investmentProfit']   = safe_get(df,'isi100')
        profit['otherProfit']        = safe_get(df,'isa16') - safe_get(df,'isi97') - safe_get(df,'isi100')
        profit['netProfit']          = safe_get(df,'isa22')
        upsert_df(profit, 'fa_chart_profit', symbol, engine)
        upsert_metric('fa_chart_profit', symbol, METRICS['bao_hiem']['fa_chart_profit'], engine)

        # fa_chart_cost
        cost = df[keys].copy()
        cost['claimsExpense']       = safe_get(df,'isi71').abs()
        cost['claimsReserveChange'] = safe_get(df,'isi73').abs()
        cost['commissionExpense']   = safe_get(df,'isi77').abs()
        cost['otherBizExpense']     = (safe_get(df,'isi75') - safe_get(df,'isi77')).abs()
        cost['adminExpense']        = safe_get(df,'isi94').abs()
        upsert_df(cost, 'fa_chart_cost', symbol, engine)
        upsert_metric('fa_chart_cost', symbol, METRICS['bao_hiem']['fa_chart_cost'], engine)

        # fa_chart_reserve_structure
        rs = df[keys].copy()
        rs['upr']                = safe_get(df,'bsi203')
        rs['claimsReserve']      = safe_get(df,'bsi205')
        rs['catastropheReserve'] = safe_get(df,'bsi206')
        rs['mathReserve']        = safe_get(df,'bsi204')
        rs['bonusReserve']       = safe_get(df,'bsi207') + safe_get(df,'bsi208')
        rs['otherReserve']       = safe_get(df,'bsi202') - safe_get(df,'bsi203') - safe_get(df,'bsi205') - safe_get(df,'bsi206') - safe_get(df,'bsi204') - safe_get(df,'bsi207') - safe_get(df,'bsi208')
        upsert_df(rs, 'fa_chart_reserve_structure', symbol, engine)
        upsert_metric('fa_chart_reserve_structure', symbol, METRICS['bao_hiem']['fa_chart_reserve_structure'], engine)

        # fa_chart_revenue_structure
        rev = df[keys].copy()
        rev['grossPremium']       = safe_get(df,'isi51')
        rev['reinsurancePremium'] = safe_get(df,'isi52')
        rev['cedingPremium']      = safe_get(df,'isi54')
        rev['reserveChange']      = safe_get(df,'isi105') - safe_get(df,'isi51') - safe_get(df,'isi52') - safe_get(df,'isi54')
        rev['netPremium']         = safe_get(df,'isi105')
        rev['financialIncome']    = safe_get(df,'isa6')
        upsert_df(rev, 'fa_chart_revenue_structure', symbol, engine)
        upsert_metric('fa_chart_revenue_structure', symbol, METRICS['bao_hiem']['fa_chart_revenue_structure'], engine)

        # fa_chart_investment_portfolio
        if not df_note.empty:
            ip = df_note[keys].copy()
            ip['htm']            = safe_get(df_note,'noi301')
            ip['stInvestments']  = safe_get(df_note,'noi7')
            ip['ltDeposits']     = safe_get(df_note,'noi69')
            ip['ltBonds']        = safe_get(df_note,'noi71')
            ip['otherInvest']    = safe_get(df_note,'noi6') + safe_get(df_note,'noi68') - safe_get(df_note,'noi301') - safe_get(df_note,'noi7') - safe_get(df_note,'noi69') - safe_get(df_note,'noi71')
            ip['totalPortfolio'] = safe_get(df_note,'noi6') + safe_get(df_note,'noi68')
            upsert_df(ip, 'fa_chart_investment_portfolio', symbol, engine)
            upsert_metric('fa_chart_investment_portfolio', symbol, METRICS['bao_hiem']['fa_chart_investment_portfolio'], engine)

        # TTM
        ttm         = df[df['lengthReport'].between(1,4)].copy()
        claims_ttm  = safe_get(ttm,'isi71').abs().rolling(4).sum()
        premium_ttm = safe_get(ttm,'isi105').rolling(4).sum()
        expense_ttm = (safe_get(ttm,'isi75').abs() + safe_get(ttm,'isi94').abs()).rolling(4).sum()
        inv_profit  = safe_get(ttm,'isi100').rolling(4).sum()
        inv_assets  = (safe_get(df,'bsa5') + safe_get(df,'bsa43')).rolling(2).mean()
        ln_4q       = safe_get(ttm,'isa20').rolling(4).sum()
        ln_4q_mom   = safe_get(ttm,'isa22').rolling(4).sum()
        avg_ta      = safe_get(ttm,'bsa53').rolling(2).mean()
        avg_eq      = safe_get(ttm,'bsa78').rolling(2).mean()
        rev_total   = safe_get(ttm,'isi64') + safe_get(ttm,'isa6')

        # fa_chart_efficiency
        eff = ttm[keys].copy()
        eff['lossRatio']      = claims_ttm / premium_ttm.replace(0, float('nan'))
        eff['expenseRatio']   = expense_ttm / premium_ttm.replace(0, float('nan'))
        eff['combinedRatio']  = eff['lossRatio'] + eff['expenseRatio']
        eff['retentionRatio'] = premium_ttm / (safe_get(ttm,'isi51') + safe_get(ttm,'isi52')).rolling(4).sum().replace(0, float('nan'))
        eff['investmentYield']= inv_profit / inv_assets.replace(0, float('nan'))
        eff['roa']            = ln_4q / avg_ta.replace(0, float('nan'))
        eff['roe']            = ln_4q_mom / avg_eq.replace(0, float('nan'))
        eff['netMargin4Q']    = ln_4q / rev_total.rolling(4).sum().replace(0, float('nan'))
        eff = eff.dropna(subset=['lossRatio'], how='all')
        upsert_df(eff, 'fa_chart_efficiency', symbol, engine)
        upsert_metric('fa_chart_efficiency', symbol, METRICS['bao_hiem']['fa_chart_efficiency'], engine)

        # fa_chart_growth
        growth = ttm[keys].copy()
        growth['premiumRevenue4Q'] = premium_ttm.values
        growth['netProfit4Q']      = ln_4q.values
        growth = growth.dropna(subset=['premiumRevenue4Q'], how='all')
        upsert_df(growth, 'fa_chart_growth', symbol, engine)
        upsert_metric('fa_chart_growth', symbol, METRICS['bao_hiem']['fa_chart_growth'], engine)

        # fa_chart_leverage
        lev = ttm[keys].copy()
        lev['debtToAssets']     = safe_get(df,'bsa54') / safe_get(df,'bsa96').replace(0, float('nan'))
        lev['reservesToEquity'] = safe_get(df,'bsi202') / safe_get(df,'bsa78').replace(0, float('nan'))
        lev['roa']              = ln_4q / avg_ta.replace(0, float('nan'))
        lev = lev.dropna(subset=['debtToAssets'], how='all')
        upsert_df(lev, 'fa_chart_leverage', symbol, engine)
        upsert_metric('fa_chart_leverage', symbol, METRICS['bao_hiem']['fa_chart_leverage'], engine)

        log.info(f"✅ {symbol} (bảo hiểm)")
    except Exception as e:
        log.error(f"❌ {symbol}: {e}")


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def calc_fa(row):
    symbol = row['symbol']
    sector = row['sector']
    if sector == 'Banking':
        calc_fa_ngan_hang(symbol)
    elif sector == 'Securities':
        calc_fa_chung_khoan(symbol)
    elif sector == 'Insurance':
        calc_fa_bao_hiem(symbol)
    else:
        calc_fa_phi_tai_chinh(symbol)


def fa_chart_history():
    with engine.begin() as conn:
        for schema in [
            'fa_chart_asset', 'fa_chart_capital', 'fa_chart_profit',
            'fa_chart_cashflow', 'fa_chart_cost', 'fa_chart_growth',
            'fa_chart_efficiency', 'fa_chart_leverage',
            'fa_chart_loan_quality', 'fa_chart_npl',
            'fa_chart_loan_structure', 'fa_chart_deposit_structure',
            'fa_chart_brokerage', 'fa_chart_margin', 'fa_chart_proprietary',
            'fa_chart_fvtpl', 'fa_chart_afs', 'fa_chart_htm',
            'fa_chart_reserve_structure', 'fa_chart_revenue_structure',
            'fa_chart_investment_portfolio',
        ]:
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))

    symbols = pd.read_sql(text("""
        SELECT symbol, sector FROM info.asset
        WHERE type = 'Stock'
          AND exchange IN ('HOSE', 'HNX', 'UPCOM')
    """), engine).to_dict('records')

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        executor.map(calc_fa, symbols)

    print("Hoàn tất!")