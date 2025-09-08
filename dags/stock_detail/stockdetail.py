import requests
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from VNSFintech import stock_info
import concurrent.futures
import time
import pytz
# ==================== CONFIG ==================== #
BASE_URL = "https://restv2.fireant.vn"
HEADERS = {
    "accept": "application/json, text/plain, */*",
    "accept-encoding": "gzip, deflate, br, zstd",
    "accept-language": "vi,en-US;q=0.9,en;q=0.8",
    "authorization": "Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6IkdYdExONzViZlZQakdvNERWdjV4QkRITHpnSSIsImtpZCI6IkdYdExONzViZlZQakdvNERWdjV4QkRITHpnSSJ9.eyJpc3MiOiJodHRwczovL2FjY291bnRzLmZpcmVhbnQudm4iLCJhdWQiOiJodHRwczovL2FjY291bnRzLmZpcmVhbnQudm4vcmVzb3VyY2VzIiwiZXhwIjoxODg5NjIyNTMwLCJuYmYiOjE1ODk2MjI1MzAsImNsaWVudF9pZCI6ImZpcmVhbnQudHJhZGVzdGF0aW9uIiwic2NvcGUiOlsiYWNhZGVteS1yZWFkIiwiYWNhZGVteS13cml0ZSIsImFjY291bnRzLXJlYWQiLCJhY2NvdW50cy13cml0ZSIsImJsb2ctcmVhZCIsImNvbXBhbmllcy1yZWFkIiwiZmluYW5jZS1yZWFkIiwiaW5kaXZpZHVhbHMtcmVhZCIsImludmVzdG9wZWRpYS1yZWFkIiwib3JkZXJzLXJlYWQiLCJvcmRlcnMtd3JpdGUiLCJwb3N0cy1yZWFkIiwicG9zdHMtd3JpdGUiLCJzZWFyY2giLCJzeW1ib2xzLXJlYWQiLCJ1c2VyLWRhdGEtcmVhZCIsInVzZXItZGF0YS13cml0ZSIsInVzZXJzLXJlYWQiXSwianRpIjoiMjYxYTZhYWQ2MTQ5Njk1ZmJiYzcwODM5MjM0Njc1NWQifQ.dA5-HVzWv-BRfEiAd24uNBiBxASO-PAyWeWESovZm_hj4aXMAZA1-bWNZeXt88dqogo18AwpDQ-h6gefLPdZSFrG5umC1dVWaeYvUnGm62g4XS29fj6p01dhKNNqrsu5KrhnhdnKYVv9VdmbmqDfWR8wDgglk5cJFqalzq6dJWJInFQEPmUs9BW_Zs8tQDn-i5r4tYq2U8vCdqptXoM7YgPllXaPVDeccC9QNu2Xlp9WUvoROzoQXg25lFub1IYkTrM66gJ6t9fJRZToewCt495WNEOQFa_rwLCZ1QwzvL0iYkONHS_jZ0BOhBCdW9dWSawD6iF1SIQaFROvMDH1rg",
    "origin": "https://fireant.vn",
    "referer": "https://fireant.vn/",
    "user-agent": "Mozilla/5.0"
}

DB_URL = "postgresql+psycopg2://vnsfintech:%40Vns123456@videv.cloud:5432/vnsfintech"
engine = create_engine(DB_URL)

# load thông tin cổ phiếu từ VNSFintech 1 lần
stock_df = stock_info()
VN_TZ = pytz.timezone("Asia/Ho_Chi_Minh")

# ==================== DANH SÁCH SYMBOLS ==================== #
HOSE= [
    'AAA', 'AAM', 'AAT', 'ABR', 'ABS', 'ABT', 'ACB', 'ACC', 'ACG', 'ACL',
    'ADG', 'ADP', 'ADS', 'AGG', 'AGR', 'ANV', 'APG', 'APH', 'ASG',
    'ASM', 'ASP', 'AST', 'BAF', 'BBC', 'BCE', 'BCG', 'BCM', 'BFC', 'BHN',
    'BIC', 'BID', 'BKG', 'BMC', 'BMI', 'BMP', 'BRC', 'BSI', 'BSR', 'BTP',
    'BTT', 'BVH', 'BWE', 'C32', 'C47', 'CCC', 'CCI', 'CCL', 'CDC',
    'CIG', 'CII', 'CKG', 'CLC', 'CLL', 'CLW', 'CMG', 'CMV', 'CMX',
    'CNG', 'COM', 'CRC', 'CRE', 'CSM', 'CSV', 'CTD', 'CTF', 'CTG',
    'CTI', 'CTR', 'CTS', 'CVT', 'D2D', 'DAH', 'DAT', 'DBC', 'DBD',
    'DBT', 'DC4', 'DCL', 'DCM', 'DGC', 'DGW', 'DHA', 'DHC', 'DHG',
    'DHM', 'DIG', 'DLG', 'DMC', 'DPG', 'DPM', 'DPR', 'DQC', 'DRC',
    'DRH', 'DRL', 'DSC', 'DSE', 'DSN', 'DTA', 'DTL', 'DTT', 'DVP',
    'DXG', 'DXS', 'DXV', 'EIB', 'ELC', 'EVE', 'EVF', 'EVG', 'FCM',
    'FCN', 'FDC', 'FIR', 'FIT', 'FMC', 'FPT', 'FRT', 'FTS', 'GAS',
    'GDT', 'GEE', 'GEG', 'GEX', 'GIL', 'GMD', 'GMH', 'GSP', 'GTA',
    'GVR', 'HAG', 'HAH', 'HAP', 'HAR', 'HAS', 'HAX', 'HCD', 'HCM',
    'HDB', 'HDC', 'HDG', 'HHP', 'HHS', 'HHV', 'HID', 'HII', 'HMC',
    'HNA', 'HPG', 'HPX', 'HQC', 'HRC', 'HSG', 'HSL', 'HT1', 'HTG',
    'HTI', 'HTL', 'HTN', 'HTV', 'HU1', 'HUB', 'HVH', 'HVN', 'HVX',
    'ICT', 'IDI', 'IJC', 'ILB', 'IMP', 'ITC', 'ITD', 'JVC', 'KBC',
    'KDC', 'KDH', 'KHG', 'KHP', 'KMR', 'KOS', 'KPF', 'KSB', 'L10',
    'LAF', 'LBM', 'LCG', 'LDG', 'LGC', 'LGL', 'LHG', 'LIX', 'LM8',
    'LPB', 'LSS', 'MBB', 'MCM', 'MCP', 'MDG', 'MHC', 'MIG', 'MSB',
    'MSH', 'MSN', 'MWG', 'NAB', 'NAF', 'NAV', 'NBB', 'NCT', 'NHA',
    'NHH', 'NHT', 'NKG', 'NLG', 'NNC', 'NO1', 'NSC', 'NT2', 'NTL',
    'NVL', 'NVT', 'OCB', 'OGC', 'OPC', 'ORS', 'PAC', 'PAN', 'PC1',
    'PDN', 'PDR', 'PET', 'PGC', 'PGD', 'PGI', 'PGV', 'PHC', 'PHR',
    'PIT', 'PJT', 'PLP', 'PLX', 'PMG', 'PNC', 'PNJ', 'POW', 'PPC',
    'PSH', 'PTB', 'PTC', 'PTL', 'PVD', 'PVP', 'PVT', 'QCG', 'QNP',
    'RAL', 'REE', 'RYG', 'S4A', 'SAB', 'SAM', 'SAV', 'SBA', 'SBG',
    'SBT', 'SBV', 'SC5', 'SCR', 'SCS', 'SFC', 'SFG', 'SFI', 'SGN',
    'SGR', 'SGT', 'SHA', 'SHB', 'SHI', 'SHP', 'SIP', 'SJD', 'SJS',
    'SKG', 'SMA', 'SMB', 'SMC', 'SPM', 'SRC', 'SRF', 'SSB', 'SSC',
    'SSI', 'ST8', 'STB', 'STG', 'STK', 'SVC', 'SVD', 'SVI', 'SVT',
    'SZC', 'SZL', 'TBC', 'TCB', 'TCD', 'TCH', 'TCI', 'TCL', 'TCM',
    'TCO', 'TCR', 'TCT', 'TDC', 'TDG', 'TDH', 'TDM', 'TDP', 'TDW',
    'TEG', 'THG', 'TIP', 'TIX', 'TLD', 'TLG', 'TLH', 'TMP', 'TMS',
    'TMT', 'TN1', 'TNC', 'TNH', 'TNI', 'TNT', 'TPB', 'TPC', 'TRA',
    'TRC', 'TSC', 'TTA', 'TTE', 'TTF', 'TV2', 'TVB', 'TVS', 'TVT',
    'TYA', 'UIC', 'VAF', 'VCA', 'VCB', 'VCF', 'VCG', 'VCI', 'VDP',
    'VDS', 'VFG', 'VGC', 'VHC', 'VHM', 'VIB', 'VIC', 'VID', 'VIP',
    'VIX', 'VJC', 'VMD', 'VND', 'VNE', 'VNG', 'VNL', 'VNM', 'VNS',
    'VOS', 'VPB', 'VPD', 'VPG', 'VPH', 'VPI', 'VPS', 'VRC', 'VRE',
    'VSC', 'VSH', 'VSI', 'VTB', 'VTO', 'VTP', 'YBM', 'YEG', 'VPL','CHP'
]

CQ=[
    'CACB2404', 'CACB2405', 'CACB2501', 'CACB2502', 'CACB2503', 'CACB2504', 'CACB2505', 'CACB2506','CACB2507','CACB2508','CACB2509','CACB2510','CACB2511',
    'CFPT2402', 'CFPT2404', 'CFPT2405', 'CFPT2407', 'CFPT2501', 'CFPT2502', 'CFPT2503', 'CFPT2504',
    'CFPT2505', 'CFPT2506', 'CFPT2507', 'CFPT2508', 'CFPT2509', 'CFPT2510','CFPT2511','CFPT2512','CFPT2513','CFPT2514','CFPT2515','CFPT2516','CFPT2517','CFPT2518','CFPT2519','CFPT2520','CFPT2521',
    'CHDB2501', 'CHDB2502', 'CHDB2503', 'CHDB2504', 'CHDB2505',
    'CHPG2402', 'CHPG2406', 'CHPG2408', 'CHPG2409', 'CHPG2410', 'CHPG2412',
    'CHPG2501', 'CHPG2502', 'CHPG2503', 'CHPG2504', 'CHPG2505', 'CHPG2506',
    'CHPG2507', 'CHPG2508', 'CHPG2509', 'CHPG2510', 'CHPG2511', 'CHPG2512',
    'CHPG2513', 'CHPG2514', 'CHPG2515', 'CHPG2516',"CHPG2517", "CHPG2518", "CHPG2519", "CHPG2520", "CHPG2521", "CHPG2522",
    "CHPG2523", "CHPG2524", "CHPG2525", "CHPG2526", "CHPG2527", "CHPG2528", "CHPG2529", "CHPG2530", "CHPG2531", "CHPG2532",
    "CLPB2501",	"CLPB2502",	"CLPB2503",	"CLPB2504",	"CLPB2505",	"CLPB2506",	"CLPB2507",	"CLPB2508",
    'CMBB2402', 'CMBB2405', 'CMBB2407', 'CMBB2409',
    'CMBB2501', 'CMBB2502', 'CMBB2503', 'CMBB2504', 'CMBB2505', 'CMBB2506', 'CMBB2507', 'CMBB2508', "CMBB2509", "CMBB2510",	"CMBB2511",	"CMBB2512",	"CMBB2513",	"CMBB2514",	"CMBB2515",	"CMBB2516",	"CMBB2517", "CMBB2518",
    'CMSN2404', 'CMSN2406', 'CMSN2408',
    'CMSN2501', 'CMSN2502', 'CMSN2503', 'CMSN2504', 'CMSN2505', 'CMSN2506', 'CMSN2507', 'CMSN2508', 'CMSN2509', 'CMSN2510',
    "CMSN2511",	"CMSN2512",	"CMSN2513",	"CMSN2514",	"CMSN2515",	"CMSN2516",	"CMSN2517",	"CMSN2518",
    'CMWG2401', 'CMWG2406', 'CMWG2407', 'CMWG2408', 'CMWG2410',
    'CMWG2501', 'CMWG2502', 'CMWG2503', 'CMWG2504', 'CMWG2505', 'CMWG2506', 'CMWG2507', 'CMWG2508', 
    "CMWG2509",	"CMWG2510",	"CMWG2511",	"CMWG2512",	"CMWG2513",	"CMWG2514",	"CMWG2515",	"CMWG2516",	"CMWG2517",	"CMWG2518",
    'CSHB2401', 'CSHB2403',
    'CSHB2501', 'CSHB2502', 'CSHB2503', 'CSHB2504', 'CSHB2505', "CSHB2506",	"CSHB2507",	"CSHB2508",	"CSHB2509",	"CSHB2510",
    'CSSB2501', 'CSSB2502', 'CSSB2503', 'CSSB2504', "CSSB2505",	"CSSB2506",	"CSSB2507",	"CSSB2508",
    'CSTB2402', 'CSTB2409', 'CSTB2410', 'CSTB2411', 'CSTB2413',
    'CSTB2501', 'CSTB2502', 'CSTB2503', 'CSTB2504', 'CSTB2505', 'CSTB2506', 'CSTB2507', 'CSTB2508', 'CSTB2509', 'CSTB2510', 'CSTB2511', 'CSTB2512', "CSTB2513",	"CSTB2514",	"CSTB2515",	"CSTB2516",	"CSTB2517",	"CSTB2518",	"CSTB2519",	"CSTB2520",	"CSTB2521",	"CSTB2522",	"CSTB2523",	"CSTB2524",	"CSTB2525", 
    'CTCB2403', 'CTCB2404', 'CTCB2406',
    'CTCB2501', 'CTCB2502', 'CTCB2503', 'CTCB2504', 'CTCB2505', 'CTCB2506', "CTCB2507",	"CTCB2508",	"CTCB2509",	"CTCB2510",	"CTCB2511",	"CTCB2512",	"CTCB2513",
    "CTG125001",	"CTG125002",	"CTG125013",	"CTG125014",
    'CTPB2403', 'CTPB2405',
    'CTPB2501', "CTPB2502",	"CTPB2503",	"CTPB2504",	"CTPB2505",	"CTPB2506",
    'CVHM2406', 'CVHM2408', 'CVHM2409', 'CVHM2411',
    'CVHM2501', 'CVHM2502', 'CVHM2503', 'CVHM2504', 'CVHM2505', 'CVHM2506', 'CVHM2507', 'CVHM2508', 'CVHM2509', "CVHM2510",	"CVHM2511",	"CVHM2512",	"CVHM2513",	"CVHM2514",	"CVHM2515",	"CVHM2516",	"CVHM2517",	"CVHM2518",	"CVHM2519",	"CVHM2520",
    'CVIB2402', 'CVIB2406', 'CVIB2407', 'CVIB2408', 
    'CVIB2501', 'CVIB2502', 'CVIB2503', "CVIB2504",	"CVIB2505",	"CVIB2506",	"CVIB2507",	"CVIB2508",	"CVIB2509",	"CVIB2510",
    'CVIC2405', 'CVIC2407',
    'CVIC2501', 'CVIC2502', 'CVIC2503', 'CVIC2504', 'CVIC2505', 'CVIC2506', 'CVIC2507', 'CVIC2508', "CVIC2509",	"CVIC2510",	"CVIC2511",
    'CVJC2501', 'CVJC2502', 'CVJC2503', 'CVJC2504', 'CVJC2505',
    'CVNM2401', 'CVNM2406', 'CVNM2407',
    'CVNM2501', 'CVNM2502', 'CVNM2503', 'CVNM2504', 'CVNM2505', 'CVNM2506', 'CVNM2507', 'CVNM2508', 'CVNM2509', "CVNM2510",	"CVNM2511",	"CVNM2512",	"CVNM2513",	"CVNM2514",	"CVNM2515",	"CVNM2516",	"CVNM2517",	"CVNM2518",	"CVNM2519",	"CVNM2520",
    'CVPB2401', 'CVPB2407', 'CVPB2409', 'CVPB2410', 'CVPB2412',
    'CVPB2501', 'CVPB2502', 'CVPB2503', 'CVPB2504', 'CVPB2505', 'CVPB2506', 'CVPB2507', 'CVPB2508', 'CVPB2509', 'CVPB2510', "CVPB2511",	"CVPB2512",	"CVPB2513",	"CVPB2514",	"CVPB2515",	"CVPB2516",	"CVPB2517",	"CVPB2518",	"CVPB2519",	"CVPB2520",	"CVPB2521",	"CVPB2522",
    'CVRE2406', 'CVRE2407', 'CVRE2408', 'CVRE2410',
    'CVRE2501', 'CVRE2502', 'CVRE2503', 'CVRE2504', 'CVRE2505', 'CVRE2506', 'CVRE2507', 'CVRE2508', 'CVRE2509', 'CVRE2510', "CVRE2511",	"CVRE2512",	"CVRE2513",	"CVRE2514",	"CVRE2515",	"CVRE2516",	"CVRE2517",	"CVRE2518",	"CVRE2519",	"CVRE2520",	"CVRE2521",
    'FUCTVGF3',	'FUCTVGF4',	'FUCTVGF5',	'FUCVREIT'
]

HNX= [
    'AAV','ADC','ALT','AMC','AME','AMV','API','APS','ARM','ATS','BAB','BAX','BBS','BCC','BCF','BDB','BED',
    'BKC','BNA','BPC','BSC','BST','BTS','BTW','BVS','BXH','C69','CAG','CAN','CAP','CAR','CCR','CDN','CEO',
    'CET','CIA','CJC','CKV','CLH','CLM','CMC','CMS','CPC','CSC','CST','CTB','CTP','CTT','CVN','CX8','D11',
    'DAD','DAE','DC2','DDG','DHP','DHT','DIH','DL1','DNC','DNP','DP3','DS3','DST','DTD','DTG','DTK','DVM',
    'DXP','EBS','ECI','EID','EVS','FID','GDW','GIC','GKM','GLT','GMA','GMX','HAD','HAT','HBS','HCC','HCT',
    'HDA','HEV','HGM','HHC','HJS','HKT','HLC','HLD','HMH','HMR','HOM','HTC','HUT','HVT','ICG','IDC','IDJ',
    'IDV','INC','INN','IPA','ITQ','IVS','KDM','KHS','KKC','KMT','KSD','KSF','KSQ','KST','KSV','KTS','L14',
    'L18','L40','LAS','LBE','LCD','LDP','LHC','LIG','MAC','MAS','MBG','MBS','MCC','MCF','MCO','MDC','MED',
    'MEL','MIC','MKV','MST','MVB','NAG','NAP','NBC','NBP','NBW','NDN','NDX','NET','NFC','NHC','NRC','NSH',
    'NST','NTH','NTP','NVB','OCH','ONE','PBP','PCE','PCG','PCH','PCT','PDB','PEN','PGN','PGS','PGT','PHN',
    'PIA','PIC','PJC','PLC','PMB','PMC','PMP','PMS','POT','PPE','PPP','PPS','PPT','PPY','PRC','PRE','PSC',
    'PSD','PSE','PSI','PSW','PTD','PTI','PTS','PTX','PV2','PVB','PVC','PVG','PVI','PVS','QHD','QST','QTC',
    'RCL','S55','S99','SAF','SCG','SCI','SD5','SD9','SDA','SDC','SDG','SDN','SDU','SEB','SED','SFN','SGC',
    'SGD','SGH','SHE','SHN','SHS','SJ1','SJE','SLS','SMN','SMT','SPC','SPI','SRA','SSM','STC','STP','SVN',
    'SZB','TA9','TBX','TDT','TET','TFC','THB','THD','THS','THT','TIG','TJC','TKU','TMB','TMC','TMX','TNG',
    'TOT','TPH','TPP','TSB','TTC','TTH','TTL','TTT','TV3','TV4','TVC','TVD','TXM','UNI','V12','V21','VBC',
    'VC1','VC2','VC3','VC6','VC7','VC9','VCC','VCM','VCS','VDL','VE1','VE3','VE4','VE8','VFS','VGP','VGS',
    'VHE','VHL','VIF','VIG','VIT','VLA','VMC','VMS','VNC','VNF','VNR','VNT','VSA','VSM','VTC','VTH','VTJ',
    'VTV','VTZ','WCS','WSS','X20','TD6'
]

HNXBOND= [
    'BAB122032','BAB123007','BAB123031','BAB123032','BAB124014','BAB124015','BAB124016','BAB124024','BAB124025','BAB124026',
    'BAF122029','BAF123020','BCG122006','BID122005','BID123003','BID123004','BVB124020','CII124021','CII42013','CII424002',
    'CMX123035','CTG121031','CTG123018','CTG123019','CTG123033','CTG123034','CVT122009','DSE125004','HDB124006','HDB124018',
    'HDB124023','LPB121036','LPB122011','LPB122013','LPB123008','LPB123009','LPB123015','LPB123016','MBB124017','MBB124022',
    'MML121021','MSN123008','MSN123009','MSN123010','MSN123014','NPM123021','NPM123022','NPM123023','NPM123024','NVL122001',
    'TCX124011','TCX124012','TCX124013','TDP124010','TNG122017','VBA121033','VBA122001','VBA123036','VBA124019','VBB123017',
    'VBB124007','VHM121025','VIC123028','VIC123029','VIC124003','VIC124004','VIC124005','VPI124001','VRE12007',
    'BVB125003','HDB125011','HDB125012', 'KLB124009','KLB125015','LPB125006','LPB125007','MBB125008','SHB125010',
    'TNG124027','VCK125005'
]

UPCOM= [
    'A32', 'AAH', 'AAS', 'ABB', 'ABC', 'ABI', 'ABW', 'ACE', 'ACM', 'ACS','ACV', 'AFX', 'AG1', 'AGF', 'AGP', 'AGX', 'AIC', 'AIG', 'ALV', 'AMD',
    'AMP', 'AMS', 'ANT', 'APC', 'APF', 'APL', 'APP', 'APT', 'ART', 'ASA','ATA', 'ATB', 'ATG', 'AVC', 'AVF', 'AVG', 'BAL', 'BBH', 'BBM', 'BBT',
    'BCA', 'BCB', 'BCP', 'BCR', 'BCV', 'BDG', 'BDT', 'BDW', 'BEL', 'BGE','BGW', 'BHA', 'BHC', 'BHG', 'BHH', 'BHI', 'BHK', 'BHP', 'BIG', 'BII',
    'BIO', 'BLF', 'BLI', 'BLN', 'BLT', 'BMD', 'BMF', 'BMG', 'BMJ', 'BMK','BMN', 'BMS', 'BMV', 'BNW', 'BOT', 'BQB', 'BRR', 'BRS', 'BSA', 'BSD',
    'BSG', 'BSH', 'BSL', 'BSP', 'BSQ', 'BT1', 'BT6', 'BTB', 'BTD', 'BTG','BTH', 'BTN', 'BTU', 'BTV', 'BVB', 'BVG', 'BVL', 'BVN', 'BWA', 'BWS',
    'C12', 'C21', 'C22', 'C4G', 'C92', 'CAD', 'CAT', 'CBI', 'CBS', 'CC1','CC4', 'CCA', 'CCM', 'CCP', 'CCT', 'CCV', 'CDG', 'CDH', 'CDO', 'CDP',
    'CDR', 'CEN', 'CFM', 'CFV', 'CGV', 'CH5', 'CHC', 'CHS', 'CI5', 'CID','CIP', 'CK8', 'CKA', 'CKD', 'CLG', 'CLX', 'CMD', 'CMF', 'CMI', 'CMK',
    'CMM', 'CMN', 'CMP', 'CMT', 'CMW', 'CNA', 'CNC', 'CNN', 'CNT', 'CPA','CPH', 'CPI', 'CQN', 'CQT', 'CSI', 'CT3', 'CT6', 'CTA', 'CTN', 'CTW',
    'CTX', 'CYC', 'DAC', 'DAG', 'DAN', 'DAS', 'DBM', 'DC1', 'DCF', 'DCG','DCH', 'DCR', 'DCS', 'DCT', 'DDB', 'DDH', 'DDM', 'DDN', 'DDV', 'DFC',
    'DFF', 'DGT', 'DHB', 'DHD', 'DHN', 'DIC', 'DID', 'DKC', 'DKG', 'DKW','DLD', 'DLR', 'DLT', 'DM7', 'DMN', 'DMS', 'DNA', 'DND', 'DNE', 'DNH',
    'DNL', 'DNM', 'DNN', 'DNT', 'DNW', 'DOC', 'DOP', 'DP1', 'DP2', 'DPC','DPH', 'DPP', 'DRG', 'DRI', 'DSD', 'DSG', 'DSH', 'DSP', 'DTB',
    'DTC', 'DTE', 'DTH', 'DTI', 'DTP', 'DUS', 'DVC', 'DVG', 'DVN', 'DVT','DVW', 'DWC', 'DWS', 'DXL', 'DZM', 'E12', 'E29', 'ECO', 'EFI', 'EIC',
    'EIN', 'EME', 'EMG', 'EMS', 'EPC', 'EPH', 'F88', 'FBA', 'FBC', 'FCC','FCS', 'FGL', 'FHN', 'FHS', 'FIC', 'FLC', 'FOC', 'FOX', 'FRC', 'FRM',
    'FSO', 'FT1', 'FTI', 'FTM', 'G20', 'G36', 'GAB', 'GCB', 'GCF', 'GDA','GER', 'GGG', 'GH3', 'GHC', 'GLC', 'GLW', 'GMC', 'GND', 'GPC', 'GSM',
    'GTD', 'GTS', 'GTT', 'GVT', 'H11', 'HAC', 'HAF', 'HAI', 'HAM', 'HAN','HAV', 'HBC', 'HBD', 'HBH', 'HC1', 'HC3', 'HCB', 'HCI', 'HD2', 'HD6',
    'HD8', 'HDM', 'HDO', 'HDP', 'HDS', 'HDW', 'HEC', 'HEJ', 'HEP', 'HES','HFB', 'HFC', 'HFX', 'HGT', 'HHG', 'HHN', 'HIG', 'HIO', 'HJC', 'HKB',
    'HLA', 'HLB', 'HLO', 'HLS', 'HLT', 'HLY', 'HMD', 'HMG', 'HMS', 'HNB','HND', 'HNF', 'HNG', 'HNI', 'HNM', 'HNP', 'HNR', 'HOT', 'HPB', 'HPD',
    'HPH', 'HPI', 'HPM', 'HPP', 'HPT', 'HPW', 'HRB', 'HSA', 'HSI', 'HSM','HSP', 'HSV', 'HTE', 'HTM', 'HTP', 'HTT', 'HU3', 'HU4', 'HU6', 'HUG',
    'HVA', 'HVG', 'HWS', 'IBC', 'IBD', 'ICC', 'ICF', 'ICI', 'ICN', 'IDP','IFS', 'IHK', 'ILA', 'ILC', 'ILS', 'IME', 'IN4', 'ING', 'IRC', 'ISG',
    'ISH', 'IST', 'ITA', 'ITS', 'JOS', 'KAC', 'KCB', 'KCE', 'KGM', 'KHD','KHL', 'KHW', 'KIP', 'KLB', 'KLF', 'KSH', 'KTC', 'KTL', 'KTT', 'KVC',
    'KWA', 'L12', 'L35', 'L43', 'L44', 'L45', 'L61', 'L62', 'L63', 'LAI','LAW', 'LCC', 'LCM', 'LCS', 'LDW', 'LEC', 'LG9', 'LGM', 'LIC', 'LKW',
    'LLM', 'LM3', 'LM7', 'LMC', 'LMH', 'LMI', 'LNC', 'LO5', 'LPT', 'LQN','LSG', 'LTC', 'LTG', 'LUT', 'M10', 'MA1', 'MBN', 'MBT', 'MCG', 'MCH',
    'MDA', 'MDF', 'MEC', 'MEF', 'MES', 'MFS', 'MGC', 'MGG', 'MGR', 'MH3','MHL', 'MIE', 'MIM', 'MKP', 'MLC', 'MLS', 'MML', 'MNB', 'MND', 'MPC',
    'MPT', 'MPY', 'MQB', 'MQN', 'MRF', 'MSR', 'MTA', 'MTB', 'MTC', 'MTG','MTH', 'MTL', 'MTP', 'MTS', 'MTV', 'MTX', 'MVC', 'MVN', 'MZG', 'NAC',
    'NAS', 'NAU', 'NAW', 'NBE', 'NBT', 'NCG', 'NCS', 'ND2', 'NDC', 'NDF','NDP', 'NDT', 'NDW', 'NED', 'NEM', 'NGC', 'NHP', 'NHV', 'NJC', 'NLS',
    'NNT', 'NOS', 'NQB', 'NQN', 'NQT', 'NS2', 'NSG', 'NSL', 'NSS', 'NTB','NTC', 'NTF', 'NTT', 'NTW', 'NUE', 'NVP', 'NWT', 'NXT', 'ODE', 'OIL',
    'ONW', 'PAI', 'PAP', 'PAS', 'PAT', 'PBC', 'PBT', 'PCC', 'PCF', 'PCM','PDC', 'PDV', 'PEC', 'PEG', 'PEQ', 'PFL', 'PGB', 'PHH', 'PHP', 'PHS',
    'PID', 'PIS', 'PIV', 'PJS', 'PLA', 'PLE', 'PLO', 'PMJ', 'PMT', 'PMW','PND', 'PNG', 'PNP', 'PNT', 'POB', 'POM', 'POS', 'POV', 'PPH', 'PPI',
    'PQN', 'PRO', 'PRT', 'PSB', 'PSG', 'PSL', 'PSN', 'PSP', 'PTE', 'PTG','PTH', 'PTO', 'PTP', 'PTT', 'PTV', 'PVA', 'PVE', 'PVH', 'PVL', 'PVM',
    'PVO', 'PVR', 'PVV', 'PVX', 'PVY', 'PWA', 'PWS', 'PX1', 'PXA', 'PXC','PXI', 'PXL', 'PXM', 'PXS', 'PXT', 'QBS', 'QCC', 'QHW', 'QNC', 'QNS',
    'QNT', 'QNU', 'QNW', 'QPH', 'QSP', 'QTP', 'RAT', 'RBC', 'RCC', 'RCD','RDP', 'RIC', 'RTB', 'S12', 'S27', 'S72', 'S74', 'S96', 'SAC', 'SAL',
    'SAP', 'SAS', 'SB1', 'SBB', 'SBD', 'SBH', 'SBL', 'SBM', 'SBR', 'SBS','SCC', 'SCD', 'SCJ', 'SCL', 'SCO', 'SCY', 'SD1', 'SD2', 'SD3', 'SD4',
    'SD6', 'SD7', 'SD8', 'SDB', 'SDD', 'SDJ', 'SDK', 'SDP', 'SDT', 'SDV','SDX', 'SDY', 'SEA', 'SEP', 'SGB', 'SGI', 'SGP', 'SGS', 'SHC', 'SHG',
    'SID', 'SIG', 'SII', 'SIV', 'SJC', 'SJF', 'SJG', 'SJM', 'SKH', 'SKN','SKV', 'SLD', 'SNC', 'SNZ', 'SP2', 'SPB', 'SPD', 'SPH', 'SPV', 'SQC',
    'SRB', 'SSF', 'SSG', 'SSH', 'SSN', 'STH', 'STL', 'STS', 'STT', 'STW','SVG', 'SVH', 'SWC', 'SZE', 'SZG', 'TA6', 'TAB', 'TAL', 'TAN', 'TAR',
    'TAW', 'TB8', 'TBD', 'TBH', 'TBR', 'TBT', 'TBW', 'TCJ', 'TCK', 'TCW','TDB', 'TDF', 'TDS', 'TED', 'TEL', 'TGG', 'TGP', 'TH1', 'THM', 'THN',
    'THP', 'THU', 'THW', 'TID', 'TIE', 'TIN', 'TIS', 'TKA', 'TKC', 'TKG','TL4', 'TLI', 'TLP', 'TLT', 'TMG', 'TMW', 'TNA', 'TNB', 'TNM', 'TNP',
    'TNS', 'TNV', 'TNW', 'TOP', 'TOS', 'TOW', 'TPS', 'TQN', 'TQW', 'TR1','TRS', 'TRT', 'TRV', 'TS3', 'TS4', 'TSA', 'TSD', 'TSG', 'TSJ', 'TST',
    'TT6', 'TTB', 'TTD', 'TTG', 'TTN', 'TTS', 'TTZ', 'TUG', 'TV1', 'TV6','TVA', 'TVG', 'TVH', 'TVM', 'TVN', 'TW3', 'UCT', 'UDC', 'UDJ', 'UDL',
    'UEM', 'UMC', 'UPC', 'UPH', 'USC', 'USD', 'UXC', 'V11', 'V15', 'VAB','VAV', 'VBB', 'VBG', 'VBH', 'VC5', 'VCE', 'VCP', 'VCR', 'VCT', 'VCW',
    'VCX', 'VDB', 'VDG', 'VDN', 'VDT', 'VE2', 'VE9', 'VEA', 'VEC', 'VEF','VES', 'VET', 'VFC', 'VFR', 'VGG', 'VGI', 'VGL', 'VGR', 'VGT', 'VGV',
    'VHD', 'VHF', 'VHG', 'VHH', 'VIE', 'VIH', 'VIM', 'VIN', 'VIR', 'VIW','VKC', 'VKP', 'VLB', 'VLC', 'VLF', 'VLG', 'VLP', 'VLW', 'VMA', 'VMG',
    'VMK', 'VMT', 'VNA', 'VNB', 'VNH', 'VNI', 'VNP', 'VNX', 'VNY', 'VNZ','VOC', 'VPA', 'VPC', 'VPR', 'VPW', 'VQC', 'VRG', 'VSE', 'VSF', 'VSG',
    'VSN', 'VST', 'VTA', 'VTD', 'VTE', 'VTG', 'VTI', 'VTK', 'VTL', 'VTM','VTQ', 'VTR', 'VTS', 'VTX', 'VUA', 'VVN', 'VVS', 'VW3', 'VWS', 'VXB',
    'VXP', 'VXT', 'WSB', 'WTC', 'X26', 'X77', 'XDH', 'XHC', 'XLV', 'XMC','XMD', 'XMP', 'XPH', 'YBC', 'YTC','PTM', 'AGM'
]

DERIVATIVES =[
    '41I1F7000', '41I1F8000', '41I1G3000', 'GB05F2506', 'GB05F2509', 'GB05F2512', 'GB10F2506', 'GB10F2509', 'GB10F2512',
    'VN30F2505', 'VN30F2506', 'VN30F2509', 'VN30F2512', '41B5G3000', '41BAG3000', '41I1FA000', 'VN30F1M', 'VN30F1Q', 'VN30F2M', 'VN30F2Q'
]

ETFHOSE =[
    "E1VFVN30",'FUEABVND','FUEBFVND','FUEDCMID','FUEFCV50','FUEIP100','FUEKIV30','FUEKIVFS','FUEKIVND','FUEMAV30','FUEMAVND','FUESSV30','FUESSV50','FUESSVFL','FUETCC50','FUEVFVND','FUEVN100',
]

# gộp lại tất cả symbols
all_symbols = HOSE + CQ + HNX + HNXBOND + UPCOM + DERIVATIVES + ETFHOSE

# ==================== HELPERS ==================== #
def detect_exchange(symbol: str) -> str:
    """Xác định sàn/nhóm theo danh sách mã hiện có, giữ nguyên cấu trúc code cũ."""
    if symbol in HOSE: return "HOSE"
    if symbol in CQ: return "CQ"
    if symbol in HNX: return "HNX"
    if symbol in HNXBOND: return "HNXBOND"
    if symbol in UPCOM: return "UPCOM"
    if symbol in DERIVATIVES: return "DERIVATIVES"
    if symbol in ETFHOSE: return "ETFHOSE"
    return None

def get_company_info(symbol):
    row = stock_df.loc[stock_df["Mã CP"] == symbol]
    if not row.empty:
        return {
            "name": row.iloc[0].get("Tên công ty (VI)"),
            "industry": row.iloc[0].get("Ngành (VI)")
        }
    return None

def get_first(d: dict, candidates, default=None):
    """Return first existing non-None value among candidate keys in dict d."""
    if not isinstance(d, dict):
        return default
    for k in candidates:
        if k in d and d[k] is not None:
            return d[k]
    return default

def get_latest_from_historical(h_data):
    """Handle both list and dict-with-data responses and return the latest valid entry or {}.
       Prefer the most recent record if API returns a list ordered newest-first or oldest-first.
       We'll try common shapes: list, {'data':[...]} or {'items':[...]}.
    """
    if isinstance(h_data, dict):
        # flatten structures like {'data': [...] } or {'data': {'items': [...]}}
        if "data" in h_data:
            data_part = h_data["data"]
            if isinstance(data_part, list) and data_part:
                return data_part[0]
            if isinstance(data_part, dict):
                # try items inside data
                if "items" in data_part and isinstance(data_part["items"], list) and data_part["items"]:
                    return data_part["items"][0]
        if "items" in h_data and isinstance(h_data["items"], list) and h_data["items"]:
            return h_data["items"][0]
        return {}
    if isinstance(h_data, list) and h_data:
        return h_data[0]
    return {}

def trading_date_range(days_back=7):
    """Return (startDate, endDate) in VN timezone: endDate = today_vn, startDate = today_vn - days_back"""
    now_vn = datetime.now(VN_TZ)
    end = now_vn.date()
    start = end - timedelta(days=days_back)
    return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")

# ==================== FUNCTIONS ==================== #
def get_data(symbol, retries=3, delay=1):
    exchange = detect_exchange(symbol)
    for attempt in range(retries):
        try:
            # fundamental endpoint
            f_url = f"{BASE_URL}/symbols/{symbol}/fundamental"
            f_resp = requests.get(f_url, headers=HEADERS, timeout=10)
            try:
                f_data = f_resp.json()
                # flatten if wrapped in 'data'
                if isinstance(f_data, dict) and "data" in f_data and isinstance(f_data["data"], dict):
                    f_data = f_data["data"]
            except Exception:
                f_data = {}

            # Lấy historical trong vài ngày gần nhất (tránh trường hợp 'today' rỗng do timezone)
            start_date, end_date = trading_date_range(days_back=7)
            h_url = f"{BASE_URL}/symbols/{symbol}/historical-quotes"
            h_params = {"startDate": start_date, "endDate": end_date, "offset": 0, "limit": 30}
            h_resp = requests.get(h_url, headers=HEADERS, params=h_params, timeout=10)
            try:
                h_data = h_resp.json()
            except Exception:
                h_data = []

            latest = get_latest_from_historical(h_data)

            # nếu latest rỗng -> thử lùi thêm 7 ngày (nếu API trả rỗng range này)
            if not latest:
                start_date2, end_date2 = trading_date_range(days_back=30)
                h_params = {"startDate": start_date2, "endDate": end_date2, "offset": 0, "limit": 50}
                try:
                    h_data2 = requests.get(h_url, headers=HEADERS, params=h_params, timeout=10).json()
                    latest = get_latest_from_historical(h_data2)
                except Exception:
                    latest = {}

            # Nếu vẫn rỗng, log và return None (hoặc bạn giữ lại nhưng sẽ có nhiều null)
            if not latest:
                # bạn có thể ghi log vào file nếu cần
                print(f"[WARN] Không có historical data cho {symbol} trong khoảng gần đây.")
                # fallback: tạo object rỗng để vẫn lưu company info + fundamental
                latest = {}

            # dò nhiều tên trường khác nhau
            ref_price = get_first(latest, ["priceBasic", "referencePrice", "refPrice", "priceRef"])
            price_close = get_first(latest, ["priceClose", "close", "price", "lastPrice"], 0)
            open_price = get_first(latest, ["priceOpen", "open"])
            high_price = get_first(latest, ["priceHigh", "high"])
            low_price = get_first(latest, ["priceLow", "low"])

            foreignBuy = get_first(latest, ["buyForeignQuantity", "foreignBuyQuantity", "buyForeign"], 0)
            foreignSell = get_first(latest, ["sellForeignQuantity", "foreignSellQuantity", "sellForeign"], 0)
            foreignBuyValue = get_first(latest, ["buyForeignValue", "foreignBuyValue", "buyForeignVal"], 0)
            foreignSellValue = get_first(latest, ["sellForeignValue", "foreignSellValue", "sellForeignVal"], 0)

            foreignNetVolume = (foreignBuy or 0) - (foreignSell or 0)
            foreignNetValue = (foreignBuyValue or 0) - (foreignSellValue or 0)

            company_info = get_company_info(symbol) or {"name": None, "industry": None}

            # tính ratioChange an toàn
            ratio_change = None
            try:
                if ref_price and price_close is not None:
                    ratio_change = (price_close - ref_price) / ref_price * 100
            except Exception:
                ratio_change = None

            return {
                "symbol": symbol,
                "exchange": exchange,
                "name": company_info["name"],
                "industry": company_info["industry"],
                "price": price_close,
                "change": (price_close - (open_price or 0)) if price_close is not None and open_price is not None else None,
                "ratioChange": ratio_change,
                "refPrice": ref_price,
                "openPrice": open_price,
                "highPrice": high_price,
                "lowPrice": low_price,
                "marketCap": f_data.get("marketCap") if isinstance(f_data, dict) else None,
                "peRatio": f_data.get("pe") if isinstance(f_data, dict) else None,
                "eps": f_data.get("eps") if isinstance(f_data, dict) else None,
                "foreignBuyVolume": foreignBuy,
                "foreignBuyValue": foreignBuyValue,
                "foreignSellVolume": foreignSell,
                "foreignSellValue": foreignSellValue,
                "foreignNetVolume": foreignNetVolume,
                "foreignNetValue": foreignNetValue,
                "created_at": datetime.now(VN_TZ)
            }
        except Exception as e:
            print(f"Thử {attempt+1}/{retries} lỗi {symbol}: {e}")
            time.sleep(delay)
    return None


def save_to_db():
    all_data = []
    # giảm workers để tránh bị rate-limit
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        futures = [executor.submit(get_data, sym) for sym in all_symbols]
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            if result:
                all_data.append(result)

    if all_data:
        df = pd.DataFrame(all_data)
        df = df.drop_duplicates(subset=["symbol"], keep="last")  # loại trùng
        df.to_sql("stock_details", engine, schema="history_data", if_exists="replace", index=False)
        print(f"✅ Đã lưu {len(df)} bản ghi vào history_data.stock_details")
    else:
        print("⚠️ Không có dữ liệu để lưu.")


if __name__ == "__main__":
    save_to_db()