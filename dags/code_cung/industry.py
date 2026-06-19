import requests
import pandas as pd
from sqlalchemy import create_engine, text

# ===================== CONFIG =====================

DB_URL = "postgresql://vnsfintech:Vns_123456@tanhungsoft.com:5433/vnsfintech"

VIETCAP_IQ_HEADERS = {
    "accept": "application/json",
    "accept-language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
    "origin": "https://trading.vietcap.com.vn",
    "referer": "https://trading.vietcap.com.vn/",
    "user-agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/140.0.0.0 Safari/537.36"
    ),
    "sec-fetch-site": "same-site",
    "sec-fetch-mode": "cors",
    "sec-fetch-dest": "empty",
}

# ===================== SYMBOL LIST =====================
# dùng danh sách mã để quét ra industry
HOSE = [
    "AAA",	"AAM",	"AAT",	"ABR",	"ABS",	"ABT",	"ACB",	"ACC",	"ACG",	"ACL",
    "ADG",	"ADP",	"ADS",	"AGG",	"AGR",	"ANV",	"APG",	"APH",	"ASG",	"ASM",
    "ASP",	"AST",	"BAF",	"BBC",	"BCE",	"BCG",	"BCM",	"BFC",	"BHN",	"BIC",
    "BID",	"BKG",	"BMC",	"BMI",	"BMP",	"BRC",	"BSI",	"BSR",	"BTP",	"BTT",
    "BVH",	"BWE",	"C32",	"C47",	"CCC",	"CCI",	"CCL",	"CDC",	"CHP",	"CIG",
    "CII",	"CKG",	"CLC",	"CLL",	"CLW",	"CMG",	"CMV",	"CMX",	"CNG",	"COM",
    "CRC",	"CRE",	"CRV",	"CSM",	"CSV",	"CTD",	"CTF",	"CTG",	"CTI",	"CTR",
    "CTS",	"CVT",	"D2D",	"DAH",	"DAT",	"DBC",	"DBD",	"DBT",	"DC4",	"DCL",
    "DCM",	"DGC",	"DGW",	"DHA",	"DHC",	"DHG",	"DHM",	"DIG",	"DLG",	"DMC",
    "DPG",	"DPM",	"DPR",	"DQC",	"DRC",	"DRH",	"DRL",	"DSC",	"DSE",	"DSN",
    "DTA",	"DTL",	"DTT",	"DVP",	"DXG",	"DXS",	"DXV",	"EIB",	"ELC",	"EVE",
    "EVF",	"EVG",	"FCM",	"FCN",	"FDC",	"FIR",	"FIT",	"FMC",	"FPT",	"FRT",
    "FTS",	"GAS",	"GDT",	"GEE",	"GEG",	"GEX",	"GIL",	"GMD",	"GMH",	"GSP",
    "GTA",	"GVR",	"HAG",	"HAH",	"HAP",	"HAR",	"HAS",	"HAX",	"HCD",	"HCM",
    "HDB",	"HDC",	"HDG",	"HHP",	"HHS",	"HHV",	"HID",	"HII",	"HMC",	"HNA",
    "HPG",	"HPX",	"HQC",	"HRC",	"HSG",	"HSL",	"HT1",	"HTG",	"HTI",	"HTL",
    "HTN",	"HTV",	"HU1",	"HUB",	"HVH",	"HVN",	"HVX",	"ICT",	"IDI",	"IJC",
    "ILB",	"IMP",	"ITC",	"ITD",	"JVC",	"KBC",	"KDC",	"KDH",	"KHG",	"KHP",
    "KMR",	"KOS",	"KSB",	"L10",	"LAF",	"LBM",	"LCG",	"LDG",	"LGC",	"LGL",
    "LHG",	"LIX",	"LM8",	"LPB",	"LSS",	"MBB",	"MCM",	"MCP",	"MDG",	"MHC",
    "MIG",	"MSB",	"MSH",	"MSN",	"MWG",	"NAB",	"NAF",	"NAV",	"NBB",	"NCT",
    "NHA",	"NHH",	"NHT",	"NKG",	"NLG",	"NNC",	"NO1",	"NSC",	"NT2",	"NTC",
    "NTL",	"NVL",	"NVT",	"OCB",	"OGC",	"OPC",	"ORS",	"PAC",	"PAN",	"PC1",
    "PDN",	"PDR",	"PET",	"PGC",	"PGD",	"PGI",	"PGV",	"PHC",	"PHR",	"PIT",
    "PJT",	"PLP",	"PLX",	"PMG",	"PNC",	"PNJ",	"POW",	"PPC",	"PTB",	"PTC",
    "PTL",	"PVD",	"PVP",	"PVT",	"QCG",	"QNP",	"RAL",	"REE",	"RYG",	"S4A",
    "SAB",	"SAM",	"SAV",	"SBA",	"SBG",	"SBT",	"SBV",	"SC5",	"SCR",	"SCS",
    "SFC",	"SFG",	"SFI",	"SGN",	"SGR",	"SGT",	"SHA",	"SHB",	"SHI",	"SHP",
    "SIP",	"SJD",	"SJS",	"SKG",	"SMA",	"SMB",	"SMC",	"SPM",	"SRC",	"SRF",
    "SSB",	"SSC",	"SSI",	"ST8",	"STB",	"STG",	"STK",	"SVC",	"SVD",	"SVI",
    "SVT",	"SZC",	"SZL",	"TAL",	"TBC",	"TCB",	"TCD",	"TCH",	"TCI",	"TCL",
    "TCM",	"TCO",	"TCR",	"TCT",	"TCX",	"TDC",	"TDG",	"TDH",	"TDM",	"TDP",
    "TDW",	"TEG",	"THG",	"TIP",	"TIX",	"TLD",	"TLG",	"TLH",	"TMP",	"TMS",
    "TMT",	"TN1",	"TNC",	"TNH",	"TNI",	"TNT",	"TPB",	"TPC",	"TRA",	"TRC",
    "TSC",	"TTA",	"TTE",	"TTF",	"TV2",	"TVB",	"TVS",	"TVT",	"TYA",	"UIC",
    "VAB",	"VAF",	"VCA",	"VCB",	"VCF",	"VCG",	"VCI",	"VDP",	"VDS",	"VFG",
    "VGC",	"VHC",	"VHM",	"VIB",	"VIC",	"VID",	"VIP",	"VIX",	"VJC",	"VMD",
    "VND",	"VNE",	"VNG",	"VNL",	"VNM",	"VNS",	"VOS",	"VPB",	"VPD",	"VPG",
    "VPH",	"VPI",	"VPL",	"VPS",	"VRC",	"VRE",	"VSC",	"VSH",	"VSI",	"VTB",
    "VTO",	"VTP",	"YBM",	"YEG",
]
# <HOSE END>

# <HNX START>
HNX = [
    "AAV",	"ADC",	"ALT",	"AMC",	"AME",	"AMV",	"API",	"APS",	"ARM",	"ATS",
    "BAB",	"BAX",	"BBS",	"BCC",	"BCF",	"BED",	"BKC",	"BNA",	"BPC",	"BSC",
    "BTS",	"BTW",	"BVS",	"BXH",	"C69",	"CAG",	"CAN",	"CAP",	"CAR",	"CCR",
    "CDN",	"CEO",	"CET",	"CIA",	"CJC",	"CKV",	"CLH",	"CLM",	"CMC",	"CMS",
    "CPC",	"CSC",	"CST",	"CTB",	"CTP",	"CTT",	"CX8",	"D11",	"DAD",	"DAE",
    "DC2",	"DDG",	"DHP",	"DHT",	"DIH",	"DL1",	"DNC",	"DNP",	"DP3",	"DS3",
    "DST",	"DTD",	"DTG",	"DTK",	"DVM",	"DXP",	"EBS",	"ECI",	"EID",	"EVS",
    "FID",	"GDW",	"GIC",	"GKM",	"GLT",	"GMA",	"GMX",	"HAD",	"HAT",	"HBS",
    "HCC",	"HCT",	"HDA",	"HEV",	"HGM",	"HHC",	"HJS",	"HKT",	"HLC",	"HLD",
    "HMH",	"HMR",	"HOM",	"HTC",	"HUT",	"HVT",	"ICG",	"IDC",	"IDJ",	"IDV",
    "INC",	"INN",	"IPA",	"ITQ",	"IVS",	"KDM",	"KHS",	"KKC",	"KMT",	"KSD",
    "KSF",	"KST",	"KSV",	"KTS",	"L14",	"L18",	"L40",	"LAS",	"LBE",	"LCD",
    "LDP",	"LHC",	"LIG",	"MAC",	"MAS",	"MBG",	"MBS",	"MCC",	"MCF",	"MCO",
    "MDC",	"MED",	"MEL",	"MIC",	"MKV",	"MST",	"MVB",	"NAG",	"NAP",	"NBC",
    "NBP",	"NBW",	"NDN",	"NDX",	"NET",	"NFC",	"NHC",	"NRC",	"NSH",	"NST",
    "NTH",	"NTP",	"NVB",	"OCH",	"ONE",	"PBP",	"PCE",	"PCH",	"PCT",	"PDB",
    "PEN",	"PGN",	"PGS",	"PGT",	"PHN",	"PIA",	"PIC",	"PJC",	"PLC",	"PMB",
    "PMC",	"PMP",	"PMS",	"POT",	"PPE",	"PPP",	"PPS",	"PPT",	"PPY",	"PRC",
    "PRE",	"PSC",	"PSD",	"PSE",	"PSI",	"PSW",	"PTD",	"PTI",	"PTS",	"PTX",
    "PV2",	"PVB",	"PVC",	"PVG",	"PVI",	"PVS",	"QHD",	"QST",	"QTC",	"RCL",
    "S55",	"S99",	"SAF",	"SCG",	"SCI",	"SD5",	"SD9",	"SDA",	"SDC",	"SDG",
    "SDN",	"SDU",	"SEB",	"SED",	"SFN",	"SGC",	"SGD",	"SGH",	"SHE",	"SHN",
    "SHS",	"SJ1",	"SJE",	"SLS",	"SMN",	"SMT",	"SPC",	"SRA",	"SSM",	"STC",
    "STP",	"SVN",	"SZB",	"TA9",	"TBX",	"TD6",	"TDT",	"TET",	"TFC",	"THB",
    "THD",	"THS",	"THT",	"TIG",	"TJC",	"TKU",	"TMB",	"TMC",	"TMX",	"TNG",
    "TOT",	"TPP",	"TSB",	"TTC",	"TTH",	"TTL",	"TTT",	"TV3",	"TV4",	"TVC",
    "TVD",	"TXM",	"UNI",	"V12",	"V21",	"VBC",	"VC1",	"VC2",	"VC3",	"VC6",
    "VC7",	"VC9",	"VCC",	"VCM",	"VCS",	"VDL",	"VE1",	"VE3",	"VE4",	"VFS",
    "VGP",	"VGS",	"VHE",	"VHL",	"VIF",	"VIG",	"VIT",	"VLA",	"VMC",	"VMS",
    "VNC",	"VNF",	"VNR",	"VNT",	"VSA",	"VSM",	"VTC",	"VTH",	"VTJ",	"VTV",
    "VTZ",	"WCS",	"WSS",	"X20",
]
# <HNX END>

# <UPCOM START>
UPCOM = [
    "A32",	"AAH",	"AAS",	"ABB",	"ABC",	"ABI",	"ABW",	"ACE",	"ACM",	"ACS",
    "ACV",	"AFX",	"AG1",	"AGF",	"AGM",	"AGP",	"AGX",	"AIC",	"AIG",	"ALV",
    "AMD",	"AMP",	"AMS",	"ANT",	"APC",	"APF",	"APL",	"APP",	"APT",	"ART",
    "ASA",	"ATA",	"ATB",	"ATG",	"AVC",	"AVF",	"AVG",	"BAL",	"BBH",	"BBM",
    "BBT",	"BCA",	"BCB",	"BCP",	"BCR",	"BCV",	"BDG",	"BDT",	"BDW",	"BEL",
    "BGE",	"BGW",	"BHA",	"BHC",	"BHG",	"BHH",	"BHI",	"BHK",	"BHP",	"BIG",
    "BII",	"BIO",	"BLF",	"BLI",	"BLN",	"BLT",	"BMD",	"BMF",	"BMG",	"BMJ",
    "BMK",	"BMS",	"BMV",	"BNW",	"BOT",	"BQB",	"BQP",	"BRR",	"BRS",	"BSA",
    "BSD",	"BSG",	"BSH",	"BSL",	"BSP",	"BSQ",	"BT1",	"BT6",	"BTB",	"BTD",
    "BTG",	"BTH",	"BTN",	"BTU",	"BTV",	"BVB",	"BVG",	"BVL",	"BVN",	"BWA",
    "BWS",	"C12",	"C21",	"C22",	"C4G",	"C92",	"CAD",	"CAT",	"CBI",	"CBS",
    "CC1",	"CCA",	"CCM",	"CCP",	"CCT",	"CCV",	"CDG",	"CDH",	"CDO",	"CDP",
    "CDR",	"CEN",	"CFM",	"CFV",	"CGV",	"CH5",	"CHC",	"CHS",	"CI5",	"CID",
    "CIP",	"CK8",	"CKA",	"CKD",	"CLG",	"CLX",	"CMD",	"CMF",	"CMI",	"CMK",
    "CMM",	"CMN",	"CMP",	"CMT",	"CMW",	"CNA",	"CNC",	"CNN",	"CNT",	"CPA",
    "CPH",	"CPI",	"CQN",	"CQT",	"CSI",	"CT3",	"CT6",	"CTA",	"CTN",	"CTW",
    "CTX",	"CVN",	"CYC",	"DAC",	"DAG",	"DAN",	"DAS",	"DBM",	"DC1",	"DCF",
    "DCG",	"DCH",	"DCR",	"DCS",	"DCT",	"DDB",	"DDH",	"DDM",	"DDN",	"DDV",
    "DFC",	"DFF",	"DGT",	"DHB",	"DHD",	"DHN",	"DIC",	"DID",	"DKC",	"DKG",
    "DKW",	"DLD",	"DLR",	"DLT",	"DM7",	"DMN",	"DMS",	"DNA",	"DND",	"DNE",
    "DNH",	"DNL",	"DNM",	"DNN",	"DNT",	"DNW",	"DOC",	"DOP",	"DP1",	"DP2",
    "DPC",	"DPH",	"DPP",	"DPS",	"DRG",	"DRI",	"DSD",	"DSG",	"DSH",	"DSP",
    "DTC",	"DTE",	"DTH",	"DTI",	"DTP",	"DUS",	"DVC",	"DVG",	"DVN",	"DVT",
    "DVW",	"DWC",	"DWS",	"DXL",	"DZM",	"E12",	"E29",	"ECO",	"EFI",	"EGL",
    "EIC",	"EIN",	"EME",	"EMG",	"EMS",	"F88",	"FBA",	"FBC",	"FCC",	"FCS",
    "FGL",	"FHN",	"FHS",	"FIC",	"FLC",	"FOC",	"FOX",	"FRC",	"FRM",	"FSO",
    "FT1",	"FTI",	"FTM",	"G20",	"G36",	"GAB",	"GCB",	"GCF",	"GDA",	"GER",
    "GGG",	"GH3",	"GHC",	"GLC",	"GLW",	"GMC",	"GND",	"GPC",	"GSM",	"GTD",
    "GTS",	"GTT",	"GVT",	"H11",	"HAC",	"HAF",	"HAI",	"HAM",	"HAN",	"HAV",
    "HBC",	"HBD",	"HBH",	"HC1",	"HC3",	"HCI",	"HD2",	"HD6",	"HD8",	"HDM",
    "HDP",	"HDW",	"HEC",	"HEJ",	"HEP",	"HES",	"HFB",	"HFC",	"HFX",	"HGT",
    "HHB",	"HHG",	"HHN",	"HIO",	"HJC",	"HKB",	"HLA",	"HLB",	"HLO",	"HLS",
    "HLT",	"HLY",	"HMD",	"HMG",	"HMS",	"HNB",	"HND",	"HNF",	"HNG",	"HNI",
    "HNM",	"HNP",	"HNR",	"HOT",	"HPB",	"HPD",	"HPH",	"HPI",	"HPM",	"HPO",
    "HPP",	"HPT",	"HPW",	"HRB",	"HSA",	"HSI",	"HSM",	"HSP",	"HSV",	"HTE",
    "HTM",	"HTP",	"HTT",	"HU3",	"HU4",	"HU6",	"HUG",	"HVA",	"HVG",	"HWS",
    "IBC",	"IBD",	"ICC",	"ICF",	"ICI",	"ICN",	"IDP",	"IFS",	"IHK",	"ILA",
    "ILC",	"ILS",	"IME",	"IN4",	"ING",	"IRC",	"ISG",	"ISH",	"IST",	"ITA",
    "ITS",	"JOS",	"KCB",	"KGM",	"KHD",	"KHL",	"KHW",	"KIP",	"KLB",	"KLF",
    "KPF",	"KSH",	"KSQ",	"KTC",	"KTL",	"KTT",	"KVC",	"KWA",	"L12",	"L35",
    "L43",	"L44",	"L45",	"L61",	"L62",	"L63",	"LAI",	"LAW",	"LCC",	"LCM",
    "LCS",	"LDW",	"LEC",	"LG9",	"LIC",	"LKW",	"LLM",	"LM3",	"LM7",	"LMC",
    "LMH",	"LMI",	"LNC",	"LO5",	"LPT",	"LQN",	"LSG",	"LTC",	"LTG",	"LUT",
    "M10",	"MA1",	"MBN",	"MBT",	"MCG",	"MCH",	"MDA",	"MDF",	"MEC",	"MEF",
    "MES",	"MFS",	"MGC",	"MGG",	"MGR",	"MH3",	"MHL",	"MIE",	"MIM",	"MKP",
    "MLC",	"MLS",	"MML",	"MNB",	"MND",	"MPC",	"MPT",	"MPY",	"MQB",	"MQN",
    "MRF",	"MSR",	"MTA",	"MTB",	"MTG",	"MTH",	"MTL",	"MTP",	"MTS",	"MTV",
    "MVC",	"MVN",	"MZG",	"NAC",	"NAS",	"NAU",	"NAW",	"NBE",	"NBT",	"NCG",
    "NCS",	"ND2",	"NDC",	"NDF",	"NDP",	"NDT",	"NDW",	"NED",	"NGC",	"NHP",
    "NHV",	"NJC",	"NLS",	"NNT",	"NOS",	"NQB",	"NQN",	"NS2",	"NSG",	"NSL",
    "NSS",	"NTB",	"NTF",	"NTT",	"NTW",	"NUE",	"NVP",	"NWT",	"NXT",	"ODE",
    "OIL",	"ONW",	"PAI",	"PAP",	"PAS",	"PAT",	"PBC",	"PBT",	"PCC",	"PCF",
    "PCG",	"PCM",	"PDC",	"PEC",	"PEG",	"PEQ",	"PFL",	"PGB",	"PHH",	"PHP",
    "PHS",	"PID",	"PIS",	"PIV",	"PJS",	"PLA",	"PLE",	"PLO",	"PMJ",	"PMT",
    "PMW",	"PND",	"PNG",	"PNP",	"PNT",	"POB",	"POM",	"POS",	"POV",	"PPH",
    "PPI",	"PQN",	"PRO",	"PRT",	"PSB",	"PSG",	"PSH",	"PSL",	"PSN",	"PSP",
    "PTE",	"PTG",	"PTH",	"PTM",	"PTO",	"PTP",	"PTT",	"PTV",	"PVA",	"PVE",
    "PVH",	"PVL",	"PVM",	"PVO",	"PVR",	"PVV",	"PVX",	"PVY",	"PWA",	"PWS",
    "PX1",	"PXA",	"PXC",	"PXI",	"PXL",	"PXM",	"PXS",	"PXT",	"QBS",	"QCC",
    "QHW",	"QNC",	"QNS",	"QNT",	"QNU",	"QNW",	"QPH",	"QSP",	"QTP",	"RAT",
    "RBC",	"RCC",	"RCD",	"RDP",	"RGG",	"RIC",	"RTB",	"S12",	"S72",	"S74",
    "S96",	"SAC",	"SAL",	"SAP",	"SAS",	"SB1",	"SBB",	"SBD",	"SBH",	"SBL",
    "SBM",	"SBR",	"SBS",	"SCC",	"SCD",	"SCJ",	"SCL",	"SCO",	"SD1",	"SD2",
    "SD3",	"SD4",	"SD6",	"SD7",	"SD8",	"SDB",	"SDD",	"SDK",	"SDP",	"SDT",
    "SDV",	"SDY",	"SEA",	"SEP",	"SGB",	"SGI",	"SGP",	"SGS",	"SHC",	"SHG",
    "SID",	"SIG",	"SII",	"SIV",	"SJC",	"SJF",	"SJG",	"SJM",	"SKH",	"SKN",
    "SKV",	"SLD",	"SNC",	"SNZ",	"SP2",	"SPB",	"SPD",	"SPH",	"SPI",	"SPV",
    "SQC",	"SRB",	"SSF",	"SSG",	"SSH",	"SSN",	"STH",	"STL",	"STS",	"STT",
    "STW",	"SVG",	"SVH",	"SWC",	"SZE",	"SZG",	"TA6",	"TAB",	"TAN",	"TAR",
    "TAW",	"TB8",	"TBD",	"TBH",	"TBR",	"TBW",	"TCJ",	"TCK",	"TCW",	"TDB",
    "TDF",	"TDS",	"TED",	"TEL",	"TGG",	"TGP",	"TH1",	"THM",	"THN",	"THP",
    "THU",	"THW",	"TID",	"TIE",	"TIN",	"TIS",	"TKA",	"TKC",	"TL4",	"TLI",
    "TLP",	"TLT",	"TMG",	"TMW",	"TNA",	"TNB",	"TNM",	"TNP",	"TNS",	"TNV",
    "TNW",	"TOP",	"TOS",	"TOW",	"TPS",	"TQN",	"TQW",	"TR1",	"TRS",	"TRT",
    "TRV",	"TS3",	"TS4",	"TSA",	"TSD",	"TSG",	"TSJ",	"TST",	"TT6",	"TTB",
    "TTD",	"TTG",	"TTN",	"TTS",	"TTZ",	"TUG",	"TV1",	"TV6",	"TVA",	"TVG",
    "TVH",	"TVM",	"TVN",	"TW3",	"UCT",	"UDC",	"UDJ",	"UDL",	"UEM",	"UMC",
    "UPC",	"UPH",	"USC",	"USD",	"UTT",	"UXC",	"V15",	"VAV",	"VBB",	"VBG",
    "VBH",	"VC5",	"VCE",	"VCP",	"VCR",	"VCT",	"VCW",	"VCX",	"VDB",	"VDG",
    "VDN",	"VDT",	"VE2",	"VE8",	"VE9",	"VEA",	"VEC",	"VEF",	"VES",	"VET",
    "VFC",	"VFR",	"VGG",	"VGI",	"VGL",	"VGR",	"VGT",	"VGV",	"VHD",	"VHF",
    "VHG",	"VHH",	"VIE",	"VIH",	"VIM",	"VIN",	"VIR",	"VIW",	"VKC",	"VKP",
    "VLB",	"VLC",	"VLF",	"VLG",	"VLP",	"VLW",	"VMA",	"VMG",	"VMK",	"VMT",
    "VNA",	"VNB",	"VNH",	"VNI",	"VNP",	"VNX",	"VNY",	"VNZ",	"VOC",	"VPA",
    "VPC",	"VPR",	"VPW",	"VQC",	"VRG",	"VSE",	"VSF",	"VSG",	"VSN",	"VST",
    "VTA",	"VTD",	"VTE",	"VTG",	"VTI",	"VTK",	"VTM",	"VTQ",	"VTR",	"VTS",
    "VTX",	"VUA",	"VVN",	"VVS",	"VW3",	"VWS",	"VXB",	"VXP",	"VXT",	"WSB",
    "WTC",	"X26",	"X77",	"XDH",	"XHC",	"XLV",	"XMC",	"XMD",	"XMP",	"XPH",
    "YBC",	"YTC",
]

# ===================== GET INDUSTRY FROM VIETCAP IQ =====================
def get_industry_from_symbol(symbol: str):
    """
    return:
        {
            industry : industry ENG (PK),
            name     : industry VI
        }
    """
    url = f"https://iq.vietcap.com.vn/api/iq-insight-service/v1/company/{symbol}"

    try:
        res = requests.get(url, headers=VIETCAP_IQ_HEADERS, timeout=30)
        res.raise_for_status()

        data = res.json().get("data")
        if not data:
            return None

        industry_en = data.get("sector")
        industry_vi = data.get("sectorVn")

        if not industry_en or not industry_vi:
            return None

        return {
            "industry": industry_en.strip(),
            "name": industry_vi.strip(),
        }

    except requests.RequestException as e:
        print(f"❌ Vietcap IQ error {symbol}: {e}")
        return None


# ===================== COLLECT UNIQUE INDUSTRIES =====================
def collect_industries(symbols: list):
    industry_map = {}

    for sym in symbols:
        info = get_industry_from_symbol(sym.strip().upper())
        if info:
            # industry ENG làm key (PK)
            industry_map[info["industry"]] = info["name"]

    return [
        {"industry": k, "name": v}
        for k, v in industry_map.items()
    ]


# ===================== DATABASE =====================
def save_to_postgres(df: pd.DataFrame):
    engine = create_engine(DB_URL)

    with engine.begin() as conn:
        sql = text("""
            INSERT INTO info.industry (industry, name)
            VALUES (:industry, :name)
            ON CONFLICT (industry)
            DO UPDATE SET
                name = EXCLUDED.name;
        """)

        conn.execute(sql, df.to_dict(orient="records"))


# ===================== MAIN =====================
if __name__ == "__main__":
    symbols = HOSE + HNX + UPCOM

    industries = collect_industries(symbols)

    if not industries:
        print("⚠️ Không lấy được industry nào.")
    else:
        df = pd.DataFrame(industries).sort_values("industry")
        print(df.to_string(index=False))
        save_to_postgres(df)
