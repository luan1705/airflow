#!/usr/bin/env python3
import psycopg2

# Kết nối DB (đổi nếu cần)
CONN_STR = "postgresql://vnsfintech:Vns_123456@videv.cloud:5433/vnsfintech"
#"postgresql://vnsfintech:%40Vns123456@videv.cloud:5432/vnsfintech"

def clear_eboard():
    conn = psycopg2.connect(CONN_STR)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("TRUNCATE TABLE details.asset, details.dnse_asset;")
    cur.close()
    conn.close()

if __name__ == "__main__":
    clear_eboard()