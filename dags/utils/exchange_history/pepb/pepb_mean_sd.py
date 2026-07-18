from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool
import pandas as pd

engine = create_engine(
    "postgresql+psycopg2://root:Dnl_123456@tanhungsoft.com:5432/dnl",
    poolclass=NullPool
)

def pepb_mean_sd():
    for metric in ['pe', 'pb']:
        for exchange in ['HOSE', 'HNX', 'UPCOM']:
            table = f'{metric}_{exchange}'
            try:
                df = pd.read_sql(f'SELECT date, "{metric}" FROM exchange_history."{table}" ORDER BY date ASC', engine)
                if df.empty:
                    continue

                def get_sd_values(series, window):
                    s = series.tail(window)
                    m = s.mean()
                    std = s.std()
                    return {
                        'mean':     round(m, 4),
                        'plus1SD':  round(m + std, 4),
                        'plus2SD':  round(m + 2*std, 4),
                        'minus1SD': round(m - std, 4),
                        'minus2SD': round(m - 2*std, 4),
                    }

                sd_1y = get_sd_values(df[metric], 252)
                sd_3y = get_sd_values(df[metric], 252 * 3)
                sd_5y = get_sd_values(df[metric], 252 * 5)

                # Fill cùng 1 giá trị cho tất cả các ngày
                df['mean_1Y']     = sd_1y['mean']
                df['plus1SD_1Y']  = sd_1y['plus1SD']
                df['plus2SD_1Y']  = sd_1y['plus2SD']
                df['minus1SD_1Y'] = sd_1y['minus1SD']
                df['minus2SD_1Y'] = sd_1y['minus2SD']

                df['mean_3Y']     = sd_3y['mean']
                df['plus1SD_3Y']  = sd_3y['plus1SD']
                df['plus2SD_3Y']  = sd_3y['plus2SD']
                df['minus1SD_3Y'] = sd_3y['minus1SD']
                df['minus2SD_3Y'] = sd_3y['minus2SD']

                df['mean_5Y']     = sd_5y['mean']
                df['plus1SD_5Y']  = sd_5y['plus1SD']
                df['plus2SD_5Y']  = sd_5y['plus2SD']
                df['minus1SD_5Y'] = sd_5y['minus1SD']
                df['minus2SD_5Y'] = sd_5y['minus2SD']

                sd_cols = [
                    'mean_1Y','plus1SD_1Y','plus2SD_1Y','minus1SD_1Y','minus2SD_1Y',
                    'mean_3Y','plus1SD_3Y','plus2SD_3Y','minus1SD_3Y','minus2SD_3Y',
                    'mean_5Y','plus1SD_5Y','plus2SD_5Y','minus1SD_5Y','minus2SD_5Y',
                ]

                with engine.begin() as conn:
                    for col in sd_cols:
                        conn.execute(text(f'ALTER TABLE exchange_history."{table}" ADD COLUMN IF NOT EXISTS "{col}" DOUBLE PRECISION'))
                    for _, row in df.iterrows():
                        row_dict = {k: (None if pd.isna(v) else v) for k, v in row.to_dict().items()}
                        updates = ', '.join([f'"{c}" = :{c}' for c in sd_cols])
                        conn.execute(text(f"""
                            UPDATE exchange_history."{table}"
                            SET {updates}
                            WHERE date = :date
                        """), row_dict)

                print(f"✅ {table}")
            except Exception as e:
                print(f"❌ {table}: {e}")

if __name__ == '__main__':
    pepb_mean_sd()