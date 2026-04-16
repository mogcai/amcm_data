import os
import pandas as pd
import re
import sqlite3
from sqlalchemy import create_engine, inspect, text

# for create db

def create_db(engine, db_table_name, df, unique_keys):
    df.to_sql(db_table_name, con=engine, if_exists='replace', index=False)
    # quoted_keys = ", ".join([f'"{k}"' for k in unique_keys])
    index_name = f"idx_{db_table_name}_unique"

    # with engine.connect() as conn:
    #     conn.execute(text(f"""
    #         CREATE UNIQUE INDEX IF NOT EXISTS {index_name}
    #         ON {db_table_name} ({', '.join(unique_keys)})
    #     """))
    #     conn.commit()
    quoted_keys = ", ".join([f'"{k}"' for k in unique_keys])
    with engine.begin() as conn:  # .begin() automatically commits
        conn.execute(text(f"""
            CREATE UNIQUE INDEX IF NOT EXISTS {index_name}
            ON {db_table_name} ({quoted_keys})
        """))
    print(f"Index {index_name} created successfully.")

# # --- 1. PREPARATION (Only runs if table doesn't exist) ---
# # We use 'append' + a check to ensure we don't wipe data
# with engine.connect() as conn:
#     # Create the table if it's missing (empty df to just get the schema)
#     df.head(0).to_sql(db_table_name, con=engine, if_exists='append', index=False)
    
#     # Ensure the Unique Index exists
#     conn.execute(text(f"""
#         CREATE UNIQUE INDEX IF NOT EXISTS idx_composite_keys
#         ON {db_table_name} ({', '.join(unique_keys)})
#     """))
#     conn.commit()

def upsert_to_sqlite(engine, db_table_name, df_to_insert, unique_keys):
    if df_to_insert.empty:
        print("✨ No changes found.")
        return

    all_cols = df_to_insert.columns.tolist()
    update_cols = [c for c in all_cols if c not in unique_keys]
    
    # 1. Create a mapping of Real Column Name -> Clean Parameter Name
    # e.g., "Nominal_Value_(M)" becomes "param_Nominal_Value__M_"
    col_to_param = {c: f"param_{re.sub(r'[^a-zA-Z0-9]', '_', c)}" for c in all_cols}

    # 2. Build the SQL using the clean parameter names
    col_names_str = ", ".join([f'"{c}"' for c in all_cols])
    placeholders = ", ".join([f":{col_to_param[c]}" for c in all_cols])
    conflict_keys = ", ".join([f'"{c}"' for c in unique_keys])
    update_stmt = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in update_cols])

    upsert_sql = text(f"""
        INSERT INTO {db_table_name} ({col_names_str})
        VALUES ({placeholders})
        ON CONFLICT({conflict_keys}) 
        DO UPDATE SET {update_stmt}
    """)

    # 3. Transform the dictionary keys to match the clean parameter names
    records = []
    for row in df_to_insert.to_dict(orient='records'):
        clean_row = {col_to_param[k]: v for k, v in row.items()}
        records.append(clean_row)

    # 4. Execute
    with engine.begin() as conn:
        conn.execute(upsert_sql, records)
    
    print(f"✅ Successfully synced {len(df_to_insert)} rows (handled special characters).")

def check_updates(engine, db_table_name, df, unique_keys):
    # 執行替換
    df['updated_at'] = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
    df=df.fillna('')

    # 1. 撈出 DB 現有的數據 (做對比用)
    existing_df = pd.read_sql(f"SELECT * FROM {db_table_name}", engine)
    existing_df=existing_df.fillna('')

    # 解決existing_df 0.0不等於df 0的問題 (暫時得'No__of_Roll_over'出事)
    if 'No__of_Roll_over' in existing_df.columns:
        existing_df['No__of_Roll_over'] = pd.to_numeric(existing_df['No__of_Roll_over'], errors='coerce')
        existing_df['No__of_Roll_over'] = existing_df['No__of_Roll_over'].astype('Int64').astype(str).replace('<NA>', '')

    # 2. 定義你想對比嘅欄位 (除了 Key 之外所有欄位)
    cols_to_compare = [c for c in df.columns if c not in unique_keys + ['updated_at']]

    if 'Date' in unique_keys:
        # Convert local df date to string
        df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')
        # Convert DB date to string (and handle any potential nulls)
        existing_df['Date'] = pd.to_datetime(existing_df['Date']).dt.strftime('%Y-%m-%d')
    

        
    for col in cols_to_compare:
        # 1. First, handle the DB/Dataframe NaNs so they don't become the string 'nan'
        df[col] = df[col].fillna('')
        existing_df[col] = existing_df[col].fillna('')

        # 2. Check if the column is naturally numeric (int or float)
        if df[col].dtype == bool or set(df[col].unique()).issubset({True, False, 1, 0, 'True', 'False', '1', '0'}):
            df[col] = df[col].map({'True': 1, 'False': 0, True: 1, False: 0, '1': 1, '0': 0, 1: 1, 0: 0}).fillna(0).astype(int)
            existing_df[col] = existing_df[col].map({'True': 1, 'False': 0, True: 1, False: 0, '1': 1, '0': 0, 1: 1, 0: 0}).fillna(0).astype(int)

        elif pd.api.types.is_numeric_dtype(df[col]) or pd.api.types.is_numeric_dtype(existing_df[col]):
            # Only now is it safe to treat as a number
            # Convert to numeric, round to fix the .35999999 issue, then to string to match DB
            df[col] = pd.to_numeric(df[col], errors='coerce').round(4).astype(str).str.replace(r'\.0$', '', regex=True)
            existing_df[col] = pd.to_numeric(existing_df[col], errors='coerce').round(4).astype(str).str.replace(r'\.0$', '', regex=True)
        else:
            # It's a text column (like your ID), just keep it as a clean string
            df[col] = df[col].astype(str).str.strip()
            existing_df[col] = existing_df[col].astype(str).str.strip()


        # 3. Final cleanup: after all that conversion, make sure 'nan' strings go back to ''
        df[col] = df[col].replace('nan', '')
        existing_df[col] = existing_df[col].replace('nan', '')

    for key in unique_keys:
        # fillna('') ensures null keys don't become the string 'nan'
        df[key] = df[key].fillna('').astype(str).str.strip()
        existing_df[key] = existing_df[key].fillna('').astype(str).str.strip()
    
    # 3. 執行 Merge
    comparison = pd.merge(
        df, 
        existing_df, 
        on=unique_keys, 
        how='left', 
        suffixes=('', '_old')
    )

    update_mask = comparison[f"{cols_to_compare[0]}_old"].isna()

    # 5. 循環檢查所有對比欄位：只要其中一個有變，就標記為需要更新
    for col in cols_to_compare:
        old_col = f"{col}_old"
        # 如果新舊唔同，或者新數唔係 NaN (處理 NULL 安全對比)
        update_mask |= (comparison[col] != comparison[old_col])

    # 6. 提取真正需要更新嘅數據
    to_update = comparison[update_mask][df.columns]

    return to_update


def create_update_db(engine, db_table_name, df, unique_keys):
    inspector = inspect(engine)
    # Check if the table exists
    if inspector.has_table(db_table_name):
        print(f"Table '{db_table_name}' exists.")
        to_update=check_updates(engine, db_table_name, df, unique_keys)
        upsert_to_sqlite(engine, db_table_name, to_update, unique_keys)
    else:
        print(f"Table '{db_table_name}' does not exist, creating...")
        create_db(engine, db_table_name, df, unique_keys)