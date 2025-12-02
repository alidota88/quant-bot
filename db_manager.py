import os
from sqlalchemy import create_engine, text
import pandas as pd

class DBManager:
    def __init__(self, db_path='/app/data/quant.db'):
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self.engine = create_engine(f'sqlite:///{db_path}')
        
    def save_data(self, df, table_name, if_exists='append'):
        if df.empty: return
        try:
            df.to_sql(table_name, self.engine, if_exists=if_exists, index=False)
        except Exception as e:
            print(f"❌ 保存 {table_name} 失败: {e}")

    def get_data(self, table_name, start_date=None, end_date=None, codes=None):
        """
        核心升级：支持 codes 参数，实现 SQL 级别的过滤
        """
        query = f"SELECT * FROM {table_name} WHERE 1=1"
        
        if start_date:
            query += f" AND trade_date >= '{start_date}'"
        if end_date:
            query += f" AND trade_date <= '{end_date}'"
            
        # === 关键修改：支持按股票代码筛选 ===
        if codes:
            # 将列表转换为 SQL 的 IN ('code1', 'code2') 格式
            code_str = "'" + "','".join(codes) + "'"
            query += f" AND ts_code IN ({code_str})"
            
        try:
            return pd.read_sql(query, self.engine)
        except Exception as e:
            print(f"SQL Error: {e}")
            return pd.DataFrame()

    def check_latest_date(self, table_name):
        try:
            with self.engine.connect() as con:
                # 检查表是否存在
                check = con.execute(text(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")).fetchone()
                if not check: return None
                
                res = con.execute(text(f"SELECT MAX(trade_date) FROM {table_name}"))
                return res.scalar()
        except:
            return None
