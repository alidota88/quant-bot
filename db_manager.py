import os
from sqlalchemy import create_engine, text
import pandas as pd

class DBManager:
    def __init__(self, db_path='/app/data/quant.db'):
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        # 初始化数据库引擎
        self.engine = create_engine(f'sqlite:///{db_path}')
        
    def save_data(self, df, table_name, if_exists='append'):
        """保存数据到数据库"""
        if df.empty: return
        try:
            # 这里的 to_sql Pandas 会自动处理连接，通常没问题
            # 如果报错，也可以改为 with self.engine.connect() as conn: ...
            df.to_sql(table_name, self.engine, if_exists=if_exists, index=False)
        except Exception as e:
            print(f"❌ 保存 {table_name} 失败: {e}")

    def get_data(self, table_name, start_date=None, end_date=None, codes=None):
        """
        【关键修复】读取数据
        修复 'OptionEngine object has no attribute execute' 错误
        """
        query = f"SELECT * FROM {table_name} WHERE 1=1"
        
        if start_date:
            query += f" AND trade_date >= '{start_date}'"
        if end_date:
            query += f" AND trade_date <= '{end_date}'"
            
        if codes:
            # 将列表转换为 SQL 的 IN ('code1', 'code2') 格式
            code_str = "'" + "','".join(codes) + "'"
            query += f" AND ts_code IN ({code_str})"
            
        try:
            # === 修改点开始 ===
            # SQLAlchemy 2.0 必须显式建立连接
            with self.engine.connect() as conn:
                return pd.read_sql(text(query), conn)
            # === 修改点结束 ===
        except Exception as e:
            print(f"SQL Error: {e}")
            return pd.DataFrame()

    def check_latest_date(self, table_name):
        """检查最新日期"""
        try:
            # === 修改点开始 ===
            with self.engine.connect() as con:
                # 先检查表是否存在
                check = con.execute(text(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")).fetchone()
                if not check: return None
                
                # 再查日期
                res = con.execute(text(f"SELECT MAX(trade_date) FROM {table_name}"))
                return res.scalar()
            # === 修改点结束 ===
        except Exception as e:
            # 打印错误方便调试
            print(f"Check Date Error: {e}")
            return None
