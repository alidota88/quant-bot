import os
from sqlalchemy import create_engine, text, inspect
import pandas as pd

class DBManager:
    def __init__(self, db_path='/app/data/quant.db'):
        # 确保目录存在
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        # 创建连接引擎
        self.engine = create_engine(f'sqlite:///{db_path}')
        
    def init_db(self):
        """初始化数据库结构"""
        # SQLite 会在第一次连接时自动创建文件，不需要手动 CREATE TABLE
        # 我们可以用 pandas 的 to_sql 自动建表
        pass

    def save_data(self, df, table_name, if_exists='append'):
        """保存 DataFrame 到数据库"""
        if df.empty: return
        try:
            # 使用 pandas 的 to_sql，极其方便
            # index=False 表示不把 index 存为单独一列
            df.to_sql(table_name, self.engine, if_exists=if_exists, index=False)
        except Exception as e:
            print(f"❌ 保存 {table_name} 失败: {e}")

    def get_data(self, table_name, trade_date=None, start_date=None, end_date=None, codes=None):
        """从数据库读取数据"""
        query = f"SELECT * FROM {table_name} WHERE 1=1"
        params = {}
        
        if trade_date:
            query += " AND trade_date = :trade_date"
            params['trade_date'] = trade_date
        
        if start_date:
            query += " AND trade_date >= :start_date"
            params['start_date'] = start_date

        if end_date:
            query += " AND trade_date <= :end_date"
            params['end_date'] = end_date
            
        if codes:
            # SQLite 的 IN 查询比较麻烦，这里简化处理，或者在 Python 端过滤
            # 为了效率，如果 code 很多，建议全量读出来再 filter
            pass

        try:
            return pd.read_sql(query, self.engine, params=params)
        except Exception as e:
            # 表可能还不存在
            return pd.DataFrame()

    def check_latest_date(self, table_name):
        """查询数据库中最新的日期"""
        try:
            inspector = inspect(self.engine)
            if not inspector.has_table(table_name):
                return None
            
            with self.engine.connect() as con:
                res = con.execute(text(f"SELECT MAX(trade_date) FROM {table_name}"))
                return res.scalar()
        except:
            return None
