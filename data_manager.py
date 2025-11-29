import tushare as ts
import pandas as pd
import time
from datetime import datetime, timedelta
from config import Config
from db_manager import DBManager

class DataManager:
    def __init__(self):
        ts.set_token(Config.TUSHARE_TOKEN)
        self.pro = ts.pro_api()
        self.db = DBManager()

    def get_trade_date(self):
        """获取最近一个交易日"""
        today = datetime.now().strftime('%Y%m%d')
        df = self.pro.trade_cal(exchange='', start_date='20240101', end_date=today, is_open='1')
        return df['cal_date'].values[-1]

    def sync_data(self, lookback_days=60):
        """
        核心函数：同步数据
        逻辑：检查数据库最新日期 -> 如果落后 -> 补全中间日期的所有数据
        """
        print("🔄 正在检查数据同步状态...")
        
        # 1. 获取目标日期范围
        end_date = self.get_trade_date()
        
        # 检查数据库里最新的一天
        latest_in_db = self.db.check_latest_date('daily_price')
        
        if latest_in_db is None:
            # 数据库为空，初始化下载过去 N 天
            start_date = (pd.to_datetime(end_date) - timedelta(days=lookback_days)).strftime('%Y%m%d')
            print(f"⚡️ 首次初始化，准备下载自 {start_date} 以来的数据...")
        elif latest_in_db < end_date:
            # 增量更新
            start_date = (pd.to_datetime(latest_in_db) + timedelta(days=1)).strftime('%Y%m%d')
            print(f"📈 增量更新，准备下载 {start_date} -> {end_date}...")
        else:
            print("✅ 数据已是最新，无需更新。")
            return

        # 2. 获取交易日历
        cal = self.pro.trade_cal(exchange='', start_date=start_date, end_date=end_date, is_open='1')
        trade_dates = cal['cal_date'].tolist()

        if not trade_dates:
            print("✅ 没有新的交易日需要更新。")
            return

        # 3. 按日期循环下载（最高效的方式）
        for date in trade_dates:
            print(f"📥 下载数据: {date} ...")
            try:
                # A. 下载全市场日线
                df_daily = self.pro.daily(trade_date=date)
                self.db.save_data(df_daily, 'daily_price')
                
                # B. 下载全市场资金流 (高级权限)
                df_flow = self.pro.moneyflow(trade_date=date)
                self.db.save_data(df_flow, 'money_flow')
                
                # C. 稍微限流，防止触发 Tushare 频率限制
                time.sleep(0.3) 
            except Exception as e:
                print(f"❌ 同步 {date} 失败: {e}")

        # 4. 更新基础信息表 (每次覆盖即可)
        print("📥 更新股票列表...")
        df_basic = self.pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,industry,market')
        self.db.save_data(df_basic, 'stock_basic', if_exists='replace')
        
        print("🎉 数据同步完成！")

    # ============ 策略调用的接口 (只读库) ============

    def get_history_from_db(self, codes=None, days=60):
        """从数据库取历史行情"""
        # 这里简化处理：直接取最近 N 天的全量数据，在内存里 filter
        # 实际生产中可以用 SQL 筛选 codes，但 SQLite 读全量也很快
        end_date = datetime.now().strftime('%Y%m%d')
        start_date = (datetime.now() - timedelta(days=days*2)).strftime('%Y%m%d') # 多取点以防万一
        return self.db.get_data('daily_price', start_date=start_date)

    def get_moneyflow_from_db(self, days=10):
        start_date = (datetime.now() - timedelta(days=days*2)).strftime('%Y%m%d')
        return self.db.get_data('money_flow', start_date=start_date)
    
    def get_stock_basics(self):
        return self.db.get_data('stock_basic')

    # 主线板块依然需要实时请求（数据量小，且需要最新排名）
    def get_top_sectors(self, trade_date):
        try:
            sw_index = self.pro.index_classify(level='L1', src='SW2021')
            df = self.pro.sw_daily(trade_date=trade_date)
            df = df.merge(sw_index[['index_code', 'industry_name']], left_on='ts_code', right_on='index_code')
            return df.sort_values('pct_change', ascending=False)
        except:
            return pd.DataFrame()
            
    def get_sector_members(self, sector_code):
        return self.pro.index_member(index_code=sector_code)['con_code'].tolist()
        
    def get_benchmark_return(self, end_date, days=20):
        # 简单处理：实时请求一次指数即可
        start_date = (pd.to_datetime(end_date) - timedelta(days=days*2)).strftime('%Y%m%d')
        df = self.pro.index_daily(ts_code=Config.RS_BENCHMARK, start_date=start_date, end_date=end_date)
        if len(df) < days: return 0
        df = df.head(days)
        return (df.iloc[0]['close'] - df.iloc[-1]['close']) / df.iloc[-1]['close']
