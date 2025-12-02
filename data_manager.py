import tushare as ts
import pandas as pd
import time
from datetime import datetime, timedelta
from config import Config
from db_manager import DBManager

class DataManager:
    def __init__(self):
        ts.set_token(Config.TUSHARE_TOKEN)
        self.pro = ts.pro_api(timeout=120) 
        self.db = DBManager()

    def get_trade_date(self):
        today = datetime.now().strftime('%Y%m%d')
        start = (datetime.now() - timedelta(days=30)).strftime('%Y%m%d')
        df = self.pro.trade_cal(exchange='', start_date=start, end_date=today, is_open='1')
        df = df.sort_values('cal_date')
        return df['cal_date'].values[-1]

    def sync_data(self, lookback_days=60):
        # ... (sync_data 代码保持不变，不需要改动，这里省略以节省篇幅) ...
        # ... 请保留你原本那个好用的 sync_data ...
        print("🔄 正在检查数据同步状态...")
        end_date = self.get_trade_date()
        latest_in_db = self.db.check_latest_date('daily_price')
        
        if latest_in_db is None:
            start_date = (pd.to_datetime(end_date) - timedelta(days=lookback_days)).strftime('%Y%m%d')
            print(f"⚡️ 首次初始化: {start_date} -> {end_date}")
        elif latest_in_db < end_date:
            start_date = (pd.to_datetime(latest_in_db) + timedelta(days=1)).strftime('%Y%m%d')
            print(f"📈 增量更新: {start_date} -> {end_date}")
        else:
            return 0, 0, f"数据已最新 ({latest_in_db})"

        cal = self.pro.trade_cal(exchange='', start_date=start_date, end_date=end_date, is_open='1')
        cal = cal.sort_values('cal_date')
        trade_dates = cal['cal_date'].tolist()

        if not trade_dates: return 0, 0, "无新交易日"

        success_count = 0; fail_count = 0; last_error = ""

        for date in trade_dates:
            print(f"📥 下载全市场: {date} ...")
            for i in range(3):
                try:
                    df_daily = self.pro.daily(trade_date=date)
                    self.db.save_data(df_daily, 'daily_price')
                    df_flow = self.pro.moneyflow(trade_date=date)
                    self.db.save_data(df_flow, 'money_flow')
                    success_count += 1; time.sleep(1); break 
                except Exception as e:
                    if i == 2: fail_count += 1; last_error = str(e)
                    else: time.sleep(5)
        
        try:
            df_basic = self.pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,industry,market')
            self.db.save_data(df_basic, 'stock_basic', if_exists='replace')
        except: pass
            
        return success_count, fail_count, last_error

    # ============ 👇 关键修改在这里 👇 ============
    
    def get_history_batch(self, codes, days=60):
        """批量获取指定股票的历史行情"""
        start_date = (datetime.now() - timedelta(days=days*2)).strftime('%Y%m%d')
        # 传 codes 给 db_manager
        return self.db.get_data('daily_price', start_date=start_date, codes=codes)

    def get_moneyflow_batch(self, codes, days=10):
        """批量获取指定股票的资金流"""
        start_date = (datetime.now() - timedelta(days=days*2)).strftime('%Y%m%d')
        return self.db.get_data('money_flow', start_date=start_date, codes=codes)
    
    # ... 其他 get_top_sectors 等函数保持不变 ...
    def get_stock_basics(self): return self.db.get_data('stock_basic')
    def get_top_sectors(self, trade_date):
        try:
            sw_index = self.pro.index_classify(level='L1', src='SW2021')
            df = self.pro.sw_daily(trade_date=trade_date)
            if df.empty: return pd.DataFrame()
            df = df.merge(sw_index[['index_code', 'industry_name']], left_on='ts_code', right_on='index_code')
            return df.sort_values('pct_change', ascending=False)
        except: return pd.DataFrame()
    def get_sector_members(self, sector_code): return self.pro.index_member(index_code=sector_code)['con_code'].tolist()
    def get_benchmark_return(self, end_date, days=20):
        start_date = (pd.to_datetime(end_date) - timedelta(days=days*2)).strftime('%Y%m%d')
        df = self.pro.index_daily(ts_code=Config.RS_BENCHMARK, start_date=start_date, end_date=end_date)
        if len(df) < days: return 0
        df = df.head(days)
        return (df.iloc[0]['close'] - df.iloc[-1]['close']) / df.iloc[-1]['close']
