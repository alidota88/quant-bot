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
        """
        获取最近一个【已收盘】的交易日
        逻辑：如果当前时间 < 16:00，则强制使用上一个交易日
        """
        now = datetime.now()
        today_str = now.strftime('%Y%m%d')
        
        # 往前推 30 天查日历
        start = (now - timedelta(days=30)).strftime('%Y%m%d')
        df = self.pro.trade_cal(exchange='', start_date=start, end_date=today_str, is_open='1')
        df = df.sort_values('cal_date')
        trade_dates = df['cal_date'].values
        
        # === 核心修复逻辑 ===
        # 如果获取到的最后一天是“今天”，但现在还没到 16:00 (收盘后数据整理时间)
        # 那么就认为是“未完结”，回退一天
        if trade_dates[-1] == today_str:
            if now.hour < 16: 
                return trade_dates[-2] # 返回倒数第二天
                
        # 否则返回最后一天
        return trade_dates[-1]

    def sync_data(self, lookback_days=60):
        print("🔄 正在检查数据同步状态...")
        
        # 这里的 get_trade_date 也会自动遵循上面的“收盘逻辑”
        # 所以如果你下午1点跑，它只会检查到昨天的数据是否同步
        end_date = self.get_trade_date()
        
        latest_in_db = self.db.check_latest_date('daily_price')
        
        if latest_in_db is None:
            start_date = (pd.to_datetime(end_date) - timedelta(days=lookback_days)).strftime('%Y%m%d')
            print(f"⚡️ 首次初始化模式: {start_date} -> {end_date}")
        elif latest_in_db < end_date:
            start_date = (pd.to_datetime(latest_in_db) + timedelta(days=1)).strftime('%Y%m%d')
            print(f"📈 增量更新模式: {start_date} -> {end_date}")
        else:
            print(f"✅ 数据已是最新 (DB: {latest_in_db} == Target: {end_date})")
            return 0, 0, f"数据已最新 ({latest_in_db})"

        # 获取交易日
        cal = self.pro.trade_cal(exchange='', start_date=start_date, end_date=end_date, is_open='1')
        cal = cal.sort_values('cal_date')
        trade_dates = cal['cal_date'].tolist()

        if not trade_dates:
            return 0, 0, f"无新交易日 ({start_date}-{end_date})"

        success_count = 0
        fail_count = 0
        last_error = ""

        for date in trade_dates:
            print(f"📥 下载全市场: {date} ...")
            retry_times = 3
            
            for i in range(retry_times):
                try:
                    # A. 日线
                    df_daily = self.pro.daily(trade_date=date)
                    print(f"   -> 日线: {len(df_daily)} 行")
                    self.db.save_data(df_daily, 'daily_price')
                    
                    # B. 资金流
                    df_flow = self.pro.moneyflow(trade_date=date)
                    self.db.save_data(df_flow, 'money_flow')
                    
                    success_count += 1
                    time.sleep(1.0)
                    break 
                    
                except Exception as e:
                    print(f"⚠️ {date} 重试 {i+1}/{retry_times}: {e}")
                    if i == retry_times - 1:
                        fail_count += 1
                        last_error = str(e)
                    else:
                        time.sleep(5)

        # 更新列表
        try:
            df_basic = self.pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,industry,market')
            self.db.save_data(df_basic, 'stock_basic', if_exists='replace')
        except: pass
            
        return success_count, fail_count, last_error

    # ============ 其他接口保持不变 ============
    
    def get_history_batch(self, codes, days=60):
        start_date = (datetime.now() - timedelta(days=days*2)).strftime('%Y%m%d')
        return self.db.get_data('daily_price', start_date=start_date, codes=codes)

    def get_moneyflow_batch(self, codes, days=10):
        start_date = (datetime.now() - timedelta(days=days*2)).strftime('%Y%m%d')
        return self.db.get_data('money_flow', start_date=start_date, codes=codes)
    
    def get_history_from_db(self, days=60):
        start_date = (datetime.now() - timedelta(days=days*2)).strftime('%Y%m%d')
        return self.db.get_data('daily_price', start_date=start_date)

    def get_moneyflow_from_db(self, days=10):
        start_date = (datetime.now() - timedelta(days=days*2)).strftime('%Y%m%d')
        return self.db.get_data('money_flow', start_date=start_date)
    
    def get_stock_basics(self):
        return self.db.get_data('stock_basic')

    def get_top_sectors(self, trade_date):
        try:
            sw_index = self.pro.index_classify(level='L1', src='SW2021')
            df = self.pro.sw_daily(trade_date=trade_date)
            if df.empty: return pd.DataFrame()
            df = df.merge(sw_index[['index_code', 'industry_name']], left_on='ts_code', right_on='index_code')
            return df.sort_values('pct_change', ascending=False)
        except:
            return pd.DataFrame()
            
    def get_sector_members(self, sector_code):
        return self.pro.index_member(index_code=sector_code)['con_code'].tolist()
        
    def get_benchmark_return(self, end_date, days=20):
        start_date = (pd.to_datetime(end_date) - timedelta(days=days*2)).strftime('%Y%m%d')
        df = self.pro.index_daily(ts_code=Config.RS_BENCHMARK, start_date=start_date, end_date=end_date)
        if len(df) < days: return 0
        df = df.head(days)
        return (df.iloc[0]['close'] - df.iloc[-1]['close']) / df.iloc[-1]['close']
