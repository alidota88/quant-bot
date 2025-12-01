import tushare as ts
import pandas as pd
import time
from datetime import datetime, timedelta
from config import Config
from db_manager import DBManager

class DataManager:
    def __init__(self):
        ts.set_token(Config.TUSHARE_TOKEN)
        # ==================== 👇 关键修改 1 👇 ====================
        # 设置 120秒 超时，防止网络波动导致 Read timed out
        self.pro = ts.pro_api(timeout=120) 
        # =========================================================
        self.db = DBManager()

    def get_trade_date(self):
        """获取最近一个交易日"""
        today = datetime.now().strftime('%Y%m%d')
        # 获取最近两周的交易日历
        df = self.pro.trade_cal(exchange='', start_date=(datetime.now() - timedelta(days=15)).strftime('%Y%m%d'), end_date=today, is_open='1')
        return df['cal_date'].values[-1]

    def sync_data(self, lookback_days=60):
        """
        同步数据，并返回同步结果报告
        Returns: (success_count, fail_count, error_msg)
        """
        print("🔄 正在检查数据同步状态...")
        
        end_date = self.get_trade_date()
        latest_in_db = self.db.check_latest_date('daily_price')
        
        # 确定下载范围
        if latest_in_db is None:
            start_date = (pd.to_datetime(end_date) - timedelta(days=lookback_days)).strftime('%Y%m%d')
            print(f"⚡️ 首次初始化: {start_date} -> {end_date}")
        elif latest_in_db < end_date:
            start_date = (pd.to_datetime(latest_in_db) + timedelta(days=1)).strftime('%Y%m%d')
            print(f"📈 增量更新: {start_date} -> {end_date}")
        else:
            return 0, 0, "数据已是最新"

        # 获取交易日
        cal = self.pro.trade_cal(exchange='', start_date=start_date, end_date=end_date, is_open='1')
        trade_dates = cal['cal_date'].tolist()

        if not trade_dates:
            return 0, 0, "无新交易日"

        success_count = 0
        fail_count = 0
        last_error = ""

        # ==================== 👇 关键修改 2 👇 ====================
        # 增加失败重试机制
        for date in trade_dates:
            print(f"📥 下载: {date} ...")
            retry_times = 2 # 失败允许重试2次
            
            for i in range(retry_times):
                try:
                    # A. 日线
                    df_daily = self.pro.daily(trade_date=date)
                    self.db.save_data(df_daily, 'daily_price')
                    
                    # B. 资金流
                    df_flow = self.pro.moneyflow(trade_date=date)
                    self.db.save_data(df_flow, 'money_flow')
                    
                    # 成功！
                    success_count += 1
                    time.sleep(0.8) # 稍微休息
                    break # 跳出重试循环
                    
                except Exception as e:
                    print(f"⚠️ {date} 第{i+1}次失败: {e}")
                    if i == retry_times - 1: # 最后一次也没成功
                        fail_count += 1
                        last_error = str(e)
                    else:
                        time.sleep(3) # 失败后多休息几秒再试

        return success_count, fail_count, last_error
        # =========================================================

    # ============ 下面的代码保持不变 ============

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
