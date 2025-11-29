import tushare as ts
import pandas as pd
import time
from datetime import datetime, timedelta
from config import Config
from db_manager import DBManager

class DataManager:
    def __init__(self):
        # 初始化 Tushare
        ts.set_token(Config.TUSHARE_TOKEN)
        self.pro = ts.pro_api()
        # 初始化数据库连接
        self.db = DBManager()

    def get_trade_date(self):
        """获取最近一个交易日"""
        today = datetime.now().strftime('%Y%m%d')
        # 获取最近两周的交易日历，防止长假期间取不到数据
        df = self.pro.trade_cal(exchange='', start_date=(datetime.now() - timedelta(days=15)).strftime('%Y%m%d'), end_date=today, is_open='1')
        return df['cal_date'].values[-1]

    def sync_data(self, lookback_days=60):
        """
        核心函数：同步数据到本地数据库
        """
        print("🔄 正在检查数据同步状态...")
        
        # 1. 确定时间范围
        end_date = self.get_trade_date()
        latest_in_db = self.db.check_latest_date('daily_price')
        
        if latest_in_db is None:
            # === 关键修正 ===
            # 只有数据库彻底为空时，才回溯 60 天
            start_date = (pd.to_datetime(end_date) - timedelta(days=lookback_days)).strftime('%Y%m%d')
            print(f"⚡️ 首次初始化，下载范围: {start_date} -> {end_date}")
        elif latest_in_db < end_date:
            # 增量更新：从数据库最新日期的下一天开始
            start_date = (pd.to_datetime(latest_in_db) + timedelta(days=1)).strftime('%Y%m%d')
            print(f"📈 增量更新，下载范围: {start_date} -> {end_date}")
        else:
            print("✅ 数据已是最新，无需更新。")
            return

        # 2. 获取期间的所有交易日
        cal = self.pro.trade_cal(exchange='', start_date=start_date, end_date=end_date, is_open='1')
        trade_dates = cal['cal_date'].tolist()

        if not trade_dates:
            print("✅ 没有新的交易日需要更新。")
            return

        # 3. 循环下载并入库
        for date in trade_dates:
            print(f"📥 正在下载: {date} ...")
            try:
                # A. 下载全市场日线
                df_daily = self.pro.daily(trade_date=date)
                self.db.save_data(df_daily, 'daily_price')
                
                # B. 下载全市场资金流 (高级接口)
                df_flow = self.pro.moneyflow(trade_date=date)
                self.db.save_data(df_flow, 'money_flow')
                
                # === 关键修正: 增加延时防止封锁 ===
                # 每次请求后暂停 0.8 秒，确保每分钟请求数在 Tushare 限制内
                time.sleep(0.8) 
                
            except Exception as e:
                print(f"❌ 同步 {date} 失败: {e}")

        # 4. 最后更新基础信息表 (覆盖旧的)
        print("📥 更新股票基础列表...")
        try:
            df_basic = self.pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,industry,market')
            self.db.save_data(df_basic, 'stock_basic', if_exists='replace')
        except Exception as e:
            print(f"❌ 股票列表更新失败: {e}")
        
        print("🎉 数据同步完成！")

    # ============ 数据读取接口 (供策略使用) ============

    def get_history_from_db(self, days=60):
        """从数据库读取最近N天的日线"""
        start_date = (datetime.now() - timedelta(days=days*2)).strftime('%Y%m%d')
        return self.db.get_data('daily_price', start_date=start_date)

    def get_moneyflow_from_db(self, days=10):
        """从数据库读取最近N天的资金流"""
        start_date = (datetime.now() - timedelta(days=days*2)).strftime('%Y%m%d')
        return self.db.get_data('money_flow', start_date=start_date)
    
    def get_stock_basics(self):
        return self.db.get_data('stock_basic')

    # 主线板块依然走实时请求 (数据量小，且需最新排名)
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
        # 实时请求大盘指数
        start_date = (pd.to_datetime(end_date) - timedelta(days=days*2)).strftime('%Y%m%d')
        df = self.pro.index_daily(ts_code=Config.RS_BENCHMARK, start_date=start_date, end_date=end_date)
        if len(df) < days: return 0
        df = df.head(days)
        return (df.iloc[0]['close'] - df.iloc[-1]['close']) / df.iloc[-1]['close']
