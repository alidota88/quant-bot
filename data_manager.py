import tushare as ts
import pandas as pd
import time
from datetime import datetime, timedelta
from config import Config
from db_manager import DBManager

class DataManager:
    def __init__(self):
        ts.set_token(Config.TUSHARE_TOKEN)
        # 设置 120秒 超时，防止网络拥堵导致 Read timed out
        self.pro = ts.pro_api(timeout=120) 
        self.db = DBManager()

    def get_trade_date(self):
        """
        获取全市场最近的一个交易日
        修复：强制排序，防止 Tushare 返回乱序日期导致 Bot 误判
        """
        today = datetime.now().strftime('%Y%m%d')
        # 往前推 30 天，确保能跨过长假
        start = (datetime.now() - timedelta(days=30)).strftime('%Y%m%d')
        
        # exchange='' 表示不分市场，获取所有交易所的日历
        df = self.pro.trade_cal(exchange='', start_date=start, end_date=today, is_open='1')
        
        # 【核心修复】强制按日期排序 (Ascending)
        # 这样 values[-1] 拿到的永远是时间轴上最晚的一天
        df = df.sort_values('cal_date')
        
        return df['cal_date'].values[-1]

    def sync_data(self, lookback_days=60):
        """
        同步全市场数据 (主板 + 创业板 + 科创板)
        """
        print("🔄 正在计算同步范围...")
        
        # 1. 拿到真正的最新交易日
        end_date = self.get_trade_date()
        
        # 2. 检查数据库里的进度
        latest_in_db = self.db.check_latest_date('daily_price')
        
        if latest_in_db is None:
            # 首次运行，回溯 N 天
            start_date = (pd.to_datetime(end_date) - timedelta(days=lookback_days)).strftime('%Y%m%d')
            print(f"⚡️ 首次初始化模式: {start_date} -> {end_date}")
        elif latest_in_db < end_date:
            # 增量更新：从数据库断点的下一天开始
            start_date = (pd.to_datetime(latest_in_db) + timedelta(days=1)).strftime('%Y%m%d')
            print(f"📈 增量更新模式: {start_date} -> {end_date}")
        else:
            # 这里打印出来，让你确认日期是对的
            print(f"✅ 数据已是最新 (DB: {latest_in_db} == Now: {end_date})")
            return 0, 0, f"数据已最新 ({latest_in_db})"

        # 3. 获取期间的交易日列表
        cal = self.pro.trade_cal(exchange='', start_date=start_date, end_date=end_date, is_open='1')
        cal = cal.sort_values('cal_date') # 再次保险排序
        trade_dates = cal['cal_date'].tolist()

        if not trade_dates:
            return 0, 0, f"无新交易日 ({start_date}-{end_date})"

        success_count = 0
        fail_count = 0
        last_error = ""

        # 4. 循环下载
        for date in trade_dates:
            print(f"📥 下载全市场: {date} ...")
            retry_times = 3
            
            for i in range(retry_times):
                try:
                    # A. 下载日线 (exchange='' 默认包含 00/60/30/68 所有股票)
                    df_daily = self.pro.daily(trade_date=date)
                    
                    # 【调试】打印下载行数，确保包含了 5000+ 只股票
                    print(f"   -> 日线: {len(df_daily)} 行 (含主板/创业板)")
                    self.db.save_data(df_daily, 'daily_price')
                    
                    # B. 下载资金流
                    df_flow = self.pro.moneyflow(trade_date=date)
                    self.db.save_data(df_flow, 'money_flow')
                    
                    success_count += 1
                    time.sleep(1.0) # 稳健延时
                    break 
                    
                except Exception as e:
                    print(f"⚠️ {date} 重试 {i+1}/{retry_times}: {e}")
                    if i == retry_times - 1:
                        fail_count += 1
                        last_error = str(e)
                    else:
                        time.sleep(5)

        # 5. 更新股票列表 (确保 300xxx 在库里)
        # list_status='L' 表示只取上市的，exchange='' 表示全市场
        print("📥 更新股票基础列表 (含创业板)...")
        try:
            df_basic = self.pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,industry,market')
            self.db.save_data(df_basic, 'stock_basic', if_exists='replace')
            print(f"   -> 股票列表更新完毕: 共 {len(df_basic)} 只")
        except Exception as e:
            print(f"❌ 股票列表更新失败: {e}")
            
        return success_count, fail_count, last_error

    # ============ 数据读取接口 ============
    
    def get_history_from_db(self, days=60):
        start_date = (datetime.now() - timedelta(days=days*2)).strftime('%Y%m%d')
        return self.db.get_data('daily_price', start_date=start_date)

    def get_moneyflow_from_db(self, days=10):
        start_date = (datetime.now() - timedelta(days=days*2)).strftime('%Y%m%d')
        return self.db.get_data('money_flow', start_date=start_date)
    
    def get_stock_basics(self):
        return self.db.get_data('stock_basic')

    def get_top_sectors(self, trade_date):
        """获取主线板块"""
        try:
            sw_index = self.pro.index_classify(level='L1', src='SW2021')
            df = self.pro.sw_daily(trade_date=trade_date)
            # 过滤掉空的板块数据
            if df.empty: return pd.DataFrame()
            df = df.merge(sw_index[['index_code', 'industry_name']], left_on='ts_code', right_on='index_code')
            return df.sort_values('pct_change', ascending=False)
        except:
            return pd.DataFrame()
            
    def get_sector_members(self, sector_code):
        """获取板块成分股"""
        return self.pro.index_member(index_code=sector_code)['con_code'].tolist()
        
    def get_benchmark_return(self, end_date, days=20):
        """获取基准收益"""
        start_date = (pd.to_datetime(end_date) - timedelta(days=days*2)).strftime('%Y%m%d')
        df = self.pro.index_daily(ts_code=Config.RS_BENCHMARK, start_date=start_date, end_date=end_date)
        if len(df) < days: return 0
        df = df.head(days)
        return (df.iloc[0]['close'] - df.iloc[-1]['close']) / df.iloc[-1]['close']
