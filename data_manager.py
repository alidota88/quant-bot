import tushare as ts
import pandas as pd
import time
from config import Config

class DataManager:
    def __init__(self):
        ts.set_token(Config.TUSHARE_TOKEN)
        self.pro = ts.pro_api()

    def get_trade_date(self):
        """获取最近一个交易日"""
        today = pd.Timestamp.now().strftime('%Y%m%d')
        df = self.pro.trade_cal(exchange='', start_date='20240101', end_date=today, is_open='1')
        return df['cal_date'].values[-1]

    def get_top_sectors(self, trade_date):
        """
        获取申万一级行业涨幅榜
        """
        # 获取申万一级行业列表
        sw_index = self.pro.index_classify(level='L1', src='SW2021')
        codes = sw_index['index_code'].tolist()
        
        # 批量获取行业行情 (Tushare 支持批量获取 index_daily)
        # 计算过去5日涨幅，需要获取前5个交易日的数据
        # 简化逻辑：这里获取单日涨幅排行，若要精确5日需回溯
        # 2000积分可以直接调取 sw_daily (申万行业行情)
        try:
            df = self.pro.sw_daily(trade_date=trade_date)
            # 筛选一级行业 (申万代码通常是 801xxx)
            # 需要合并行业名称
            df = df.merge(sw_index[['index_code', 'industry_name']], left_on='ts_code', right_on='index_code')
            
            # 计算简单的 N日涨幅需要更多数据，这里暂用单日涨幅演示，
            # 实际生产建议拉取一段时间数据计算 pct_chg_5d
            df = df.sort_values('pct_change', ascending=False)
            return df # 返回排序后的 DataFrame
        except Exception as e:
            print(f"获取板块数据失败: {e}")
            return pd.DataFrame()

    def get_sector_members(self, sector_code):
        """获取行业成分股"""
        return self.pro.index_member(index_code=sector_code)['con_code'].tolist()

    def get_stock_history(self, ts_code, end_date, lookback=120):
        """
        获取个股历史行情 (日线 + 复权)
        lookback: 回溯天数，确保足够计算 MA120 或 Box55
        """
        start_date = (pd.to_datetime(end_date) - pd.Timedelta(days=lookback*2)).strftime('%Y%m%d')
        # 重点：adj='qfq' 前复权，技术分析必须用复权价
        df = self.pro.daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
        # 同样需要复权因子调整，Tushare pro.daily 默认不复权，需配合 adj_factor 计算
        # 或者直接用通用接口 pro.query('daily', ...) 配合 adj_factor
        # 为简化代码，这里假设你使用 tushare SDK 的通用复权接口 ts.pro_bar
        df = ts.pro_bar(ts_code=ts_code, adj='qfq', start_date=start_date, end_date=end_date, api=self.pro)
        if df is None: return pd.DataFrame()
        return df # 也就是按时间倒序：今天, 昨天...

    def get_money_flow(self, ts_code, end_date, days=3):
        """获取个股资金流向"""
        start_date = (pd.to_datetime(end_date) - pd.Timedelta(days=10)).strftime('%Y%m%d')
        df = self.pro.moneyflow(ts_code=ts_code, start_date=start_date, end_date=end_date)
        return df.head(days) # 取最近 N 天
    
    def get_benchmark_return(self, end_date, days=20):
        """获取大盘涨幅"""
        start_date = (pd.to_datetime(end_date) - pd.Timedelta(days=days*2)).strftime('%Y%m%d')
        df = self.pro.index_daily(ts_code=Config.RS_BENCHMARK, start_date=start_date, end_date=end_date)
        if len(df) < days: return 0
        df = df.head(days)
        # (最新收盘 - N天前收盘) / N天前收盘
        return (df.iloc[0]['close'] - df.iloc[-1]['close']) / df.iloc[-1]['close']
