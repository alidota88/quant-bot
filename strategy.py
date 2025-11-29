import pandas as pd
import numpy as np
from config import Config

class StrategyAnalyzer:
    def __init__(self, data_manager):
        self.dm = data_manager

    def check_stock(self, ts_code, sector_name, benchmark_ret, trade_date):
        """
        对单只股票进行全量化体检
        返回: {passed: bool, data: dict}
        """
        # 1. 获取行情数据 (包含今天)
        df = self.dm.get_stock_history(ts_code, trade_date, lookback=Config.BOX_DAYS + 10)
        if len(df) < Config.BOX_DAYS: return None # 次新股排除

        # df.iloc[0] 是今天
        curr = df.iloc[0]
        
        # ================= 规则 1: 突破箱体 =================
        # 过去 N 天 (不含今天)
        past_days = df.iloc[1:Config.BOX_DAYS+1]
        box_high = past_days['high'].max()
        
        # 突破判定：收盘价 > 箱体上沿 * 1.01
        is_breakout = curr['close'] > (box_high * Config.BREAKOUT_THRESHOLD)
        if not is_breakout: return None

        # ================= 规则 2: 放量 =================
        # 计算过去 20 天均量 (不含今天)
        vol_ma20 = past_days['vol'].head(Config.VOL_MA_DAYS).mean()
        # 放量判定：今日量 > MA20 * 1.5
        is_volume_up = curr['vol'] > (vol_ma20 * Config.VOL_MULTIPLIER)
        
        # 补充原文规则：连续3天量能 > MA20 (可选，这里先执行严格放量)
        if not is_volume_up: return None

        # ================= 规则 5: RS 相对强弱 =================
        # 个股20日涨幅
        past_20 = df.iloc[Config.VOL_MA_DAYS] # 20天前的数据点
        stock_ret = (curr['close'] - past_20['close']) / past_20['close']
        
        # 必须强于大盘 且 自身必须是涨的 (去弱留强)
        if stock_ret < benchmark_ret or stock_ret < 0: return None

        # ================= 规则 4: 资金流向 (核心) =================
        # 检查连续 3 天主力净流入 > 0
        mf = self.dm.get_money_flow(ts_code, trade_date, days=Config.FLOW_DAYS)
        if len(mf) < Config.FLOW_DAYS: return None
        
        # net_mf_amount 单位是万元
        is_money_in = (mf['net_mf_amount'] > 0).all()
        if not is_money_in: return None

        # ================= 规则 7: 评分 =================
        score = 80 # 基础分
        score += 10 if curr['pct_chg'] > 5 else 0 # 大阳线加分
        score += 10 if stock_ret > benchmark_ret * 1.5 else 0 # 超强RS加分

        return {
            'ts_code': ts_code,
            'name': '', # 稍后补充
            'sector': sector_name,
            'price': curr['close'],
            'pct_chg': curr['pct_chg'],
            'score': score,
            'reason': f"突破{Config.BOX_DAYS}日新高, 放量{round(curr['vol']/vol_ma20, 1)}倍, 主力连买3日"
        }

    def run_daily_scan(self):
        trade_date = self.dm.get_trade_date()
        print(f"🚀 开始分析交易日: {trade_date}")

        # 1. 筛选主线板块 (前 20%)
        sector_df = self.dm.get_top_sectors(trade_date)
        if sector_df.empty: return []
        
        top_count = int(len(sector_df) * Config.SECTOR_TOP_PCT)
        top_sectors = sector_df.head(top_count)
        print(f"🔥 锁定主线板块: {top_sectors['industry_name'].tolist()}")

        # 2. 获取大盘基准
        benchmark_ret = self.dm.get_benchmark_return(trade_date)

        results = []

        # 3. 遍历主线板块下的个股
        # 为了演示速度，这里每板块只取前几只，实际跑全量请去掉切片
        for _, row in top_sectors.iterrows():
            sector_name = row['industry_name']
            sector_code = row['index_code']
            
            members = self.dm.get_sector_members(sector_code)
            # 限制数量防止 Railway 超时，实际建议分批运行
            # 如果是付费 Docker，可以跑全量
            for stock in members[:30]: 
                try:
                    res = self.check_stock(stock, sector_name, benchmark_ret, trade_date)
                    if res:
                        # 补充名称
                        base_info = self.dm.pro.stock_basic(ts_code=stock, fields='name')
                        res['name'] = base_info.iloc[0]['name']
                        results.append(res)
                        print(f"✅ 选中: {res['name']}")
                except Exception as e:
                    continue
        
        return sorted(results, key=lambda x: x['score'], reverse=True)
