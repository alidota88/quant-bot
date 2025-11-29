import pandas as pd
from config import Config

class StrategyAnalyzer:
    def __init__(self, data_manager):
        self.dm = data_manager

    def run_daily_scan(self):
        # 1. 确保数据已同步
        # 注意：这里最好由外部控制同步，策略只负责算。但为了保险，可以检查一下。
        # self.dm.sync_data() <--- 移到 main.py 的 /scan 指令里去
        
        trade_date = self.dm.get_trade_date()
        print(f"🚀 开始本地计算，日期: {trade_date}")

        # 2. 筛选主线板块 (实时)
        sector_df = self.dm.get_top_sectors(trade_date)
        if sector_df.empty: return []
        top_sectors = sector_df.head(int(len(sector_df) * Config.SECTOR_TOP_PCT))
        
        # 获取所有主线板块的股票池
        target_codes = set()
        for _, row in top_sectors.iterrows():
            members = self.dm.get_sector_members(row['index_code'])
            target_codes.update(members)
        
        if not target_codes: return []
        print(f"🔥 主线股票池: {len(target_codes)} 只")

        # 3. 一次性从数据库读取所需数据 (内存计算)
        print("💾 正在读取本地数据库...")
        df_daily = self.dm.get_history_from_db(days=Config.BOX_DAYS + 20)
        df_flow = self.dm.get_moneyflow_from_db(days=Config.FLOW_DAYS + 5)
        df_basic = self.dm.get_stock_basics()
        benchmark_ret = self.dm.get_benchmark_return(trade_date)

        if df_daily.empty:
            print("⚠️ 数据库为空，请先执行数据同步！")
            return []

        # 优化：只保留主线股票的数据
        df_daily = df_daily[df_daily['ts_code'].isin(target_codes)]
        
        # 4. 开始遍历计算
        results = []
        grouped = df_daily.groupby('ts_code')

        for ts_code, df in grouped:
            try:
                # 按日期倒序
                df = df.sort_values('trade_date', ascending=False).reset_index(drop=True)
                if len(df) < Config.BOX_DAYS: continue
                
                # 规则 1: 突破箱体
                curr = df.iloc[0]
                past = df.iloc[1:Config.BOX_DAYS+1]
                if curr['close'] <= past['high'].max() * Config.BREAKOUT_THRESHOLD: continue

                # 规则 2: 放量
                vol_ma20 = past['vol'].head(Config.VOL_MA_DAYS).mean()
                if vol_ma20 == 0 or curr['vol'] <= vol_ma20 * Config.VOL_MULTIPLIER: continue
                
                # 规则 5: RS强弱
                past_20 = df.iloc[Config.VOL_MA_DAYS]
                stock_ret = (curr['close'] - past_20['close']) / past_20['close']
                if stock_ret < benchmark_ret: continue

                # 规则 4: 资金流
                if not df_flow.empty:
                    flow = df_flow[df_flow['ts_code'] == ts_code].sort_values('trade_date', ascending=False)
                    if len(flow) < Config.FLOW_DAYS: continue
                    if not (flow.head(Config.FLOW_DAYS)['net_mf_amount'] > 0).all(): continue

                # 选中
                stock_name = ''
                if not df_basic.empty:
                    name_row = df_basic[df_basic['ts_code'] == ts_code]
                    if not name_row.empty:
                        stock_name = name_row.iloc[0]['name']

                results.append({
                    'ts_code': ts_code,
                    'name': stock_name,
                    'sector': '主线', 
                    'price': curr['close'],
                    'pct_chg': 0, # 这里需要计算一下
                    'score': 80,
                    'reason': '本地数据库筛选'
                })

            except Exception as e:
                continue
        
        return sorted(results, key=lambda x: x['score'], reverse=True)
