import pandas as pd
import time
from config import Config

class StrategyAnalyzer:
    def __init__(self, data_manager):
        self.dm = data_manager

    def run_daily_scan(self):
        print("🚀 [Strategy] 开始执行选股策略...", flush=True)
        
        trade_date = self.dm.get_trade_date()
        print(f"📅 日期: {trade_date}", flush=True)

        # 1. 筛选主线板块
        sector_df = self.dm.get_top_sectors(trade_date)
        
        if sector_df.empty:
            print("⚠️ 未获取到板块数据，尝试全市场扫描...", flush=True)
            # 如果没板块数据，就读所有股票的基础列表
            df_basic = self.dm.get_stock_basics()
            target_codes = df_basic['ts_code'].tolist() if not df_basic.empty else []
        else:
            top_sectors = sector_df.head(int(len(sector_df) * Config.SECTOR_TOP_PCT))
            print(f"🔥 主线板块: {len(top_sectors)} 个", flush=True)
            
            target_codes = set()
            for _, row in top_sectors.iterrows():
                members = self.dm.get_sector_members(row['index_code'])
                target_codes.update(members)
            target_codes = list(target_codes)
            
        print(f"🎯 待扫描股票: {len(target_codes)} 只", flush=True)
        
        if not target_codes:
            return []

        # 2. 准备基础数据
        benchmark_ret = self.dm.get_benchmark_return(trade_date)
        df_basic = self.dm.get_stock_basics()
        
        # 3. 【核心优化】分批次处理，防止爆内存
        results = []
        batch_size = 50 # 每次只从数据库读 50 只，内存占用极小
        
        print(f"💻 开始分批计算，每批 {batch_size} 只...", flush=True)

        for i in range(0, len(target_codes), batch_size):
            # 取出一小批代码
            batch_codes = target_codes[i : i + batch_size]
            
            try:
                # 📥 从数据库只读取这 50 只股票的数据
                df_daily = self.dm.get_history_batch(batch_codes, days=Config.BOX_DAYS + 20)
                df_flow = self.dm.get_moneyflow_batch(batch_codes, days=Config.FLOW_DAYS + 5)
                
                if df_daily.empty: continue

                # 开始计算这 50 只
                grouped = df_daily.groupby('ts_code')
                
                for ts_code, df in grouped:
                    try:
                        df = df.sort_values('trade_date', ascending=False).reset_index(drop=True)
                        if len(df) < Config.BOX_DAYS: continue

                        curr = df.iloc[0]
                        past = df.iloc[1:Config.BOX_DAYS+1]
                        
                        # 规则 1: 突破
                        box_high = past['high'].max()
                        if curr['close'] <= box_high * Config.BREAKOUT_THRESHOLD: continue

                        # 规则 2: 放量
                        vol_ma20 = past['vol'].head(Config.VOL_MA_DAYS).mean()
                        if vol_ma20 == 0 or curr['vol'] <= vol_ma20 * Config.VOL_MULTIPLIER: continue
                        
                        # 规则 5: RS强弱
                        past_20 = df.iloc[Config.VOL_MA_DAYS]
                        stock_ret = (curr['close'] - past_20['close']) / past_20['close']
                        if stock_ret < benchmark_ret: continue

                        # 规则 4: 资金流
                        if not df_flow.empty:
                            flow = df_flow[df_flow['ts_code'] == ts_code]
                            if len(flow) >= Config.FLOW_DAYS:
                                recent_flow = flow.sort_values('trade_date', ascending=False).head(Config.FLOW_DAYS)
                                if not (recent_flow['net_mf_amount'] > 0).all(): continue
                            else: continue

                        # 选中了
                        name = ts_code
                        if not df_basic.empty:
                            row = df_basic[df_basic['ts_code'] == ts_code]
                            if not row.empty: name = row.iloc[0]['name']
                        
                        print(f"✅ 选中: {name}", flush=True)
                        
                        results.append({
                            'ts_code': ts_code, 'name': name, 'sector': '主线',
                            'price': curr['close'], 'score': 85, 'reason': '模型筛选'
                        })

                    except Exception: continue
            
            except Exception as e:
                print(f"Batch Error: {e}", flush=True)
                continue

        print(f"🏁 扫描完成，共选中 {len(results)} 只", flush=True)
        return sorted(results, key=lambda x: x['score'], reverse=True)
