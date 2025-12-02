import pandas as pd
import time
from config import Config

class StrategyAnalyzer:
    def __init__(self, data_manager):
        self.dm = data_manager

    def run_daily_scan(self):
        print("🚀 [Strategy] 开始执行【完全体】策略...", flush=True)
        
        trade_date = self.dm.get_trade_date()
        print(f"📅 分析日期: {trade_date}", flush=True)

        # 1. 优先获取主线板块 (实时请求)
        print("🔍 正在扫描领涨板块...", flush=True)
        sector_df = self.dm.get_top_sectors(trade_date)
        
        target_codes = []
        if not sector_df.empty:
            # 取前 20% 的板块
            top_sectors = sector_df.head(int(len(sector_df) * Config.SECTOR_TOP_PCT))
            print(f"🔥 锁定主线: {len(top_sectors)} 个板块 ({top_sectors['industry_name'].tolist()[:5]}...)", flush=True)
            
            # 获取成分股
            code_set = set()
            for _, row in top_sectors.iterrows():
                members = self.dm.get_sector_members(row['index_code'])
                code_set.update(members)
            target_codes = list(code_set)
        
        # 兜底机制：如果板块数据没取到，或者太少，就扫描全市场
        if len(target_codes) < 50:
            print("⚠️ 板块数据不足，切换为【全市场扫描】模式...", flush=True)
            df_basic = self.dm.get_stock_basics()
            if not df_basic.empty:
                target_codes = df_basic['ts_code'].tolist()

        print(f"🎯 最终待扫描股票: {len(target_codes)} 只", flush=True)
        
        if not target_codes:
            print("❌ 错误: 股票列表为空，请检查 /update", flush=True)
            return []

        # 2. 准备基准数据
        benchmark_ret = self.dm.get_benchmark_return(trade_date)
        df_basic = self.dm.get_stock_basics()
        
        results = []
        batch_size = 50 # 每次处理 50 只，内存安全
        
        print(f"💻 开始计算 (共 {len(target_codes)} 只)...", flush=True)

        # 3. 分批次循环
        for i in range(0, len(target_codes), batch_size):
            batch_codes = target_codes[i : i + batch_size]
            
            try:
                # 从数据库批量读取 (History + MoneyFlow)
                df_daily = self.dm.get_history_batch(batch_codes, days=Config.BOX_DAYS + 20)
                df_flow = self.dm.get_moneyflow_batch(batch_codes, days=Config.FLOW_DAYS + 5)
                
                if df_daily.empty: continue

                # 分组计算
                grouped = df_daily.groupby('ts_code')
                
                for ts_code, df in grouped:
                    try:
                        # 按日期倒序
                        df = df.sort_values('trade_date', ascending=False).reset_index(drop=True)
                        
                        # 数据长度检查
                        if len(df) < Config.BOX_DAYS: continue

                        curr = df.iloc[0] # 今天
                        past = df.iloc[1:Config.BOX_DAYS+1] # 过去 N 天
                        
                        # === 核心策略逻辑 ===
                        
                        # 1. 突破箱体 (收盘价 > 过去55天最高价 * 1.01)
                        box_high = past['high'].max()
                        if curr['close'] <= box_high * Config.BREAKOUT_THRESHOLD: 
                            continue

                        # 2. 放量 (今日量 > 20日均量 * 1.5)
                        vol_ma20 = past['vol'].head(Config.VOL_MA_DAYS).mean()
                        if vol_ma20 == 0 or curr['vol'] <= vol_ma20 * Config.VOL_MULTIPLIER:
                            continue
                        
                        # 3. RS 相对强弱 (跑赢大盘)
                        past_20 = df.iloc[Config.VOL_MA_DAYS]
                        stock_ret = (curr['close'] - past_20['close']) / past_20['close']
                        if stock_ret < benchmark_ret:
                            continue

                        # 4. 资金流 (最近 N 天净流入 > 0)
                        # 注意：如果数据库没资金流数据，是否放行？这里选择严格模式：必须有数据
                        if df_flow.empty: continue
                        
                        flow = df_flow[df_flow['ts_code'] == ts_code]
                        if len(flow) < Config.FLOW_DAYS: continue
                        
                        # 取最近 N 天
                        recent_flow = flow.sort_values('trade_date', ascending=False).head(Config.FLOW_DAYS)
                        if not (recent_flow['net_mf_amount'] > 0).all():
                            continue

                        # === 选中了！ ===
                        
                        # 找名字
                        name = ts_code
                        if not df_basic.empty:
                            row = df_basic[df_basic['ts_code'] == ts_code]
                            if not row.empty: name = row.iloc[0]['name']

                        print(f"✅ 选中: {name} (突破+放量+资金)", flush=True)
                        
                        # 计算评分
                        score = 80
                        if curr['pct_chg'] > 5: score += 10 # 大涨加分
                        
                        results.append({
                            'ts_code': ts_code,
                            'name': name,
                            'sector': '主线优选',
                            'price': curr['close'],
                            'score': score,
                            'reason': f"突破{Config.BOX_DAYS}日新高, 量比{round(curr['vol']/vol_ma20, 1)}"
                        })

                    except Exception: continue
            
            except Exception as e:
                print(f"Batch Error: {e}", flush=True)
                continue

        print(f"🏁 扫描完成，最终选中 {len(results)} 只", flush=True)
        return sorted(results, key=lambda x: x['score'], reverse=True)
