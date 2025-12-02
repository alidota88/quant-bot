import pandas as pd
import time
from config import Config

class StrategyAnalyzer:
    def __init__(self, data_manager):
        self.dm = data_manager

    def run_daily_scan(self):
        print("🚀 [测试模式] 开始执行极简策略...", flush=True)
        
        # 1. 获取日期
        trade_date = self.dm.get_trade_date()
        print(f"📅 分析日期: {trade_date}", flush=True)

        # 2. 直接获取所有股票列表 (不走板块，防止板块接口没数据卡死)
        df_basic = self.dm.get_stock_basics()
        if df_basic.empty:
            print("⚠️ 数据库没股票列表，请先执行 /update", flush=True)
            return []
            
        # 3. 为了测试速度，只取前 200 只股票进行“体检”
        # (如果这200只能跑通，说明整个系统都没问题)
        all_codes = df_basic['ts_code'].tolist()
        target_codes = all_codes[:200] 
        print(f"🎯 本次测试扫描: {len(target_codes)} 只股票 (前200只)", flush=True)

        results = []
        batch_size = 50 # 每次处理 50 只
        
        print("💻 开始计算...", flush=True)

        # 4. 分批计算
        for i in range(0, len(target_codes), batch_size):
            batch_codes = target_codes[i : i + batch_size]
            
            try:
                # 只读取最近 20 天的数据就够算 MA5 了
                df_daily = self.dm.get_history_batch(batch_codes, days=20)
                
                if df_daily.empty: continue

                # 按股票分组
                grouped = df_daily.groupby('ts_code')
                
                for ts_code, df in grouped:
                    try:
                        # 按日期倒序
                        df = df.sort_values('trade_date', ascending=False).reset_index(drop=True)
                        
                        # 只要有 5 天数据就能算
                        if len(df) < 5: continue

                        curr = df.iloc[0] # 今天
                        
                        # === 极简规则 ===
                        # 1. 计算 5日均线
                        ma5 = df['close'].head(5).mean()
                        
                        # 2. 条件: 收盘价 > MA5 且 今天涨了
                        if curr['close'] > ma5 and curr['pct_chg'] > 0:
                            
                            # 找名字
                            name = ts_code
                            row = df_basic[df_basic['ts_code'] == ts_code]
                            if not row.empty: name = row.iloc[0]['name']
                            
                            print(f"✅ 选中: {name} (现价{curr['close']} > 均线{round(ma5,2)})", flush=True)
                            
                            results.append({
                                'ts_code': ts_code,
                                'name': name,
                                'sector': '测试',
                                'price': curr['close'],
                                'score': curr['pct_chg'], # 用涨幅当分数
                                'reason': f"站上MA5, 涨幅 {curr['pct_chg']}%"
                            })

                    except Exception: continue
            
            except Exception as e:
                print(f"Batch Error: {e}", flush=True)
                continue

        print(f"🏁 测试扫描完成，选中 {len(results)} 只", flush=True)
        # 按涨幅排序返回
        return sorted(results, key=lambda x: x['score'], reverse=True)
