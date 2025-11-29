import schedule
import time
from data_manager import DataManager
from strategy import StrategyAnalyzer
from notification import TelegramBot
from datetime import datetime

def job():
    print(f"⏰ 定时任务启动 - {datetime.now()}")
    
    # 初始化各个模块
    dm = DataManager()
    strategy = StrategyAnalyzer(dm)
    bot = TelegramBot()

    # 执行策略
    selected_stocks = strategy.run_daily_scan()
    
    # 发送通知
    today = datetime.now().strftime('%Y-%m-%d')
    bot.send_report(selected_stocks, today)
    print("✅ 任务完成")

if __name__ == "__main__":
    # Railway 部署逻辑
    # 注意：Railway 默认为 UTC 时间
    # UTC 07:30 = 北京时间 15:30 (收盘后)
    schedule.every().day.at("07:30").do(job)
    
    print("🤖 量化机器人已启动，等待执行...")
    
    # 首次启动如果有必要，可以取消下面这行的注释进行一次立即测试
    # job()

    while True:
        schedule.run_pending()
        time.sleep(60)
