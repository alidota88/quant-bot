import os

class Config:
    # 环境变量 (在 Railway 中设置)
    TUSHARE_TOKEN = os.getenv('TUSHARE_TOKEN')
    TG_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
    TG_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')

    # 策略参数
    BOX_DAYS = 55           # 箱体周期 (规则1)
    BREAKOUT_THRESHOLD = 1.01 # 突破幅度 (规则1)
    VOL_MA_DAYS = 20        # 量能均线周期 (规则2)
    VOL_MULTIPLIER = 1.5    # 放量倍数 (规则2)
    FLOW_DAYS = 3           # 资金连买天数 (规则4)
    SECTOR_TOP_PCT = 0.2    # 板块前 20% (规则3)
    RS_BENCHMARK = '000300.SH' # RS对比基准 (沪深300)
    
    # 调试模式 (True时会打印更多日志)
    DEBUG = True
