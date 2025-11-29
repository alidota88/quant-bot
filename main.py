import telebot
import time
from datetime import datetime
from config import Config
from data_manager import DataManager
from strategy import StrategyAnalyzer

# 1. 初始化 Bot
bot = telebot.TeleBot(Config.TG_BOT_TOKEN)

# 2. 初始化数据与策略模块
# DataManager 内部会自动初始化 DBManager
dm = DataManager()
strategy = StrategyAnalyzer(dm)

# ================== 权限验证 ==================
def is_authorized(message):
    """防止陌生人调用"""
    if str(message.chat.id) != Config.TG_CHAT_ID:
        bot.reply_to(message, "⛔️ 你没有权限使用此机器人。")
        return False
    return True

# ================== 指令: /start & /help ==================
@bot.message_handler(commands=['start', 'help'])
def send_welcome(message):
    if not is_authorized(message): return
    
    msg = (
        "🤖 **量化私有云 (Plan B)**\n\n"
        "👇 常用指令：\n\n"
        "🔄 `/update`\n"
        "   > **同步数据**。收盘后点一次，下载当日数据到云硬盘。\n"
        "   > 首次运行需下载60天数据，约需2-3分钟。\n\n"
        "🚀 `/scan`\n"
        "   > **极速选股**。从本地数据库扫描，秒出结果。\n\n"
        "🔍 `/check 600519.SH`\n"
        "   > **单股诊断**。实时联网检查某只股票。\n"
    )
    bot.reply_to(message, msg, parse_mode='Markdown')

# ================== 指令: /update (数据同步) ==================
@bot.message_handler(commands=['update'])
def handle_update(message):
    if not is_authorized(message): return

    bot.reply_to(message, "🔄 正在同步 Tushare 数据到 Railway 云硬盘...\n(首次运行可能需要几分钟，请耐心等待)")
    
    try:
        # 调用 DataManager 的同步逻辑
        dm.sync_data(lookback_days=Config.BOX_DAYS + 10)
        
        # 获取最新数据日期
        latest_date = dm.db.check_latest_date('daily_price')
        
        bot.reply_to(message, f"✅ **同步完成！**\n\n📅 数据库最新日期: `{latest_date}`\n现在可以使用 `/scan` 秒级选股了。", parse_mode='Markdown')
        
    except Exception as e:
        bot.reply_to(message, f"❌ 同步失败: {e}")

# ================== 指令: /scan (本地极速扫描) ==================
@bot.message_handler(commands=['scan'])
def handle_scan(message):
    if not is_authorized(message): return

    bot.reply_to(message, "⏳ 正在分析本地数据库...")
    
    try:
        # 执行策略 (读取本地 DB)
        results = strategy.run_daily_scan()
        today = datetime.now().strftime('%Y-%m-%d')
        
        if not results:
            bot.send_message(message.chat.id, f"📅 {today}\n\n本地库扫描完成，无符合条件的标的。\n\n(提示：如果今天刚收盘，请先执行 `/update`)")
        else:
            msg = f"🚀 **{today} 选股结果**\n"
            msg += f"🔥 发现 {len(results)} 只符合条件的股票：\n\n"
            
            for s in results[:10]: # 限制只发前10个
                # 修复 Markdown 格式：把乘号 * 改为 x，避免报错
                msg += f"🐂 **{s['name']}** (`{s['ts_code']}`)\n"
                msg += f"   📂 板块: {s['sector']}\n"
                msg += f"   💰 现价: `{s['price']}`\n"
                msg += f"   📊 评分: `{s['score']}`\n"
                msg += f"   📝 理由: {s['reason']}\n\n"
            
            bot.send_message(message.chat.id, msg, parse_mode='Markdown')
            
    except Exception as e:
        bot.reply_to(message, f"❌ 扫描出错: {str(e)}")

# ================== 指令: /check (实时联网诊断) ==================
@bot.message_handler(commands=['check'])
def handle_check(message):
    """
    注意：为了保证诊断的准确性，/check 指令依然走实时网络请求，
    而不是查数据库。这样即使你忘了 update 也能临时查票。
    """
    if not is_authorized(message): return

    try:
        parts = message.text.split()
        if len(parts) < 2:
            bot.reply_to(message, "❌ 格式错误，请使用: `/check 600519.SH`", parse_mode='Markdown')
            return
        ts_code = parts[1].upper()
    except IndexError:
        return

    msg_id = bot.reply_to(message, f"🔍 正在联网深度诊断 `{ts_code}` ...", parse_mode='Markdown')

    try:
        # 1. 获取最新交易日
        trade_date = dm.get_trade_date()
        
        # 2. 实时获取该股数据 (不走 DB)
        df = dm.pro.daily(ts_code=ts_code, start_date='', end_date=trade_date, limit=Config.BOX_DAYS + 20)
        
        # 3. 获取名称
        try:
            base_info = dm.pro.stock_basic(ts_code=ts_code, fields='name')
            name = base_info.iloc[0]['name'] if not base_info.empty else ts_code
        except:
            name = ts_code

        if df.empty or len(df) < Config.BOX_DAYS:
            bot.edit_message_text(f"❌ 数据不足或代码错误 `{ts_code}`", chat_id=message.chat.id, message_id=msg_id.message_id, parse_mode='Markdown')
            return

        # 4. 现场计算 (简化版逻辑)
        curr = df.iloc[0]
        past = df.iloc[1:Config.BOX_DAYS+1]
        
        # 规则1: 箱体
        box_high = past['high'].max()
        is_breakout = curr['close'] > (box_high * Config.BREAKOUT_THRESHOLD)
        
        # 规则2: 放量
        vol_ma20 = past['vol'].head(Config.VOL_MA_DAYS).mean()
        is_vol_up = curr['vol'] > (vol_ma20 * Config.VOL_MULTIPLIER)

        # 构造报告
        if is_breakout and is_vol_up:
            res_txt = (
                f"✅ **{name} ({ts_code}) 形态良好！**\n\n"
                f"💰 现价: `{curr['close']}`\n"
                f"📈 突破: 是 (箱体上沿 `{box_high}`)\n"
                f"🌊 放量: 是 (量比 `{round(curr['vol']/vol_ma20, 1)}`)\n"
                f"⚠️ *提示：请结合板块与资金流判断*"
            )
        else:
            res_txt = (
                f"❌ **{name} ({ts_code}) 不符合条件**\n\n"
                f"1. 突破箱体: {'✅' if is_breakout else '❌'}\n"
                f"   (现价 `{curr['close']}` vs 上沿 `{box_high}`)\n"
                f"2. 有效放量: {'✅' if is_vol_up else '❌'}\n"
                f"   (今日 `{curr['vol']}` vs 均量 `{int(vol_ma20)}`)"
            )
            
        bot.edit_message_text(res_txt, chat_id=message.chat.id, message_id=msg_id.message_id, parse_mode='Markdown')

    except Exception as e:
        bot.edit_message_text(f"❌ 诊断出错: {str(e)}", chat_id=message.chat.id, message_id=msg_id.message_id)

# ================== 启动主循环 ==================
if __name__ == "__main__":
    print("🤖 量化机器人 (Plan B) 已启动...")
    # 移除 webhook 确保从轮询模式开始
    bot.remove_webhook()
    # 开启长轮询
    bot.infinity_polling()
