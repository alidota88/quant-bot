import telebot
import os
from datetime import datetime
from sqlalchemy import text
from config import Config
from data_manager import DataManager
from strategy import StrategyAnalyzer

# 1. 初始化 Bot
bot = telebot.TeleBot(Config.TG_BOT_TOKEN)

# 2. 初始化模块
dm = DataManager()
strategy = StrategyAnalyzer(dm)

def is_authorized(message):
    if str(message.chat.id) != Config.TG_CHAT_ID:
        bot.reply_to(message, "⛔️ 无权访问")
        return False
    return True

@bot.message_handler(commands=['start', 'help'])
def send_welcome(message):
    if not is_authorized(message): return
    msg = (
        "🤖 **量化私有云 (专业版)**\n\n"
        "1️⃣ **第一步**：发送 `/reset`\n"
        "   (清除之前的错误数据)\n\n"
        "2️⃣ **第二步**：发送 `/update`\n"
        "   (下载最近60天数据，约需2分钟)\n\n"
        "3️⃣ **第三步**：发送 `/scan`\n"
        "   (极速选股，秒出结果)\n\n"
        "🔍 `/info` - 查看数据库健康状态\n"
        "🔍 `/check 600519.SH` - 实时诊断单股"
    )
    bot.reply_to(message, msg, parse_mode='Markdown')

# ================== 核心修复指令: /reset ==================
@bot.message_handler(commands=['reset'])
def handle_reset(message):
    if not is_authorized(message): return
    
    bot.reply_to(message, "⚠️ 正在重置系统... (删除脏数据)")
    db_path = '/app/data/quant.db'
    
    try:
        # 1. 物理删除文件
        if os.path.exists(db_path):
            os.remove(db_path)
            bot.send_message(message.chat.id, "🗑️ 旧数据库文件已删除。")
        
        # 2. 重新初始化内存中的对象
        global dm, strategy
        dm = DataManager()
        strategy = StrategyAnalyzer(dm)
        
        bot.send_message(message.chat.id, "✅ **重置成功！**\n请立即发送 `/update` 重新下载最近 60 天的数据。", parse_mode='Markdown')
    except Exception as e:
        bot.reply_to(message, f"❌ 重置失败: {e}")

# ================== 查看状态: /info ==================
@bot.message_handler(commands=['info'])
def handle_info(message):
    if not is_authorized(message): return
    bot.reply_to(message, "🔍 正在读取数据库概况...")
    try:
        with dm.db.engine.connect() as con:
            count = con.execute(text("SELECT count(*) FROM daily_price")).scalar()
            dates = con.execute(text("SELECT min(trade_date), max(trade_date) FROM daily_price")).fetchone()
            
        min_date, max_date = dates if dates else ('无', '无')
        msg = (
            f"📊 **数据库状态**\n"
            f"------------------\n"
            f"📅 日期范围: `{min_date}` -> `{max_date}`\n"
            f"🔢 总数据量: `{count}` 行\n\n"
            f"💡 *正确状态*: 开始日期应为2025年9月左右，结束日期应为最新交易日。"
        )
        bot.reply_to(message, msg, parse_mode='Markdown')
    except Exception as e:
        bot.reply_to(message, f"❌ 查询失败(可能是空库): {e}")

# ================== 数据同步: /update ==================
# 在 main.py 中找到这个函数并替换
@bot.message_handler(commands=['update'])
def handle_update(message):
    if not is_authorized(message): return

    bot.reply_to(message, "🔄 开始同步... (已开启网络增强模式，超时设置为120秒)")
    
    try:
        # 接收三个返回值
        success, fail, err = dm.sync_data(lookback_days=Config.BOX_DAYS + 10)
        
        # 获取最新日期
        latest_date = dm.db.check_latest_date('daily_price')
        
        # 构造详细报告
        msg = f"✅ **同步流程结束**\n\n"
        msg += f"📅 数据库最新日期: `{latest_date}`\n"
        msg += f"📥 成功下载: `{success}` 天\n"
        
        if fail > 0:
             msg += f"❌ **失败天数**: `{fail}` 天\n"
             msg += f"⚠️ 错误原因: `{err}`\n"
             msg += "建议：请稍后再次执行 `/update` 补全缺失数据。"
        else:
             msg += "🎉 所有数据已是最新！\n快去试试 `/scan` 吧！"

        bot.reply_to(message, msg, parse_mode='Markdown')
        
    except Exception as e:
        bot.reply_to(message, f"❌ 严重错误: {e}")

# ================== 选股: /scan (这里补回来了！) ==================
@bot.message_handler(commands=['scan'])
def handle_scan(message):
    if not is_authorized(message): return
    
    # 1. 马上回复，证明Bot活着
    bot.reply_to(message, "⏳ 正在分析数据库，请稍候...")
    
    try:
        # 执行策略
        results = strategy.run_daily_scan()
        
        if not results:
            bot.send_message(message.chat.id, "📅 扫描完成，今日无符合模型的标的。")
        else:
            msg = f"🚀 **选股结果** ({len(results)}只)\n\n"
            # 只发前10个，防止消息过长发送失败
            for s in results[:10]:
                msg += f"🐂 **{s['name']}** (`{s['ts_code']}`)\n"
                msg += f"   现价: `{s['price']}`\n"
                msg += f"   理由: {s['reason']}\n\n"
            bot.send_message(message.chat.id, msg, parse_mode='Markdown')
            
    except Exception as e:
        # 捕捉所有错误并发送，而不是沉默
        bot.send_message(message.chat.id, f"❌ 扫描过程崩溃: {str(e)}")
        
# ================== 诊断: /check ==================
@bot.message_handler(commands=['check'])
def handle_check(message):
    if not is_authorized(message): return
    try:
        code = message.text.split()[1].upper()
    except:
        return
    
    bot.reply_to(message, f"🔍 正在联网诊断 `{code}` ...", parse_mode='Markdown')
    try:
        # 实时联网获取最近数据
        trade_date = dm.get_trade_date()
        df = dm.pro.daily(ts_code=code, end_date=trade_date, limit=Config.BOX_DAYS+10)
        
        if df.empty:
            bot.send_message(message.chat.id, "❌ 未获取到数据")
            return

        curr = df.iloc[0]
        past = df.iloc[1:Config.BOX_DAYS+1]
        
        box_high = past['high'].max()
        vol_ma20 = past['vol'].head(20).mean()
        
        is_breakout = curr['close'] > box_high * 1.01
        is_vol = curr['vol'] > vol_ma20 * 1.5
        
        res = (
            f"📊 **{code} 诊断结果**\n"
            f"现价: `{curr['close']}`\n"
            f"------------------\n"
            f"1. 突破箱体: {'✅' if is_breakout else '❌'}\n"
            f"   (上沿 `{box_high}`)\n"
            f"2. 有效放量: {'✅' if is_vol else '❌'}\n"
            f"   (量比 `{round(curr['vol']/vol_ma20, 1)}`)"
        )
        bot.send_message(message.chat.id, res, parse_mode='Markdown')
    except Exception as e:
        bot.send_message(message.chat.id, f"Error: {e}")

if __name__ == "__main__":
    bot.remove_webhook()
    bot.infinity_polling()
