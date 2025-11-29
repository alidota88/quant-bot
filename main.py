import telebot
import time
from datetime import datetime
from config import Config
from data_manager import DataManager
from strategy import StrategyAnalyzer

# 初始化 Bot
bot = telebot.TeleBot(Config.TG_BOT_TOKEN)

# 初始化数据和策略模块
dm = DataManager()
strategy = StrategyAnalyzer(dm)

def is_authorized(message):
    """安全检查: 防止陌生人调用你的机器人"""
    if str(message.chat.id) != Config.TG_CHAT_ID:
        bot.reply_to(message, "⛔️ 你没有权限使用此机器人。")
        return False
    return True

# ================== 指令 1: /start ==================
@bot.message_handler(commands=['start', 'help'])
def send_welcome(message):
    if not is_authorized(message): return
    
    msg = (
        "🤖 **量化交互机器人已就绪**\n\n"
        "👇你可以发送以下指令：\n\n"
        "1️⃣ `/scan`\n"
        "   > 立即扫描今日主线板块，寻找符合模型的股票。\n\n"
        "2️⃣ `/check 600519.SH`\n"
        "   > 强制按模型诊断某只具体股票。\n"
    )
    bot.reply_to(message, msg, parse_mode='Markdown')

# ================== 指令 2: /scan (立即选股) ==================
@bot.message_handler(commands=['scan'])
def handle_scan(message):
    if not is_authorized(message): return

    bot.reply_to(message, "⏳ 正在扫描主线板块与全市场，请稍候 (约需 1-2 分钟)...")
    
    try:
        # 执行策略
        results = strategy.run_daily_scan()
        today = datetime.now().strftime('%Y-%m-%d')
        
        if not results:
            bot.send_message(message.chat.id, f"📅 {today}\n\n扫描完成，今日无符合【严格条件】的标的。")
        else:
            msg = f"🚀 **{today} 实时扫描结果**\n"
            msg += f"🔥 发现 {len(results)} 只符合条件的股票：\n\n"
            
            for s in results[:10]:
                msg += f"🐂 **{s['name']}** (`{s['ts_code']}`)\n"
                msg += f"   📂 板块: {s['sector']}\n"
                msg += f"   💰 现价: {s['price']} ({s['pct_chg']}%)\n"
                msg += f"   📊 评分: {s['score']}\n"
                msg += f"   📝 理由: {s['reason']}\n\n"
            
            bot.send_message(message.chat.id, msg, parse_mode='Markdown')
            
    except Exception as e:
        bot.reply_to(message, f"❌ 运行出错: {str(e)}")

# ================== 指令 3: /check (单股诊断) ==================
@bot.message_handler(commands=['check'])
def handle_check(message):
    if not is_authorized(message): return

    try:
        parts = message.text.split()
        if len(parts) < 2:
            bot.reply_to(message, "❌ 格式错误，请使用: `/check 600519.SH`", parse_mode='Markdown')
            return
        ts_code = parts[1].upper()
    except IndexError:
        return

    # 发送一个临时消息，稍后修改它
    msg_id = bot.reply_to(message, f"🔍 正在深度诊断 `{ts_code}` ...", parse_mode='Markdown')

    try:
        # 1. 获取基本信息
        trade_date = dm.get_trade_date()
        benchmark_ret = dm.get_benchmark_return(trade_date)
        
        # 2. 借用 strategy 里的 check_stock 方法
        result = strategy.check_stock(ts_code, "手动诊断", benchmark_ret, trade_date)
        
        # 3. 获取股票名称
        try:
            base_info = dm.pro.stock_basic(ts_code=ts_code, fields='name')
            if base_info.empty:
                bot.edit_message_text(f"❌ 找不到代码 `{ts_code}`，请检查输入。", chat_id=message.chat.id, message_id=msg_id.message_id, parse_mode='Markdown')
                return
            name = base_info.iloc[0]['name']
        except:
            name = ts_code

        if result:
            # 符合模型 (注意：这里把 ** 改成了 *，这是 TG 标准加粗)
            res_txt = (
                f"✅ *{name} ({ts_code}) 符合模型！*\n\n"
                f"📊 评分: `{result['score']}`\n"
                f"💰 现价: `{result['price']}`\n"
                f"💡 理由: {result['reason']}\n"
                f"🌊 资金: 连续3日净流入"
            )
            bot.edit_message_text(res_txt, chat_id=message.chat.id, message_id=msg_id.message_id, parse_mode='Markdown')
        else:
            # 不符合模型 (注意：把 * 改成了 x，防止报错)
            fail_txt = (
                f"❌ *{name} ({ts_code}) 不符合筛选条件*\n\n"
                f"可能原因：\n"
                f"1. 未突破55日箱体\n"
                f"2. 今日未放量 (需 > MA20 x 1.5)\n" 
                f"3. 跑输沪深300指数\n"
                f"4. 主力资金未连续3日净流入"
            )
            # 这里把 * 改成了 x 1.5，同时也修正了加粗语法
            bot.edit_message_text(fail_txt, chat_id=message.chat.id, message_id=msg_id.message_id, parse_mode='Markdown')

    except Exception as e:
        # 如果出错，发送纯文本，不使用 Markdown，防止报错套报错
        print(f"Error: {e}") # 打印到日志
        bot.edit_message_text(f"❌ 诊断出错: {str(e)}", chat_id=message.chat.id, message_id=msg_id.message_id)

if __name__ == "__main__":
    print("🤖 交互式机器人已启动，正在监听 Telegram 消息...")
    # remove_webhook 确保从轮询模式开始，避免冲突
    bot.remove_webhook()
    # infinity_polling 让程序一直跑，即使网络闪断也会自动重连
    bot.infinity_polling()
