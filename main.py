# main.py
import os
import time
import telebot
from flask import Flask, request, abort
from sqlalchemy import text
from config import Config
from data_manager import DataManager
from strategy import StrategyAnalyzer

# ==================== 初始化 Flask 和 Bot ====================
app = Flask(__name__)
bot = telebot.TeleBot(Config.TG_BOT_TOKEN)

# 初始化数据和策略模块
dm = DataManager()
strategy = StrategyAnalyzer(dm)

def is_authorized(message):
    """只允许配置的 chat_id 使用"""
    if str(message.chat.id) != Config.TG_CHAT_ID:
        bot.reply_to(message, "⛔️ 无权访问")
        return False
    return True

# ==================== 命令处理 ====================

@bot.message_handler(commands=['start', 'help'])
def send_welcome(message):
    if not is_authorized(message):
        return
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

@bot.message_handler(commands=['reset'])
def handle_reset(message):
    if not is_authorized(message):
        return
    
    bot.reply_to(message, "⚠️ 正在重置系统... (删除脏数据)")
    db_path = '/app/data/quant.db'
    
    try:
        if os.path.exists(db_path):
            os.remove(db_path)
            bot.send_message(message.chat.id, "🗑️ 旧数据库文件已删除。")  # 修复：添加结束引号
        
        global dm, strategy
        dm = DataManager()
        strategy = StrategyAnalyzer(dm)
        
        bot.send_message(message.chat.id,
                         "✅ **重置成功！**\n请立即发送 `/update` 重新下载最近 60 天的数据。",
                         parse_mode='Markdown')
    except Exception as e:
        bot.reply_to(message, f"❌ 重置失败: {e}")

@bot.message_handler(commands=['info'])  # 修复：添加 @
def handle_info(message):
    if not is_authorized(message):
        return
    
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

@bot.message_handler(commands=['update'])  # 修复：添加 @
def handle_update(message):
    if not is_authorized(message):
        return
    
    bot.reply_to(message, "✅ 已收到 /update 命令，正在后台同步数据（预计2-5分钟）...")  # 新增：即时确认
    print("🔄 用户触发 /update，开始同步数据...")  # 新增：日志打印
    
    try:
        success, fail, err = dm.sync_data(lookback_days=Config.BOX_DAYS + 10)
        latest_date = dm.db.check_latest_date('daily_price')
        
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
        print(f"✅ /update 完成: 成功 {success} 天, 失败 {fail} 天")  # 新增：日志
        
    except Exception as e:
        bot.reply_to(message, f"❌ 严重错误: {e}")
        print(f"❌ /update 异常: {e}")  # 新增：日志

@bot.message_handler(commands=['scan'])  # 修复：添加 @
def handle_scan(message):
    if not is_authorized(message):
        return
    
    bot.reply_to(message, "✅ 已收到 /scan 命令，正在分析最新数据，请稍候...")  # 新增：即时确认
    print("🚀 用户触发 /scan，开始策略分析...")  # 新增：日志打印
    
    try:
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
        
        print(f"🏁 /scan 完成，最终选中 {len(results)} 只")  # 新增：日志
        
    except Exception as e:
        bot.send_message(message.chat.id, f"❌ 扫描过程崩溃: {str(e)}")
        print(f"❌ /scan 异常: {e}")  # 新增：日志

@bot.message_handler(commands=['check'])  # 修复：添加 @
def handle_check(message):
    if not is_authorized(message):
        return
    
    try:
        code = message.text.split()[1].upper()
    except:
        bot.reply_to(message, "用法：/check 600519.SH")
        return

    bot.reply_to(message, f"🔍 正在联网诊断 `{code}` ...", parse_mode='Markdown')
    try:
        trade_date = dm.get_trade_date()
        df = dm.pro.daily(ts_code=code, end_date=trade_date, limit=Config.BOX_DAYS + 10)
        
        if df.empty:
            bot.send_message(message.chat.id, "❌ 未获取到数据")
            return

        curr = df.iloc[0]
        past = df.iloc[1:Config.BOX_DAYS + 1]
        
        box_high = past['high'].max()
        vol_ma20 = past['vol'].head(20).mean()
        
        is_breakout = curr['close'] > box_high * 1.01
        is_vol = curr['vol'] > vol_ma20 * 1.5

        res = (
            f"📊 **{code} 诊断结果**\n"
            f"现价: `{curr['close']}`\n"
            f"------------------\n"
            f"1. 突破箱体: {'✅' if is_breakout else '❌'}\n"
            f"   (上沿 `{box_high:.2f}`)\n"
            f"2. 有效放量: {'✅' if is_vol else '❌'}\n"
            f"   (量比 `{round(curr['vol']/vol_ma20, 1) if vol_ma20 > 0 else 0}`)"
        )
        bot.send_message(message.chat.id, res, parse_mode='Markdown')
    except Exception as e:
        bot.send_message(message.chat.id, f"Error: {e}")

# ==================== Webhook 路由 ====================

@app.route('/webhook', methods=['POST'])
def webhook():
    """处理 Telegram 推送的更新"""
    if request.headers.get('content-type') == 'application/json':
        json_data = request.get_json(force=True)  # 强制解析 JSON，避免空体问题
        update = telebot.types.Update.de_json(json_data)
        if update:
            bot.process_new_updates([update])  # 关键！触发所有 @bot.message_handler
        return '', 200  # Telegram 要求返回 200 OK
    else:
        abort(403)

@app.route('/')
def index():
    return "🤖 Quant Bot is running! Webhook 已就绪。"

# ==================== 启动时设置 Webhook ====================

if __name__ == "__main__":
    # 先清除旧的 webhook
    bot.remove_webhook()
    time.sleep(1)

    # 自动检测常见平台的公网域名
    domain = (
        os.getenv('RAILWAY_STATIC_URL') or
        os.getenv('RENDER_EXTERNAL_URL') or
        os.getenv('FLY_APP_NAME') + '.fly.dev' if os.getenv('FLY_APP_NAME') else None
    )

    # 如果上面都没检测到，你可以直接手动写死（推荐第一次部署时这么做）
    if not domain:
        # ↓↓↓ 请改成你自己的实际域名 ↓↓↓
        domain = "quant-bot-production.up.railway.app"  # 示例：quant-bot.up.railway.app

    webhook_url = f"https://{domain.strip('/')}/webhook"
    print(f"正在设置 Webhook URL: {webhook_url}")

    if bot.set_webhook(url=webhook_url):
        print("✅ Webhook 设置成功！Bot 已上线")
    else:
        print("❌ Webhook 设置失败，请检查域名是否正确、是否为 HTTPS")

    # 启动 Flask 服务（平台会注入 PORT 环境变量）
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
