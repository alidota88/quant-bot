# main.py
import os
import time
import telebot
import threading
from datetime import datetime, timedelta
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
            bot.send_message(message.chat.id, "🗑️ 旧数据库文件已删除。")
        
        global dm, strategy
        dm = DataManager()
        strategy = StrategyAnalyzer(dm)
        
        bot.send_message(message.chat.id,
                         "✅ **重置成功！**\n请立即发送 `/update` 重新下载最近 60 天的数据。",
                         parse_mode='Markdown')
    except Exception as e:
        bot.reply_to(message, f"❌ 重置失败: {e}")


@bot.message_handler(commands=['info'])
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


@bot.message_handler(commands=['update'])
def handle_update(message):
    if not is_authorized(message):
        return
    
    bot.reply_to(message, "✅ 已收到 /update 命令，正在后台同步数据（预计2-5分钟）...")
    print("🔄 用户手动触发 /update，开始同步数据...")
    
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
        print(f"✅ 用户 /update 完成: 成功 {success} 天, 失败 {fail} 天")
        
    except Exception as e:
        bot.reply_to(message, f"❌ 严重错误: {e}")
        print(f"❌ 用户 /update 异常: {e}")


@bot.message_handler(commands=['scan'])
def handle_scan(message):
    if not is_authorized(message):
        return
    
    bot.reply_to(message, "✅ 已收到 /scan 命令，正在分析最新数据，请稍候...")
    print("🚀 用户手动触发 /scan，开始策略分析...")
    
    try:
        results = strategy.run_daily_scan()
        
        if not results:
            bot.send_message(message.chat.id, "📅 扫描完成，今日无符合模型的标的。")
        else:
            msg = f"🚀 **选股结果** ({len(results)}只)\n\n"
            for s in results[:10]:
                msg += f"🐂 **{s['name']}** (`{s['ts_code']}`)\n"
                msg += f"   现价: `{s['price']}`\n"
                msg += f"   理由: {s['reason']}\n\n"
            bot.send_message(message.chat.id, msg, parse_mode='Markdown')
        
        print(f"🏁 用户 /scan 完成，最终选中 {len(results)} 只")
        
    except Exception as e:
        bot.send_message(message.chat.id, f"❌ 扫描过程崩溃: {str(e)}")
        print(f"❌ 用户 /scan 异常: {e}")


@bot.message_handler(commands=['check'])
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


# ==================== 自动每日任务（下载数据 + 选股 + 推送） ====================

def daily_auto_task():
    """每天下午17:00自动执行：更新数据 → 选股 → 推送报告"""
    def get_next_run_time():
        now = datetime.now()
        next_run = now.replace(hour=17, minute=0, second=0, microsecond=0)
        if now >= next_run:
            next_run += timedelta(days=1)
        return next_run

    while True:
        next_run = get_next_run_time()
        sleep_seconds = (next_run - datetime.now()).total_seconds()
        
        print(f"⏰ 下次自动任务时间: {next_run.strftime('%Y-%m-%d %H:%M:%S')} (距今约 {int(sleep_seconds//60)} 分钟)")
        time.sleep(sleep_seconds)

        try:
            today_str = datetime.now().strftime('%Y%m%d')
            print(f"🕔 {today_str} 到达自动任务时间，开始执行...")

            # 1. 检查是否为交易日
            cal = dm.pro.trade_cal(exchange='', start_date=today_str, end_date=today_str)
            if cal.empty or cal.iloc[0]['is_open'] == 0:
                print(f"📅 {today_str} 非交易日，跳过本次自动任务")
                continue

            # 2. 自动更新数据
            print("🔄 自动任务：开始更新最新数据...")
            success, fail, err = dm.sync_data(lookback_days=Config.BOX_DAYS + 10)
            latest_date = dm.db.check_latest_date('daily_price')
            print(f"✅ 数据更新完成：最新日期 {latest_date}，成功 {success} 天，失败 {fail} 天")

            # 3. 自动选股扫描
            print("🚀 自动任务：开始选股扫描...")
            results = strategy.run_daily_scan()
            trade_date = dm.get_trade_date()

            # 4. 构建并推送报告
            if not results:
                msg = f"📅 {trade_date} \n\n今日无符合【严格突破模型】的标的。\n保持观察，耐心等待主升浪！"
            else:
                msg = f"🚀 **量化选股日报** ({trade_date})\n"
                msg += f"策略：突破箱体 + 机构主线 + 资金连买\n"
                msg += f"共选中 {len(results)} 只优质标的\n"
                msg += f"========================\n\n"

                for s in results[:10]:
                    msg += f"🔥 **{s['name']}** (`{s['ts_code']}`)\n"
                    msg += f"   💰 现价: {s['price']} (涨幅 {s.get('pct_chg', 'N/A')}%)\n"
                    msg += f"   💡 理由: {s['reason']}\n\n"

                if len(results) > 10:
                    msg += f"... 共 {len(results)} 只（更多请手动 /scan 查看）"

            bot.send_message(Config.TG_CHAT_ID, msg, parse_mode='Markdown')
            print(f"✅ 自动日报已推送（{len(results)} 只标的）")

        except Exception as e:
            print(f"❌ 自动任务执行出错: {e}")
            try:
                bot.send_message(Config.TG_CHAT_ID, f"⚠️ 自动任务出错：{str(e)}")
            except:
                pass


# 启动后台线程执行自动任务
threading.Thread(target=daily_auto_task, daemon=True).start()


# ==================== Webhook 路由 ====================

@app.route('/webhook', methods=['POST'])
def webhook():
    if request.headers.get('content-type') == 'application/json':
        json_data = request.get_json(force=True)
        update = telebot.types.Update.de_json(json_data)
        if update:
            bot.process_new_updates([update])
        return '', 200
    else:
        abort(403)


@app.route('/')
def index():
    return "🤖 Quant Bot is running! Webhook 已就绪。"


# ==================== 启动时设置 Webhook ====================

if __name__ == "__main__":
    bot.remove_webhook()
    time.sleep(1)

    domain = (
        os.getenv('RAILWAY_STATIC_URL') or
        os.getenv('RENDER_EXTERNAL_URL') or
        os.getenv('FLY_APP_NAME') + '.fly.dev' if os.getenv('FLY_APP_NAME') else None
    )

    if not domain:
        domain = "quant-bot-production.up.railway.app"  # ← 请确认这是你的真实域名

    webhook_url = f"https://{domain.strip('/')}/webhook"
    print(f"正在设置 Webhook URL: {webhook_url}")

    if bot.set_webhook(url=webhook_url):
        print("✅ Webhook 设置成功！Bot 已上线")
    else:
        print("❌ Webhook 设置失败，请检查域名是否正确、是否为 HTTPS")

    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
