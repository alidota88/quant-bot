import requests
from config import Config

class TelegramBot:
    def send_report(self, stocks, date_str):
        if not stocks:
            self.send_message(f"📅 {date_str} \n\n今日无符合【严格突破模型】的标的。")
            return

        msg = f"🚀 **量化选股日报** ({date_str})\n"
        msg += f"策略：突破箱体 + 机构主线 + 资金连买\n"
        msg += f"========================\n\n"

        for s in stocks[:10]: # 避免消息过长，只发前10
            msg += f"🔥 **{s['name']}** (`{s['ts_code']}`)\n"
            msg += f"   📂 板块: {s['sector']}\n"
            msg += f"   💰 现价: {s['price']} (涨幅 {s['pct_chg']}%)\n"
            msg += f"   📊 评分: {s['score']}\n"
            msg += f"   💡 理由: {s['reason']}\n\n"
        
        self.send_message(msg)

    def send_message(self, text):
        if not Config.TG_BOT_TOKEN:
            print("❌ 未配置 Telegram Token，仅打印结果:")
            print(text)
            return

        url = f"https://api.telegram.org/bot{Config.TG_BOT_TOKEN}/sendMessage"
        payload = {
            "chat_id": Config.TG_CHAT_ID,
            "text": text,
            "parse_mode": "Markdown"
        }
        try:
            requests.post(url, json=payload, timeout=10)
        except Exception as e:
            print(f"Telegram 发送失败: {e}")
