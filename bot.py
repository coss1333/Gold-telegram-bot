import os
from dotenv import load_dotenv
from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import Application, CommandHandler, ContextTypes
from data_sources import get_gold_snapshot, format_snapshot

load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
if not TELEGRAM_BOT_TOKEN:
    raise RuntimeError("TELEGRAM_BOT_TOKEN is not set. Put it in .env")

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    text = (
        "Привет! Я бот-сводка по Золоту.\n\n"
        "Команды:\n"
        "• /gold — показать текущую цену и объёмы (последний час)\n"
        "• /help — помощь\n"
    )
    await update.message.reply_text(text)

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "Используйте /gold, чтобы получить:\n"
        "• Изменение цены Spot XAU/USD и GLD (ETF)\n"
        "• Изменение цены фьючерсов (GC=F)\n"
        "• Объём фьючерсов и ETF за последний час\n"
    )

async def gold_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        await update.message.chat.send_action(action="typing")
        snapshot = get_gold_snapshot()
        message = format_snapshot(snapshot)
        await update.message.reply_text(message, parse_mode=ParseMode.MARKDOWN)
    except Exception as e:
        await update.message.reply_text(f"Ошибка: {e}")

def main() -> None:
    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("gold", gold_cmd))
    app.run_polling()

if __name__ == "__main__":
    main()
