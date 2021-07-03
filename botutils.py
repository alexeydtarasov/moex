import telebot
import traceback
import pandas as pd

ADMINS_LIST_PATH = "../common_data/admins_list.csv"
ERROR_BOT_TOKEN = '1463337477:AAF8AqAlWVmpWkbN1XRiIcKU8EO3hBZKvPU'


def load_admin_list(path):
    return pd.read_csv(path)['id'].values.tolist()


def exception_handler(event):
    dev_ids = load_admin_list(ADMINS_LIST_PATH)
    bot = telebot.TeleBot(ERROR_BOT_TOKEN)
    exc = event.exception
    for dev_id in dev_ids:
        bot.send_message(dev_id, "".join(traceback.format_exception(type(exc), exc, exc.__traceback__)))
