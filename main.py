import asyncio

from pyrogram import Client, filters, idle
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.errors import BadRequest, FloodWait

from pymongo import MongoClient

from config import *


STOP = InlineKeyboardMarkup([[InlineKeyboardButton(text="Stop", callback_data="STOP_BROADCAST")]])

total, sent, fail, stop = 0, 0, 0, False

db = MongoClient(DB_URL).get_database(DB_NAME)
user_coll = db.get_collection(USER_COLL_NAME)
chat_coll = db.get_collection(CHAT_COLL_NAME)

app = Client(USERNAME, API_ID, API_HASH, bot_token=TOKEN)


@app.on_callback_query(filters.regex("STOP_BROADCAST"))
async def stop_broadcast(app, CallbackQuery):
    user_id = CallbackQuery.from_user.id
    if user_id not in SUDO:
        return
    global stop
    stop = True
    await CallbackQuery.answer("Broadcast stopped.", show_alert = True)


@app.on_message(filters.command(["gcast"]))
async def broadcast(app, message):
    user_id = message.from_user.id
    if user_id not in SUDO:
        return
    if not message.reply_to_message:
        return await message.reply("Reply to a message to broadcast it.")
    msg = message.reply_to_message
    global total, sent, fail, stop
    if total:
        return await message.reply("Eyni vaxtda yalnız bir yayın etmək olar.")
    stop = False
    data = []
    dev = await message.reply(f"Broadcast is starting...", reply_markup = STOP)
    await asyncio.sleep(10)
    if "-u" in message.text:
        for d in user_coll.find({ID_FIELD_NAME: {"$gt": 0}}):
            data.append(d)
    elif "-c" in message.text:
        for d in chat_coll.find({ID_FIELD_NAME: {"$lt": 0}}):
            data.append(d)
    else:
        for d in user_coll.find({ID_FIELD_NAME: {"$gt": 0}}):
            data.append(d)
        for d in chat_coll.find({ID_FIELD_NAME: {"$lt": 0}}):
            data.append(d)
    for d in data:
        if stop:
            break
        State = False
        while not State:
            try:
                await msg.forward(d[ID_FIELD_NAME])
                sent += 1
                State = True
            except FloodWait as e:
                await asyncio.sleep(e.x)
            except BadRequest:
                fail += 1
                State = True
            except Exception as e:
                print(e)
                fail += 1
                State = True
        total += 1
        await asyncio.sleep(0.05)
        if not total % 60:
            try:
                await dev.edit_text(f"Broadcast is continuing...\n\nmessage sent to {total} chats. From those {sent} were successful,  and {fail} were failed.", reply_markup = STOP)
                await asyncio.sleep(0.05)
            except FloodWait as e:
                await asyncio.sleep(e.x)
                await dev.edit_text(f"Broadcast is continuing...\n\nmessage sent to {total} chats. From those {sent} were successful,  and {fail} were failed.", reply_markup = STOP)
    await dev.delete()
    await asyncio.sleep(3)
    if stop:
        await message.reply(f"Broadcast ended.\n\nmessage sent to {total} chats. From those {sent} were successful,  and {fail} were failed.", reply_markup=None)
    else:
        await message.reply(f"Broadcast ended.\n\nmessage sent to {total} chats. From those {sent} were successful,  and {fail} were failed.", reply_markup=None)
    sent = 0
    fail = 0
    total = 0

app.start()
idle()