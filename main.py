import asyncio

from pyrogram import Client, filters, idle
from pyrogram.errors import BadRequest, FloodWait

from pymongo import MongoClient

from config import *


db = MongoClient(DB_URL).get_database(DB_NAME)
user_coll = db.get_collection(USER_COLL_NAME)
chat_coll = db.get_collection(CHAT_COLL_NAME)

app = Client(USERNAME, API_ID, API_HASH, bot_token=TOKEN)


@app.on_message(filters.command(["gcast"]))
async def broadcast(app, message):
    user_id = message.from_user.id
    if user_id not in SUDO:
        return
    if not message.reply_to_message:
        return await message.reply("Reply to a message to broadcast it.")
    msg = message.reply_to_message
    sent = 0
    fail = 0
    total = 0
    data = []
    dev = await message.reply(f"Broadcast is starting...")
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
        State = False
        while not State:
            try:
                await msg.forward(d[ID_FIELD_NAME])
                sent += 1
                State = True
                await asyncio.sleep(0.125)
            except FloodWait as e:
                await asyncio.sleep(e.x)
            except BadRequest:
                fail += 1
                State = True
            except Exception as e:
                print(e)
                fail += 1
                State = True
                await asyncio.sleep(0.3)
        total += 1
        if not total % 50:
            try:
                await dev.edit_text(f"Broadcast is continuing...\n\nmessage sent to {total} chats. From those {sent} were successful,  and {fail} were failed.")
                await asyncio.sleep(1)
            except FloodWait as e:
                await asyncio.sleep(e.x)
    await asyncio.sleep(3)
    await dev.delete()
    await message.reply(f"Broadcast ended.\n\nmessage sent to {total} chats. From those {sent} were successful,  and {fail} were failed.")

app.start()
idle()