import asyncio

from pyrogram import Client, filters, idle
from pyrogram.errors import BadRequest, FloodWait

from pymongo import MongoClient


API_ID = 5435275
API_HASH = "3b3b4f97a8795e97863f6843dcc1c199"
TOKEN = "5366100656:AAFk8pzxYCK-ot_L5AMHFUMY-MFCKM5GdrU"
DB_URL = "mongodb://sudo:12345678@localhost:27017/?authMechanism=DEFAULT"
USERNAME = "CrazyGameAzBot"
SUDO = [881143712, 1407140052]

sent, fail, total = 0, 0, 0


coll = MongoClient(DB_URL).namegame.user

app = Client(USERNAME, API_ID, API_HASH, bot_token=TOKEN)


@app.on_message(filters.command(["gcast"]))
async def broadcast(app, message):
    user_id = message.from_user.id
    if user_id not in SUDO:
        return
    if not message.reply_to_message:
        return await message.reply("Mesaja cavab ver.")
    msg = message.reply_to_message
    global sent, fail, total
    sent = 0
    fail = 0
    total = 0
    data = []
    dev = await message.reply(f"yayın başlayır...")
    if "-u" in message.text:
        for d in coll.find({"_id": {"$gt": 0}}):
            data.append(d)
    elif "-c" in message.text:
        for d in coll.find({"_id": {"$lt": 0}}):
            data.append(d)
    else:
        for d in coll.find({}):
            data.append(d)
    for d in data:
        State = False
        while not State:
            try:
                await msg.forward(d["_id"])
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
                await dev.edit_text(f"Yayın davam edir...\n\nMesaj {total} çata göndərilib. Bunlardan {sent} uğurlu, {fail} uğursuz nəticə əldə olunub.")
                await asyncio.sleep(1)
            except FloodWait as e:
                await asyncio.sleep(e.x)
    await asyncio.sleep(3)
    await dev.delete()
    await message.reply(f"Yayın sona çatdı.\n\nMesaj {total} çata göndərilib. Bunlardan {sent} uğurlu, {fail} uğursuz nəticə əldə olunub.")

app.start()
idle()