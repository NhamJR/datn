import json
import discord
from discord.ext import commands
from discord import app_commands
from dotenv import load_dotenv, dotenv_values
import os

# from influx_db import get_latest_price
from datetime import datetime
import time
import asyncio
from confluent_kafka import Consumer, KafkaError
import socket
import concurrent.futures

#os.chdir("discord_bot")

print(load_dotenv("../all.env"))

DISCORD_BOT_TOKEN = os.environ["DISCORD_BOT_TOKEN"]
consumer = Consumer(
    {
        "bootstrap.servers": "pkc-ldvr1.asia-southeast1.gcp.confluent.cloud:9092",
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "PLAIN",
        "sasl.username": "KDTB5F5MMOC6KNZM",
        "sasl.password": "dNMPo1y5msUnZ9nI106CgH3r2GVWR6CqHy79h6GtOAb8itAxiaOLkFFctfAQmq5s",
        "group.id": "stock_price_group",
        "auto.offset.reset": "latest",  
        "client.id": socket.gethostname(),
    }
)
# Subscribe to the Kafka topic
consumer.subscribe(["stock_warning"])
intents = discord.Intents.default()
intents.message_content = True


bot = commands.Bot(command_prefix="", intents=intents)
tree = bot.tree


@bot.event
async def on_ready():
    print("Success: Bot is connected to Discord")
    await bot.tree.sync()
    loop = asyncio.get_running_loop()
    with concurrent.futures.ThreadPoolExecutor() as executor:
        # Run the blocking function in an executor
        await loop.run_in_executor(executor, receive_message, loop)


@bot.command()
async def reload(ctx):
    # Reloads the file, thus updating the Cog class.

    await reload_ext()
    synced = await bot.tree.sync()
    print(synced)


async def load():
    for filename in os.listdir("./cogs"):
        if filename.endswith("py"):
            await bot.load_extension(f"cogs.{filename[:-3]}")


async def reload_ext():
    print("reloading...")
    for filename in os.listdir("./cogs"):
        if filename.endswith("py"):
            await bot.reload_extension(f"cogs.{filename[:-3]}")
            print("reload" + filename)
    print("reloaded")


def receive_message(loop):
    print("my_task")
    try:
        while True:
            # print("my_task2")
            try:
                msg = consumer.poll(1)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        print(f"Error while consuming: {msg.error()}")
                else:
                    user = asyncio.run_coroutine_threadsafe(
                        bot.fetch_user(int(msg.key())), loop
                    ).result()
                    asyncio.run_coroutine_threadsafe(
                        user.send(json.loads(msg.value().decode("utf-8"))["msg"]), loop
                    ).result()
            except Exception as e:
                print(e)
    except KeyboardInterrupt:
        pass
    finally:
        # Close the consumer gracefully
        consumer.close()


async def main():
    async with bot:
        await load()
        await bot.start(DISCORD_BOT_TOKEN)


asyncio.run(main())

# bot.run(DISCORD_BOT_TOKEN)