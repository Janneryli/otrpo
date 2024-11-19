import aiohttp
import asyncio
import pika
import os
from urllib.parse import urlparse
from lxml import html
from dotenv import load_dotenv

# Загрузка переменных окружения
load_dotenv()
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")

async def fetch_links(url):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            content = await response.text()
            tree = html.fromstring(content)
            tree.make_links_absolute(base_url=url)  # Преобразуем все ссылки в абсолютные
            links = [link for link in tree.xpath("//a/@href") if urlparse(link).netloc == urlparse(url).netloc]
            return links

def send_to_queue(links):
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()
    channel.queue_declare(queue="links_queue")
    for link in links:
        channel.basic_publish(exchange="", routing_key="links_queue", body=link)
        print(f"Sent to queue: {link}")
    connection.close()

async def main():
    url = input("Enter URL: ")
    if not url.startswith("http"):
        print("Invalid URL. Please use http:// or https://")
        return
    links = await fetch_links(url)
    if links:
        send_to_queue(links)
    else:
        print("No links found on the page.")

if __name__ == "__main__":
    asyncio.run(main())
