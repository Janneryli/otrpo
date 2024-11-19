import pika
import asyncio
import aiohttp
from lxml import html
from urllib.parse import urlparse
import os
from dotenv import load_dotenv
import signal
import sys

# Загрузка переменных окружения
load_dotenv()
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")


def get_page_title(content):
    tree = html.fromstring(content)
    title = tree.xpath('//title/text()')
    return title[0] if title else "Без названия"


def get_links_with_text(content, base_url):
    tree = html.fromstring(content)
    tree.make_links_absolute(base_url=base_url)
    links = tree.xpath('//a')
    result = []
    for link in links:
        href = link.get('href', '').strip()
        text = link.text_content().strip() if link.text_content() else "Без названия"
        result.append((text, href))
    return result


async def process_page(url):
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url) as response:
                content = await response.text()

                # Получить заголовок страницы
                title = get_page_title(content)
                print(f"Обрабатывается страница:\nНазвание: {title}\nСсылка: {url}\n")

                # Найти ссылки
                links = get_links_with_text(content, url)
                print("Найдены ссылки:")
                for text, href in links:
                    print(f"Название: {text}, Ссылка: {href}")
        except Exception as e:
            print(f"Ошибка при обработке страницы {url}: {e}")


def process_message(ch, method, properties, body):
    url = body.decode()
    print(f"Получено сообщение: {url}")
    asyncio.run(process_page(url))
    ch.basic_ack(delivery_tag=method.delivery_tag)


def start_consumer():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()
    channel.queue_declare(queue="links_queue")

    # Функция для обработки сообщений
    channel.basic_consume(queue="links_queue", on_message_callback=process_message, auto_ack=False)

    print("Consumer запущен. .")

    try:
        while True:
            channel._process_data_events(time_limit=1)  # Проверка новых сообщений каждую секунду
    except KeyboardInterrupt:
        print("Остановка Consumer...")
        channel.close()
        connection.close()
        sys.exit(0)


if __name__ == "__main__":
    start_consumer()
