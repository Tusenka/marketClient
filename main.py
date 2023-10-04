import os
import re
import threading
from asyncio import futures
from multiprocessing.pool import ThreadPool
from pathlib import Path
from urllib.parse import urljoin

import click
import requests
from bs4 import BeautifulSoup, PageElement
from google.auth.transport import grpc
from requests.exceptions import MissingSchema
from selenium import webdriver
from selenium.common import InvalidSessionIdException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.webdriver import WebDriver
import grpc
from concurrent import futures
import time
import Content_pb2_grpc
import Content_pb2

EXAMPLE_URL = "https://market.yandex.ru/catalog--korma-dlia-koshek/71971/list?hid=15685457&track=peaces"

DEFAULT_DIR = "/tmp/cas12"
threadLocal = threading.local()


class Content():
    def __init__(self, dir):
        self.dir = dir

    def run(self, url):
        driver = self.get_driver()
        return to_content(get_item(driver, url), url, self.dir)

    def get_driver(self):
        driver = getattr(threadLocal, 'driver', None)
        if driver is None:
            chromeOptions = webdriver.ChromeOptions()
            chromeOptions.add_argument("--no-sandbox")
            chromeOptions.add_argument(
                "--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36")
            driver = webdriver.Chrome(options=chromeOptions)

            setattr(threadLocal, 'driver', driver)
        return driver


def next_page(url: str, driver: WebDriver) -> BeautifulSoup:
    """

    :param url: str
    :type soup: BeautifulSoup
    """
    url = url if url.find("page=") > -1 else "{}&page=0".format(url) if url.find("?") >= -1 else "{}?page=0"
    page = int(re.findall("(?<=page=)[^&]*", url)[0]) + 1
    next_page_url = re.sub("(?<=page=)[^&]*", str(page), url)
    return get_item(driver, next_page_url)


def get_links(soup: BeautifulSoup, url: str) -> list:
    """

    :param url: str
    :type soup: BeautifulSoup
    """
    try:
        links = [
            urljoin(url, "{}/spec?{}".format(items.get("href").split('?', 1)[0], items.get("href").split('?', 1)[1]))
            for items in
            soup.select("main#searchResults h3 > a:nth-child(1)")]
        return links
    except AttributeError:
        return []


def get_item(driver: WebDriver, url: str) -> BeautifulSoup:
    """

    :param dir: 
    :param url: str
    :type driver: WebDriver
    """
    try:
        driver.get(url)
        html = driver.page_source
        page = BeautifulSoup(html, 'html.parser')
        return page
    except InvalidSessionIdException as err:
        print("Error: Unable to get url {}".format(url))
        return None


def get_title(page: BeautifulSoup) -> str:
    """

    :type page: BeautifulSoup
    """
    try:
        return page.find('h1', {'data-baobab-name': 'title'}).get_text()
    except AttributeError:
        return "Can't get the title text!"


def get_description(page: BeautifulSoup) -> str:
    """

    :type page: BeautifulSoup
    """
    try:
        return page.find('div', {"data-auto": 'full-description-text'}).get_text()
    except AttributeError as err:
        return "Can't get the description text! {}"


def get_image_url(img_href: str) -> str:
    return re.sub('[0-9]{1,3}x[0-9]{1,3}$', 'orig', img_href)


def load_images(page: BeautifulSoup, img_dir: str = DEFAULT_DIR) -> list:
    """

    :param img_dir: str
    :type page: BeautifulSoup
    """
    Path(img_dir).mkdir(parents=True, exist_ok=True)
    try:
        return [load_image(get_image_url(img['src']), img_dir) for img in
                page.select(selector='ul[data-auto="gallery-nav"] div>img')]
    except AttributeError:
        return []


def load_preview(page: BeautifulSoup, img_dir: str) -> str:
    """

    :param img_dir: str
    :type page: BeautifulSoup
    """
    Path(img_dir).mkdir(parents=True, exist_ok=True)
    try:
        img = page.select(selector='div[data-apiary-widget-name=\'@MarketNode/MiniTitleImage\'] img')
        return load_image(get_image_url(img[0]['src']), img_dir) if len(img) > 0 else "Can't find preview image"
    except AttributeError as err:
        return "Can't find preview image: {}".format(err)


def load_image(url: str, img_dir: str):
    try:
        url = url if url.find(':') > -1 else "https:{}".format(url)
        img_local_path = "{}{}.jpg".format(img_dir, hash(url))
        if os.path.isfile(img_local_path):
            return img_local_path
        img = requests.get(url, allow_redirects=True).content
        with open(img_local_path, 'wb') as f:
            f.write(img)
        return img_local_path
    except MissingSchema as e:
        print(e)


def to_content(page: BeautifulSoup, url: str, img_dir: str):
    content = Content_pb2.Content()
    content.title = get_title(page)
    content.img_preview = load_preview(page, "{}/{}".format(img_dir, re.search('\d*$', url).group(0)))
    content.description = get_description(page)
    [content.properties.append(property) for property in __get_properties(page)]
    return content


def __get_property(item: PageElement):
    """
    :param item: PageElement
    """
    try:
        property = Content_pb2.Property()
        property.property = item.find('dt').get_text()
        property.values[:] = re.split('[,.;:]', item.find('dd').get_text())
        return property
    except AttributeError:
        return {}


def __get_properties(page: BeautifulSoup):
    """

    :type page: BeautifulSoup
    """
    try:
        properties = [
            __get_property(item) for
            item in page.find_all('dl')]

        return properties
    except AttributeError:
        return []


@click.group()
def main():
    pass


# TODO: Add test


def __parse(url: str, dir: str) -> list:
    chrome_options: Options = webdriver.ChromeOptions()
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument(
        "--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 "
        "Safari/537.36")
    driver = webdriver.Chrome(options=chrome_options)
    links = []
    while (page := next_page(url, driver)) is not None:
        links.extend(get_links(page, url))
    driver.close()
    content = Content(dir)
    items = []
    for item in ThreadPool(5).map(content.run, links):
        items.append(item)
    return items


def __predicate(values: list, filter: Content_pb2.FilterProperty) -> bool:
    if filter.predicate == "in":
        return len([",".join(values).find(value) > -1 for value in filter.property.values]) > 0
    elif filter.predicate == "not":
        return len([",".join(values).find(value) > -1 for value in filter.property.values]) == 0


def _filter(url: str, dir: str, filter: Content_pb2.Filter = None):
    return [item for item in __parse(url, dir) if __predicate(item.values, filter.filter_properties[0])]


@click.command()
@click.option("--url", help="Provide url",
              default=EXAMPLE_URL)
@click.option("--dir", help="Provide directory to save product images. If directory does not exist it will be created",
              default=DEFAULT_DIR)
def parse(url, dir):
    print(__parse(url, dir))


@click.command()
@click.option("--url", help="Provide url",
              default=EXAMPLE_URL)
@click.option("--dir", help="Provide directory to save product images. If directory does not exist it will be created",
              default=DEFAULT_DIR)
def filter(url, dir):
    print(_filter(dir, url, None))


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    Content_pb2_grpc.add_FilterServiceServicer_to_server(FilterService(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    server.wait_for_termination()


class FilterService(Content_pb2_grpc.FilterService):

    def __init__(self, *args, **kwargs):
        pass

    @staticmethod
    def GetContent(content_filter: Content_pb2.Filter, *args, **kwargs):
        _filter(EXAMPLE_URL, DEFAULT_DIR, content_filter)


if __name__ == '__main__':
    serve()

main.add_command(parse)
main.add_command(filter)
if __name__ == '__main__':
    main()
