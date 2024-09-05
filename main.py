from Parser.parser import IParser
import asyncio
import aiohttp
import logging
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
import Extentions.json_serializer as jsonext
import configparser
import sys
import Extentions.ini_deserializer as ini
from lxml import html as lxml

# Конфигурация
CONFIG = configparser.ConfigParser()
F_NAME = str(sys.argv[0]).split('.')[0] + '_settings.ini'

# Десериализуем настройки из INI-файла
ini.IniDeserializer.deserialize(CONFIG, F_NAME)

# Получаем значения из конфигурационного файла
API = CONFIG['GENERAL']['api']
URL = CONFIG['GENERAL']['url']
KEY = CONFIG['GENERAL']['key']
MAIN = CONFIG['PARSER']['mainpage']
KINORATE = CONFIG['PARSER']['kinoRating']
IMDBRATE = CONFIG['PARSER']['IMDbRating']

# Заголовки для HTTP-запросов
HEADERS = {
    'user-agent': UserAgent().random,
    'X-API-KEY': KEY,
    'Accept': '*/*'
}


# Класс парсера Кинопоиска
class KinopoiskParser(IParser):
    def __init__(self):
        # Настройка параметров логирования
        logging.basicConfig(filename='parser.log', encoding='utf-8', level=logging.DEBUG,
                            format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p', filemode='w')

        # Настройка параметров для Chrome браузера
        self.__chrome_options = Options()
        self.__chrome_options.add_argument("--headless")  # Запуск без интерфейса

    async def get_html(self, url):
        """
        Асинхронная функция для получения HTML-ответа от API.
        """
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=HEADERS) as resp:
                if resp.status == 200:
                    logging.info(f'Connect to api status: {resp.status}')
                    return await resp.json()
                else:
                    logging.error(f'Connection error, status: {resp.status}')
                    resp.close()

    def __get_html_api(self, url) -> webdriver:
        """
        Функция для получения HTML через Selenium WebDriver.
        """
        driver = webdriver.Chrome(ChromeDriverManager().install(), options=self.__chrome_options)
        driver.implicitly_wait(5)
        driver.get(url)
        return driver

    async def parse(self, id: int):
        """
        Основная асинхронная функция парсинга данных по идентификатору.
        """
        try:
            # Получаем ответ от API
            response = await self.get_html(f'{API}{id}')

            # Получаем HTML страницы через Selenium
            driver = self.__get_html_api(f'{URL}{id}')

            # Парсим содержимое страницы
            movie = self.__get_content(driver)

            # Обновляем данные из API информацией с веб-страницы
            response['data'].update(movie)

            # Сериализация данных в JSON
            loop = asyncio.get_event_loop()
            loop.run_until_complete(await jsonext.JsonSerializer.serialize(response, id))
            loop.close()
            print("Parsing successfully")

        except Exception as exc:
            logging.error(exc)

    def __get_content(self, html: webdriver) -> dict:
        """
        Извлечение данных из HTML-кода страницы.
        """
        try:
            soup = BeautifulSoup(html.page_source, 'lxml')
            items = soup.find_all('div', class_=f"styles_root{MAIN}")
            dom = lxml.fromstring(soup.renderContents())

            for item in items:
                kinotype = item.find('h3', class_='film-page-section-title').get_text()

            if kinotype[2:-1] == 'сериал':
                # Парсинг данных для сериалов
                act_length = len(dom.xpath(
                    '//*[@id="__next"]/div/div[2]/div[2]/div[2]/div/div[3]/div/div/div[2]/div[2]/div[2]/div[1]/ul/li')) + 1
                actors = self.__actors_worker(act_length,
                                              dom,
                                              '//*[@id="__next"]/div/div[2]/div[2]/div[2]/div/div[3]/div/div/div[2]/div[2]/div[2]/div[1]/ul/li')
                dist_length = len(dom.xpath(
                    '//*[@id="__next"]/div/div[2]/div[2]/div[2]/div/div[3]/div/div/div[2]/div[1]/div/div[5]/div[2]/a')) + 1
                distributors = self.__distributor_worker(dist_length,
                                                         dom,
                                                         '//*[@id="__next"]/div/div[2]/div[2]/div[2]/div/div[3]/div/div/div[2]/div[1]/div/div[5]/div[2]/a')

            else:
                # Парсинг данных для фильмов
                act_length = len(dom.xpath(
                    '//*[@id="__next"]/div/div[2]/div[2]/div[2]/div/div[3]/div/div/div[2]/div[2]/div/div/ul/li')) + 1
                actors = self.__actors_worker(act_length,
                                              dom,
                                              '//*[@id="__next"]/div/div[2]/div[2]/div[2]/div/div[3]/div/div/div[2]/div[2]/div/div[1]/ul/li')
                dist_length = len(dom.xpath(
                    '//*[@id="__next"]/div/div[2]/div[2]/div[2]/div/div[3]/div/div/div[2]/div[1]/div/div[5]/div[2]/a')) + 1
                distributors = self.__distributor_worker(dist_length,
                                                         dom,
                                                         '//*[@id="__next"]/div/div[2]/div[2]/div[2]/div/div[3]/div/div/div[2]/div[1]/div/div[5]/div[2]/a')

            movie = {}
            for item in items:
                movie = {
                    # Извлечение рейтинга Кинопоиска
                    'ratingKinopoisk': item.find('a', class_=f'styles_rootLink{KINORATE}').get_text()
                    if item.find('a', class_=f'styles_rootLink{KINORATE}') is not None else None,
                    # Извлечение рейтинга IMDb
                    'ratingIMDb': item.find('span', class_=f'styles_valueSection{IMDBRATE}').get_text()
                    if item.find('span', class_=f'styles_valueSection{IMDBRATE}') is not None else None,
                    # Список дистрибьюторов
                    'distributors': distributors if bool(distributors) is not False else None,
                    # Список актеров
                    'actors': actors if bool(actors) is not False else None
                }

        except Exception as ex:
            logging.error(f'Error: {ex}')

        finally:
            self.dispose(html)  # Закрытие драйвера в любом случае

        return movie

    def __actors_worker(self, act_lenght, dom, path):
        """
        Извлечение списка актеров.
        """
        if act_lenght == 1:
            act_lenght = len(dom.xpath(
                '//*[@id="__next"]/div/div[2]/div[2]/div[2]/div/div[3]/div/div/div[2]/div[2]/div/div[1]/ul/li')) + 1
            if act_lenght == 1:
                act_lenght = len(dom.xpath(
                    '//*[@id="__next"]/div/div[2]/div[1]/div[2]/div/div[3]/div/div/div[2]/div[2]/div[1]/div[1]/ul/li')) + 1

        if bool(dom.xpath(path)) is False:
            path = '//*[@id="__next"]/div/div[2]/div[2]/div[2]/div/div[3]/div/div/div[2]/div[2]/div/div[1]/ul/li'
            if bool(dom.xpath(path)) is False:
                path = '//*[@id="__next"]/div/div[2]/div[1]/div[2]/div/div[3]/div/div/div[2]/div[2]/div[1]/div[1]/ul/li'

        actors = []
        for j in range(1, act_lenght):
            actors.append({'actor': ''.join(map(str, dom.xpath(f'{path}[{j}]/a/text()')))})  # Извлечение имени актера
            for element in actors:
                if element['actor'] == '':
                    actors.remove(element)  # Удаление пустых значений

        return actors

    def __distributor_worker(self, dist_length, dom, path):
        """
        Извлечение списка дистрибьюторов.
        """
        if dist_length == 1:
            dist_length = len(dom.xpath(
                '//*[@id="__next"]/div/div[2]/div[1]/div[2]/div/div[3]/div/div/div[2]/div[1]/div/div[5]/div[2]/a')) + 1
        if bool(dom.xpath(path)) is False:
            path = '//*[@id="__next"]/div/div[2]/div[1]/div[2]/div/div[3]/div/div/div[2]/div[1]/div/div[5]/div[2]/a'

        distributors = []
        for i in range(1, dist_length):
            distributors.append(
                {'distributor': ''.join(map(str, dom.xpath(f'{path}[{i}]/text()')))})  # Извлечение имени дистрибьютора
            for dis in distributors:
                if dis['distributor'] == '...':
                    distributors.remove(dis)  # Удаление некорректных значений

        return distributors

    def dispose(self, driver: webdriver):
        """
        Закрытие и завершение работы драйвера и освобождение памяти.
        """
        driver.close()
        driver.quit()
        driver.stop_client()
        logging.info('Chrome driver is closed, memory is cleared')

