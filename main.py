import asyncio
import aiohttp
import aiofiles
import aiofiles.os
import os
import requests
import hashlib
from bs4 import BeautifulSoup
from collections import deque
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BASE_URL = 'https://gitea.radium.group'
LOCAL_URL = '/radium/project-configuration'
BRANCH_NAME = 'master'
FOLDER_TO_SAVE = 'temp_folder'
NUMBER_OF_WORKERS = 3
URLS = deque()


async def download_file(client: aiohttp.ClientSession, idx: int) -> None:
    """
    Search urls contains `/raw`.
    Write content to the corresponding file in directory FOLDER_TO_SAVE.

    :param client: async http client
    :param idx: number of worker
    :return: None
    """
    while URLS:
        url = URLS.popleft()
        text = await get_data(client=client, url=f'{BASE_URL}{url}')
        soup = BeautifulSoup(text, 'html.parser')
        if soup.find('thead'):
            find_links(soup=soup, len_source=len(url))
            continue
        for a_tag in soup.findAll('a'):
            url = a_tag.get('href', '')
            if url.startswith(f'{LOCAL_URL}/raw'):
                path = os.path.join(FOLDER_TO_SAVE, url.split(BRANCH_NAME)[-1][1:])
                logger.info(f'Worker_{idx} download to {path!r}')
                text = await get_data(client=client, url=f'{BASE_URL}{url}')
                await write_to_disk(path=path, text=text)
                break


async def get_data(client: aiohttp.ClientSession, url: str) -> bytes:
    """
    Download bytestring from `url` and return it.

    :param client: async http client
    :param url: url
    :return: bytes
    """
    for attempt in range(1, 4):
        async with client.get(url) as response:
            if response.status == 200:
                return await response.read()
            else:
                logger.error(f'Error! {response.status=} from {url!r}. {attempt=}')


async def write_to_disk(path: str, text: bytes) -> None:
    """
    Write bytestring to `path` (create all necessary folders).

    :param path: path with filename
    :param text: text
    :return: None
    """
    folder = os.path.dirname(path)
    created = await aiofiles.os.path.exists(folder)
    if not created:
        await aiofiles.os.makedirs(folder, exist_ok=True)
    async with aiofiles.open(path, 'wb') as file:
        await file.write(text)


def find_links(soup: BeautifulSoup, len_source: int) -> None:
    """
    Search urls with `/src` in <a>-tags.

    :param soup: html text
    :param len_source: length of source url
    :return: None
    """
    for a_tag in soup.find('tbody').findAll('a'):
        url = a_tag.get('href', '')
        if url.startswith(f'{LOCAL_URL}/src') and len(url) > len_source:
            URLS.append(url)


async def download_repo() -> None:
    """
    Create async tasks that downloads files.

    :return: None
    """
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(5)) as client:
        tasks = [download_file(client, idx) for idx in range(1, NUMBER_OF_WORKERS + 1)]
        await asyncio.gather(*tasks)


def hash_print(folder: str) -> None:
    """
    Print SHA256 hash of each file in `folder` (recursively).

    :param folder: folder
    :return: None
    """
    for dirpath, _, filenames in os.walk(folder):
        for filename in filenames:
            hash_obj = hashlib.sha256()
            path = os.path.join(dirpath, filename)
            with open(path, 'rb') as file:
                for row in file:
                    hash_obj.update(row)
            print(path)
            print('SHA256:', hash_obj.hexdigest())


def main() -> None:
    """
    Fill `URLS` queue by links from page `BASE_URL + LOCAL_URL`.

    :return:
    """
    response = requests.get(f'{BASE_URL}{LOCAL_URL}', timeout=3)
    if response.status_code != 200:
        logger.error(f'Error! {response.status_code=}. Message: {response.text}')
        return
    soup = BeautifulSoup(response.text, 'html.parser')
    find_links(soup=soup, len_source=len(LOCAL_URL))
    asyncio.run(download_repo())
    hash_print(FOLDER_TO_SAVE)


if __name__ == '__main__':
    main()
