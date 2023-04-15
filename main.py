import asyncio
import aiohttp
import aiofiles
import aiofiles.os
import hashlib
import os
from bs4 import BeautifulSoup
from collections import deque
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

FULL_URL = 'https://gitea.radium.group/radium/project-configuration'
BRANCH_NAME = 'master'
FOLDER_TO_SAVE = 'temp_folder'
NUMBER_OF_WORKERS = 3

BASE_URL = FULL_URL[:FULL_URL.index('/', 8)]
LOCAL_URL = FULL_URL.replace(BASE_URL, '')
QUEUE = deque()


async def analyze_and_download(client: aiohttp.ClientSession, idx: int) -> None:
    """
    Get page content, then fill `QUEUE` or download file.

    :param client: async http client
    :param idx: number of worker
    :return: None
    """
    while QUEUE:
        url = QUEUE.popleft()
        text = await fetch_from_url(client=client, url=f'{BASE_URL}{url}')
        soup = BeautifulSoup(text, 'html.parser')
        if soup.find('thead'):
            # then there is page with list of links
            append_to_queue(soup=soup, len_source=len(url))
        else:
            # else there is page with one file
            await download_file(client=client, soup=soup, idx=idx)


def append_to_queue(soup: BeautifulSoup, len_source: int) -> None:
    """
    Search urls with `/src` in <a>-tags.

    :param soup: html text
    :param len_source: length of source url
    :return: None
    """
    for a_tag in soup.find('tbody').findAll('a'):
        url = a_tag.get('href', '')
        if '/src/' in url and len(url) > len_source:
            QUEUE.append(url)


async def download_file(client: aiohttp.ClientSession, soup: BeautifulSoup, idx: int) -> None:
    """
    Search urls contains `/raw`.
    Write content to the corresponding file in directory FOLDER_TO_SAVE.

    :param client: async http client
    :param soup: html text
    :param idx: number of worker
    :return: None
    """
    for a_tag in soup.findAll('a'):
        url = a_tag.get('href', '')
        if '/raw/' in url:
            path = os.path.join(FOLDER_TO_SAVE, url.split(BRANCH_NAME)[-1][1:])
            logger.info(f'Worker_{idx} starts downloading to {path!r}')
            text = await fetch_from_url(client=client, url=f'{BASE_URL}{url}')
            await write_to_disk(path=path, text=text)
            return


async def fetch_from_url(client: aiohttp.ClientSession, url: str) -> bytes:
    """
    Download bytestring from `url` and return it (3 attempts to download).

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


async def write_to_disk(path: str | Path, text: bytes) -> None:
    """
    Write bytestring to `path` (create all necessary folders).

    :param path: path with filename
    :param text: text
    :return: None
    """
    folder = os.path.dirname(path)
    if folder and not os.path.exists(folder):
        await aiofiles.os.makedirs(folder, exist_ok=True)
    async with aiofiles.open(path, 'wb') as file:
        await file.write(text)


async def download_repo() -> None:
    """
    Fill `QUEUE` and create async tasks that downloads files.

    :return: None
    """
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(5)) as client:
        text = await fetch_from_url(client=client, url=FULL_URL)
        soup = BeautifulSoup(text, 'html.parser')
        append_to_queue(soup=soup, len_source=len(LOCAL_URL))

        tasks = [analyze_and_download(client, idx) for idx in range(1, NUMBER_OF_WORKERS + 1)]
        await asyncio.gather(*tasks)


def hash_print(folder: str | Path) -> None:
    """
    Print SHA256 hash of each file in `folder` (recursively).

    :param folder: folder
    :return: None
    """
    for dirpath, _, filenames in os.walk(folder):
        for filename in filenames:
            hash_obj = hashlib.sha256()
            path = os.path.join(dirpath, filename)
            with open(path, 'rb') as file_obj:
                for row in file_obj:
                    hash_obj.update(row)
            print('FILE:', path)
            print('SHA256:', hash_obj.hexdigest())


if __name__ == '__main__':
    asyncio.run(download_repo())
    hash_print(FOLDER_TO_SAVE)
