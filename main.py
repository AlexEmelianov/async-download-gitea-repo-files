import asyncio
import aiohttp
import aiofiles
import aiofiles.os
import os
import requests
from bs4 import BeautifulSoup
from collections import deque
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BASE_URL = 'https://gitea.radium.group'
LOCAL_URL = '/radium/project-configuration'
BRANCH_NAME = 'master/'
FOLDER = 'temp_folder'
URLS = deque()


async def get_file(client: aiohttp.ClientSession, idx: int) -> None:
    while URLS:
        url = URLS.popleft()
        data = await get_data(client=client, url=f'{BASE_URL}{url}')
        soup = BeautifulSoup(data, 'html.parser')
        if not soup.find('thead'):
            for a_tag in soup.findAll('a'):
                link = a_tag.get('href', '')
                if link.startswith(f'{LOCAL_URL}/raw'):
                    path = os.path.join(FOLDER, link.split(BRANCH_NAME)[-1])
                    logger.info(f'Worker_{idx} download to {path!r}')
                    data = await get_data(client=client, url=f'{BASE_URL}{url}')
                    await write_to_disk(path=path, data=data)
                    break
        else:
            find_links(soup=soup, len_source=len(url))


async def get_data(client, url):
    for attempt in range(1, 4):
        async with client.get(url) as response:
            if response.status != 200:
                logger.error(f'Error! {response.status_code=} from {url!r}. {attempt=}')
            else:
                return await response.read()


async def write_to_disk(path, data):
    folder = os.path.dirname(path)
    created = await aiofiles.os.path.exists(folder)
    if not created:
        await aiofiles.os.makedirs(folder, exist_ok=True)
    async with aiofiles.open(path, 'wb') as file:
        await file.write(data)


def find_links(soup, len_source):
    global URLS
    for a_tag in soup.find('tbody').findAll('a'):
        url = a_tag.get('href', '')
        if url.startswith(f'{LOCAL_URL}/src') and len(url) > len_source:
            URLS.append(url)


async def download_repo():
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(5)) as client:
        tasks = [get_file(client, idx) for idx in range(1, 4)]
        await asyncio.gather(*tasks)


def main():
    url = f'{BASE_URL}{LOCAL_URL}'
    response = requests.get(url)
    if response.status_code != 200:
        logger.error(f'Error! {response.status_code=} from {url!r}. Message: {response.text}')
    soup = BeautifulSoup(response.text, 'html.parser')
    find_links(soup=soup, len_source=len(LOCAL_URL))
    asyncio.run(download_repo())


if __name__ == '__main__':
    main()
