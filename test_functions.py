import asyncio
import hashlib
import os
import shutil

import aiohttp
import pytest
import requests
from bs4 import BeautifulSoup
import main

FILENAME = 'test.txt'


def test_append_to_queue():
    text = requests.get(main.FULL_URL).text
    soup = BeautifulSoup(text, 'html.parser')
    main.append_to_queue(soup, 0)
    assert len(main.QUEUE) == len(soup.find('tbody').findAll('tr'))


@pytest.mark.asyncio
async def test_write_to_disk(tmp_path):
    file_path = tmp_path / FILENAME
    await main.write_to_disk(path=file_path, text=FILENAME.encode())
    assert FILENAME == file_path.read_text()


def test_hash_print(tmp_path, capsys):
    folder = tmp_path
    num_nested_files = 3
    for _ in range(num_nested_files):
        folder = folder / 'test'
        folder.mkdir()
        file_path = folder / FILENAME
        file_path.write_text(FILENAME)
    main.hash_print(tmp_path)
    captured = capsys.readouterr()
    content_hash = hashlib.sha256(FILENAME.encode()).hexdigest()
    assert len(captured.out.split(content_hash)) == num_nested_files + 1


@pytest.mark.asyncio
async def test_fetch_from_url():
    async with aiohttp.ClientSession() as client:
        bytes_text = await main.fetch_from_url(client=client, url=main.FULL_URL)
        assert main.LOCAL_URL.encode() in bytes_text


def test_download_repo():
    main.FOLDER_TO_SAVE = os.path.join('.pytest_cache', 'temporary')
    asyncio.run(main.download_repo())
    assert os.path.exists(os.path.join(main.FOLDER_TO_SAVE, 'README.md'))
    shutil.rmtree(main.FOLDER_TO_SAVE)
