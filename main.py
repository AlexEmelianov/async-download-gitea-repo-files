import asyncio
import aiohttp
from pathlib import Path
import requests
from bs4 import BeautifulSoup


BASE_URL = 'https://gitea.radium.group'
LOCAL_URL = '/radium/project-configuration'


def main(location: str, path: str) -> None:
    response = requests.get(BASE_URL + location)
    soup = BeautifulSoup(response.text, 'html.parser')
    if not soup.find('thead'):
        for a_tag in soup.findAll('a'):
            link = a_tag.get('href', '')
            if link.startswith(LOCAL_URL + '/raw'):
                response = requests.get(BASE_URL + link)
                with open(path, 'w', encoding='utf-8') as file:
                    file.write(response.text)
                return

    Path(path).mkdir(parents=True, exist_ok=True)
    for a_tag in soup.findAll('a'):
        link = a_tag.get('href', '')
        if link.startswith(LOCAL_URL + '/src') and len(link) > len(location):
            name = link.split('/')[-1]
            main(
                location=link,
                path='/'.join((path, name))
            )


if __name__ == '__main__':
    main(location=LOCAL_URL, path='temp_folder')
