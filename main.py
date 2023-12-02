import asyncio
import os
import shlex
import subprocess
import sys
from time import time
import aiohttp
import requests


class PartErrorsOccurred(Exception):
    ...


async def fetch(session: aiohttp.ClientSession, url, queue):
    async with session.get(url) as response:
        await queue.put(1)
        return await response.content.read()


async def fetch_all(urls, queue):
    connector = aiohttp.TCPConnector(limit=100, force_close=True)
    async with aiohttp.ClientSession(connector=connector) as session:
        results = await asyncio.gather(*[fetch(session, url, queue) for url in urls], return_exceptions=True)
        results_clear = list(filter(filter_error_parts, results))
        await queue.put(None)
        return results_clear, len(results) - len(results_clear)


def filter_error_parts(content):
    if isinstance(content, Exception):
        return False
    else:
        return True


def get_url_pool(url):
    response = requests.get(url)
    lines = response.text.split('\n')
    pool = []
    for line in lines:
        if line[0:5] == 'https':
            pool.append(line)

    return pool


def remove_file(path):
    if os.path.exists(path):
        os.remove(path)


def get_duration(video):
    result = subprocess.run(shlex.split(f'ffprobe -i {video} -show_entries format=duration -v quiet -of csv="p=0"'),
                            capture_output=True)
    return round(float(result.stdout.decode()) * 1e6)


def clp():
    sys.stdout.write("\x1b[1F")


def clc():
    sys.stdout.write("\33[2K\r")


def render(t, path):
    duration = get_duration(timestamp_path)
    process = subprocess.Popen(shlex.split(f'ffmpeg -progress - -nostats -threads 2 -i video.ts {path}'),
                               shell=False,
                               stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)

    for line in iter(process.stdout.readline, b''):
        text = line.decode()
        if text[0:11] == 'out_time_ms':
            clc()
            current_time = int(text.split('=')[1].strip())
            print(f"Converting mp4...  {current_time / duration:.0%}, {current_time / 1e6:.0f} / {duration / 1e6:.0f}, "
                  f"{time() - t:.2f}", end='')
    print()


async def consumer(all, queue, t):
    cnt = 0
    while True:
        item = await queue.get()
        if item is None:
            break

        cnt += 1
        clc()
        print(f'Loading content... {cnt / all:.0%}, {cnt}/{all}, {time() - t:.2f}', end='')
    print()


async def get_content(pool, t):
    queue = asyncio.Queue()
    clc()
    return (await asyncio.gather(fetch_all(pool, queue), consumer(len(pool), queue, t)))[0]


def save_timestamp_video(all_content):
    remove_file(timestamp_path)
    with open(timestamp_path, 'ab') as file:
        for content in all_content:
            file.write(content)


async def full_download(url, path):
    print(f'Start downloading {path}...')

    t = time()
    pool = get_url_pool(url)
    print(f'Get url pool --- {time() - t:.2f}')

    t = time()
    all_content, errors = await get_content(pool, t)
    print(f'Load content --- {time() - t:.2f}, {errors=}')
    if errors:
        raise PartErrorsOccurred

    t = time()
    save_timestamp_video(all_content)
    print(f'Save video.ts --- {time() - t:.2f}')

    t = time()
    remove_file(path)
    render(t, path)
    print(f'Convert mp4 --- {time() - t:.2f}')
    print('Done')

    return errors


async def main():
    urls = [
        ('videos/module2/lesson1-1.mp4',
         'https://player02.getcourse.ru/player/e29f9a701dea5171dab0fc761fdc6fe6/29ed25dcc6058dfe1201fa4025813e70/media/360.m3u8?sid=&user-cdn=cdnvideo&version=8%3A2%3A1%3A0%3Acdnvideo&user-id=314601242&jwt=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ1c2VyLWlkIjozMTQ2MDEyNDJ9.mry-H5abbxz9dG8DwAC895Kfh_v71y_E1QbldxGvXh0'),
        ('videos/module2/lesson1-2.mp4',
         'https://player02.getcourse.ru/player/06f40fa468038812af31de0c89a696c5/624ce4083a7558bddb7234952399637a/media/360.m3u8?sid=&user-cdn=cdnvideo&version=8%3A2%3A1%3A0%3Acdnvideo&user-id=314601242&jwt=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ1c2VyLWlkIjozMTQ2MDEyNDJ9.mry-H5abbxz9dG8DwAC895Kfh_v71y_E1QbldxGvXh0'),
        ('videos/module2/lesson2.mp4',
         'https://player02.getcourse.ru/player/c0f1b037e082ad2d15c144f453d5bd81/b88122f381f420a930763ba219aa1b39/media/360.m3u8?sid=&user-cdn=cdnvideo&version=8%3A2%3A1%3A0%3Acdnvideo&user-id=301792886&jwt=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ1c2VyLWlkIjozMDE3OTI4ODZ9.NFyAiVzpajL7R-jGvshfuKC7YWP5GBmTbjXfRXukCwk'),
        ('videos/module2/lesson3.mp4',
         'https://player02.getcourse.ru/player/6ea805ce0fa846cc4df5cd6cf41491a0/e03eecaf72977220d09779b03c62d9b1/media/360.m3u8?sid=&user-cdn=cdnvideo&version=8%3A2%3A1%3A0%3Acdnvideo&user-id=301802176&jwt=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ1c2VyLWlkIjozMDE4MDIxNzZ9.pNR3-WzlXAkLLafWWZEmq'),
        ('videos/module2/lesson4.mp4',
         'https://player02.getcourse.ru/player/ea872160dee1dde499d1d605a1a97faa/9774e31312982b613589e5db18791aa6/media/360.m3u8?sid=&user-cdn=cdnvideo&version=8%3A2%3A1%3A0%3Acdnvideo&user-id=301802176&jwt=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ1c2VyLWlkIjozMDE4MDIxNzZ9.pNR3-WzlXAkLLafWWZEmq'),
        ('videos/module2/lesson5.mp4',
         'https://player02.getcourse.ru/player/aad53d50816bc867e4d81aa77ab75c47/f0abd81b8e3f2ef76342c68a68a16f79/media/360.m3u8?sid=&user-cdn=cdnvideo&version=8%3A2%3A1%3A0%3Acdnvideo&user-id=301791546&jwt=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ1c2VyLWlkIjozMDE3OTE1NDZ9.hGtASTx2rydHXABThVwguMbx4XMOVlkcmTBmEpeDJKU'),
        ('videos/module2/lesson6.mp4',
         'https://player02.getcourse.ru/player/7bec5f1f8024d356f0286a860d178e66/db6969adef9f44ffc867d6df2a782162/media/360.m3u8?sid=&user-cdn=cdnvideo&version=8%3A2%3A1%3A0%3Acdnvideo&user-id=301802176&jwt=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ1c2VyLWlkIjozMDE4MDIxNzZ9.pNR3-WzlXAkLLafWWZEmq'),
        ('videos/module2/lesson7-1.mp4',
         'https://player02.getcourse.ru/player/3e8b9a41e12ae86ce0e99af2839371b1/445f53bdba013cb57b97f8167db73d22/media/360.m3u8?sid=&user-cdn=cdnvideo&version=8%3A2%3A1%3A0%3Acdnvideo&user-id=301802176&jwt=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ1c2VyLWlkIjozMDE4MDIxNzZ9.pNR3-WzlXAkLLafWWZEmq'),
        ('videos/module2/lesson7-2.mp4',
         'https://player02.getcourse.ru/player/a20078fb87ca92f447a2416e147dc746/894af42fd00314be663041ea8407966e/media/360.m3u8?sid=&user-cdn=cdnvideo&version=8%3A2%3A1%3A0%3Acdnvideo&user-id=301802176&jwt=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ1c2VyLWlkIjozMDE4MDIxNzZ9.pNR3-WzlXAkLLafWWZEmq'),
        ('videos/module3/lesson1.mp4',
         'https://player02.getcourse.ru/player/ad756aefad20267eb5e266cd1c8c03ea/11a93871228bebbdf94ea19b61ec022e/media/360.m3u8?sid=&user-cdn=integros&version=8%3A2%3A1%3A0%3Aintegros&user-id=301802176&jwt=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ1c2VyLWlkIjozMDE4MDIxNzZ9.pNR3-WzlXAkLLafWWZEmqOjjiyfBhXFFcOojYmMvfCI'),
        ('videos/module3/lesson2-1.mp4',
         'https://player02.getcourse.ru/player/a7647de604b26e6924b46f8225978c91/b3202091952ee0aa441413744e67daa0/media/360.m3u8?sid=&user-cdn=integros&version=8%3A2%3A1%3A0%3Aintegros&user-id=301802176&jwt=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ1c2VyLWlkIjozMDE4MDIxNzZ9.pNR3-WzlXAkLLafWWZEmqOjjiyfBhXFFcOojYmMvfCI'),
        ('videos/module3/lesson2-2.mp4',
         'https://player02.getcourse.ru/player/b9a51b3cab8b43905a33c5a39785dbf0/af3ac158e224db8388f82e3c7792957c/media/360.m3u8?sid=&user-cdn=integros&version=8%3A2%3A1%3A0%3Aintegros&user-id=301802176&jwt=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ1c2VyLWlkIjozMDE4MDIxNzZ9.pNR3-WzlXAkLLafWWZEmqOjjiyfBhXFFcOojYmMvfCI'),
        ('videos/module3/lesson4.mp4',
         'https://player02.getcourse.ru/player/7bef24f31077d6930c0533b1fc5aebc5/0d1e41c97ba8497d55a095a27231e6d7/media/360.m3u8?sid=&user-cdn=integros&version=8%3A2%3A1%3A0%3Aintegros&user-id=301802176&jwt=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ1c2VyLWlkIjozMDE4MDIxNzZ9.pNR3-WzlXAkLLafWWZEmqOjjiyfBhXFFcOojYmMvfCI'),
        ('videos/module4/lesson1.mp4',
         'https://player02.getcourse.ru/player/338787a50c309ae872b090ab55dbba41/8ccd9deb5ff6aab458c396ea7e3a0de4/media/360.m3u8?sid=&user-cdn=integros&version=8%3A2%3A1%3A0%3Aintegros&user-id=301802176&jwt=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ1c2VyLWlkIjozMDE4MDIxNzZ9.pNR3-WzlXAkLLafWWZEmqOjjiyfBhXFFcOojYmMvfCI'),
        ('videos/module4/lesson2.mp4',
         'https://player02.getcourse.ru/player/3a5ebb6a6b866a5015a1ff3362a741d3/eb8c6587201a12164f56f8243440dd5d/media/360.m3u8?sid=&user-cdn=integros&version=8%3A2%3A1%3A0%3Aintegros&user-id=301802176&jwt=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ1c2VyLWlkIjozMDE4MDIxNzZ9.pNR3-WzlXAkLLafWWZEmqOjjiyfBhXFFcOojYmMvfCI'),
        ('videos/module4/lesson3.mp4',
         'https://player02.getcourse.ru/player/c2dab3a46ec971eb7dd65140b6390437/9af8468277316868e6fc314d7602a2f8/media/360.m3u8?sid=&user-cdn=integros&version=8%3A2%3A1%3A0%3Aintegros&user-id=301802176&jwt=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ1c2VyLWlkIjozMDE4MDIxNzZ9.pNR3-WzlXAkLLafWWZEmqOjjiyfBhXFFcOojYmMvfCI'),
        ('videos/module4/lesson4.mp4',
         'https://player02.getcourse.ru/player/661a6180dbc76570f28e6f995df78326/4ea7de22a8fad1941134441dcd468da4/media/360.m3u8?sid=&user-cdn=integros&version=8%3A2%3A1%3A0%3Aintegros&user-id=301802176&jwt=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ1c2VyLWlkIjozMDE4MDIxNzZ9.pNR3-WzlXAkLLafWWZEmqOjjiyfBhXFFcOojYmMvfCI'),
        ('videos/module4/lesson5.mp4',
         'https://player02.getcourse.ru/player/b9d23a4f1c505fbaf03fa41b80598ade/3114cf9412daaafbd641a624051f40f4/media/360.m3u8?sid=&user-cdn=integros&version=8%3A2%3A1%3A0%3Aintegros&user-id=301802176&jwt=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ1c2VyLWlkIjozMDE4MDIxNzZ9.pNR3-WzlXAkLLafWWZEmqOjjiyfBhXFFcOojYmMvfCI'),
        ('videos/module5/lesson1.mp4',
         'https://player02.getcourse.ru/player/167a529ae040bd3aec5949bb550035bf/85b48f9ba931603ca9e67ea8217b75d2/media/360.m3u8?sid=&user-cdn=integros&version=8%3A2%3A1%3A0%3Aintegros&user-id=301802176&jwt=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ1c2VyLWlkIjozMDE4MDIxNzZ9.pNR3-WzlXAkLLafWWZEmqOjjiyfBhXFFcOojYmMvfCI'),
        ('videos/module5/lesson2.mp4',
         'https://player02.getcourse.ru/player/2024dc13447039db957d1c2c74a84e99/ead14b27f1a59cb7b743ad1b99422321/media/360.m3u8?sid=&user-cdn=integros&version=8%3A2%3A1%3A0%3Aintegros&user-id=301802176&jwt=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ1c2VyLWlkIjozMDE4MDIxNzZ9.pNR3-WzlXAkLLafWWZEmqOjjiyfBhXFFcOojYmMvfCI'),
        ('videos/module6/lesson1.mp4',
         'https://player02.getcourse.ru/player/7766a7397fdb94301d0e7cc8e1f29788/835de19f414e9577440b8e521e8dc786/media/360.m3u8?sid=&user-cdn=integros&version=8%3A2%3A1%3A0%3Aintegros&user-id=301802176&jwt=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ1c2VyLWlkIjozMDE4MDIxNzZ9.pNR3-WzlXAkLLafWWZEmqOjjiyfBhXFFcOojYmMvfCI'),
        ('videos/module6/lesson2.mp4',
         'https://player02.getcourse.ru/player/0490db7b4f1cb2bd251e5241a96fb94b/c4f3f626e0fa57c345e5b67775656ad1/media/360.m3u8?sid=&user-cdn=integros&version=8%3A2%3A1%3A0%3Aintegros&user-id=301802176&jwt=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ1c2VyLWlkIjozMDE4MDIxNzZ9.pNR3-WzlXAkLLafWWZEmqOjjiyfBhXFFcOojYmMvfCI'),
        ('videos/module6/lesson3.mp4',
         'https://player02.getcourse.ru/player/9ad5c189330161ad0803b6dd46f8dd5c/d9147ac2d097056ed5f7162448529d6a/media/360.m3u8?sid=&user-cdn=integros&version=8%3A2%3A1%3A0%3Aintegros&user-id=301802176&jwt=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ1c2VyLWlkIjozMDE4MDIxNzZ9.pNR3-WzlXAkLLafWWZEmqOjjiyfBhXFFcOojYmMvfCI'),
        ('videos/module6/lesson4.mp4',
         'https://player02.getcourse.ru/player/40cc68d66c32d2a4e6be4fafbbf40ba0/f9ceda64b5141e194c6299a9b1b235f1/media/360.m3u8?sid=&user-cdn=integros&version=8%3A2%3A1%3A0%3Aintegros&user-id=301802176&jwt=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ1c2VyLWlkIjozMDE4MDIxNzZ9.pNR3-WzlXAkLLafWWZEmqOjjiyfBhXFFcOojYmMvfCI'),
    ]

    partErrors = 0
    videoErrors = []
    for idx, (path, url) in enumerate(urls):
        if os.path.exists(path):
            print(f'Already downloaded {path}, {idx + 1}/{len(urls)}, {videoErrors=}, {partErrors=}')
            continue

        print(f'Downloading {idx + 1}/{len(urls)}, {videoErrors=}, {partErrors=}')
        try:
            partErrors += await full_download(url, path)
        except Exception as ex:
            videoErrors.append(idx)
            print(f'Error: {ex.__class__.__name__}')

        print('\n\n')


if __name__ == '__main__':
    timestamp_path = 'video.ts'
    asyncio.run(main())
