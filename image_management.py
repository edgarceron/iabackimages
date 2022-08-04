import boto3, os, io, aiofiles
import asyncio
import logging
from pathlib import Path
from time import time
from PIL import Image

import aiohttp
from smart_open import open

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


async def async_download_link(aioSession: aiohttp.ClientSession,label: str, link: str):
    """
        Async version of the download_link method we've been using in the other examples.
        :param session: aiohttp ClientSession
        :param label: label beeing looked for
        :param link: the url of the link to download
        :return:
    """
    download_path = '/tmp/' + os.path.basename(link).strip()
    os.makedirs('/tmp/', exist_ok=True) 
    s3Session = boto3.Session(
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    )
    url = 's3://{}/{}/{}'.format(
        os.getenv('AWS_BUCKET_NAME'),
        label,
        os.path.basename(link)
    )
    
    logger.info('Starting %s', link)
    try:
        async with aioSession.get(link) as response:
            async with aiofiles.open(download_path, "wb") as buffer:
                while True:
                    chunk = await response.content.read(1024)
                    if chunk: await buffer.write(chunk)
                    else: break
        logger.info('Downloaded %s', link)
        downloaded = True
    except:
        downloaded = False
        logger.info('Fail to download %s', link)   
    
    if not downloaded: return

    try:
        logger.info('Resizing  %s', download_path)   
        with Image.open(download_path.strip()) as image:
            image = image.resize(size=(200, 200))
            image = image.convert("L")
            image.save(download_path)
    except Exception as e:
        logger.info('Fail to resize %s', download_path)   
        logger.info(e)   
        return


    logger.info('Uploading  %s', download_path)
    try:
        async with aiofiles.open(download_path, mode='rb') as f:
            with open(url, 'wb', transport_params={'client': s3Session.client('s3')}) as fout:
                while True:
                    # await pauses execution until the 1024 (or less) bytes are read from the stream
                    chunk = await f.read(1024)
                    if chunk: fout.write(chunk)
                    else: break
        logger.info('Uploaded %s', download_path)
            
    except Exception as e:
        logger.info('Fail to upload %s', download_path)   
        logger.info(e)   

    


# Main is now a coroutine
async def main(label: str, urls: list):
    # We use a session to take advantage of tcp keep-alive
    # Set a 3 second read and connect timeout. Default is 5 minutes
    async with aiohttp.ClientSession(conn_timeout=5, read_timeout=5, trust_env=True) as aioSession:
        tasks = [(async_download_link(aioSession, label, l)) for l in urls]
        # gather aggregates all the tasks and schedules them in the event loop
        await asyncio.gather(*tasks)


if __name__ == '__main__':
    s3 = boto3.resource('s3')

    label = os.getenv('LABEL_FILE')
    bucket = os.getenv('AWS_BUCKET_NAME')
    file_label = '/tmp/' + label
    s3.Bucket('iabackimages').download_file(label, file_label)
    


    file = open(file_label, 'r')
    urls = file.readlines()
    file.close()
    
    ts = time()
    # Create the asyncio event loop
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(main(label, urls))
    finally:
        # Shutdown the loop even if there is an exception
        loop.close()
    logger.info('Took %s seconds to complete', time() - ts)