
import os
import io
import time
import boto3
import scrapy
import urllib
import os.path
import logging
import hashlib
import requests

from bs4         import BeautifulSoup
from difflib     import SequenceMatcher 
from itertools   import cycle, tee
from collections import Counter
from slugify     import slugify
from PIL         import Image
from selenium    import webdriver
from time        import sleep
from shutil      import copyfile
from time        import gmtime, strftime

from scrapy.http  import Request
from urllib.parse import urlparse
from pathvalidate import sanitize_filename

from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError
from twisted.internet.error import TimeoutError

import logging
from scrapy.utils.log import configure_logging

# to force timeouts
import eventlet
eventlet.monkey_patch(socket=True)

import urllib3
import certifi
http = urllib3.PoolManager(cert_reqs='CERT_REQUIRED', ca_certs=certifi.where())



configure_logging(install_root_handler=False)
logging.basicConfig(
    filename='./scrapy_log.txt',
    format='%(levelname)s: %(message)s',
    level=logging.INFO
)

class COLORS:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

class TekelSpiderItem(scrapy.Item):
	title = scrapy.Field()
	description = scrapy.Field()
	file_urls = scrapy.Field() #required
	files = scrapy.Field() #required
	# image_urls = scrapy.Field()
	# images = scrapy.Field()

class SeleniumMiddleware(object):

    def __init__(self):
        self.driver = webdriver.Firefox() # Or whichever browser you want

    # Here you get the request you are making to the urls which your LinkExtractor found and use selenium to get them and return a response.
    def process_request(self, request, spider):
        self.driver.get(request.url)
        body = self.driver.page_source
        return scrapy.http.HtmlResponse(self.driver.current_url, body=body, encoding='utf-8', request=request)

class TekelScraper(scrapy.Spider):
    name = "TekelSpider"
    log_file = None
    
    custom_settings = {
        'HTTPCACHE_ENABLED' : True,
        'HTTPCACHE_EXPIRATION_SECS' : 0, # Set to 0 to never expire
        'HTTPCACHE_DIR' : os.getenv("SCRAPY_CACHE"), # os.path.join("./", "scrapy_cache"),
        'HTTPCACHE_GZIP' : True,
        'EXTENSIONS' : {'scrapy.extensions.closespider.CloseSpider': 300},
        'CLOSESPIDER_PAGECOUNT' : 500,
        'DOWNLOAD_TIMEOUT' : 10,
        'DOWNLOAD_MAXSIZE' : 26843545,
        'RETRY_ENABLED' : False,
        'RETRY_TIMES' : 0,
        # 'DOWNLOADER_MIDDLEWARES' : {'tekelspider.SeleniumMiddleware': 543, 'scrapy.downloadermiddlewares.httpcache.HttpCacheMiddleware': 300, 'scrapy.downloadermiddlewares.downloadtimeout.DownloadTimeoutMiddleware' : 600}
        'DOWNLOADER_MIDDLEWARES' : {'scrapy.downloadermiddlewares.httpcache.HttpCacheMiddleware': 300, 'scrapy.downloadermiddlewares.downloadtimeout.DownloadTimeoutMiddleware' : 600}

        # 'DOWNLOADER_MIDDLEWARES' : {'tekellib.tekelspider.SeleniumMiddleware': 543},
        # 'ITEM_PIPELINES' : {'scrapy.pipelines.files.FilesPipeline': 1},
        # 'FILES_STORE' : os.path.join(DATA_FOLDER, "images")
        # 'IMAGES_EXPIRES' : 90,
        # 'IMAGES_THUMBS' : {'small': (50, 50), 'big': (270, 270)},
        # 'IMAGES_MIN_HEIGHT' : 110,
        # 'IMAGES_MIN_WIDTH' : 110
    }

    
    def print_dbg(self, s):
        print(str(s))

        if TekelScraper.log_file:
            with open(TekelScraper.log_file, "a") as myfile:
                myfile.write(strftime("%Y-%m-%d %H:%M:%S", gmtime()) + ":>  " + str(s) + "\r\n")

    def __init__(self, full_url, folder, err_file, cache_folder, s3bucket, s3path, use_webdriver):
        
        TekelScraper.custom_settings['HTTPCACHE_DIR'] = os.path.join(cache_folder, "scrapy_cache")

        if (use_webdriver):
            print("USING WEBDRIVER")
            TekelScraper.custom_settings['DOWNLOADER_MIDDLEWARES'] = {'tekelspider.SeleniumMiddleware': 543, 'scrapy.downloadermiddlewares.httpcache.HttpCacheMiddleware': 300, 'scrapy.downloadermiddlewares.downloadtimeout.DownloadTimeoutMiddleware' : 600}
        else:
            print("NOT USING WEBDRIVER")
            TekelScraper.custom_settings['DOWNLOADER_MIDDLEWARES'] = {'scrapy.downloadermiddlewares.httpcache.HttpCacheMiddleware': 300, 'scrapy.downloadermiddlewares.downloadtimeout.DownloadTimeoutMiddleware' : 600}
            TekelScraper.custom_settings['EXTENSIONS'] = {'scrapy.extensions.closespider.CloseSpider': 300}
            

        logging.getLogger('boto3').setLevel(logging.ERROR)
        logging.getLogger('botocore').setLevel(logging.ERROR)
        logging.getLogger('nose').setLevel(logging.ERROR)
        logging.getLogger('s3transfer').setLevel(logging.ERROR)
        # logging.getLogger('urllib3').setLevel(logging.ERROR)
        # logging.getLogger('scrapy').setLevel(logging.ERROR)

        self.cache_folder = cache_folder
        self.count = 0
        self.folder = folder
        self.err_file = err_file
        self.error_count = 0
        self.pages_parsed = 0
         
        self.s3 = None
        self.s3bucket = s3bucket
        self.s3path = s3path

        if s3bucket:
            self.s3 = boto3.client('s3', aws_access_key_id=os.getenv("AWS_ID"), aws_secret_access_key=os.getenv("AWS_SECRET_KEY"))

        try:
            if not full_url.startswith("http"):
                full_url = "http://" + full_url

            full_url = urlparse(full_url, 'http')
            
            base_url = full_url.hostname
            self.allowed_domains = [base_url]
            
            
            self.url_path = full_url.path
            if not self.url_path.endswith("/"):
                self.url_path = self.url_path + "/"

            if base_url.startswith('www.'):
                self.allowed_domains.append(base_url[4:])

            self.start_urls = [
                full_url.geturl()
            ]


        except Exception as e:
            self.print_dbg(COLORS.FAIL + "Exception in init: " + str(e) + COLORS.ENDC)
            self.error_count = self.error_count + 10
            self.errback_httpbin("Exception in init" + str(e))
    
    def errback_httpbin(self, failure):
        # log all errback failures,
        # in case you want to do something special for some errors,
        # you may need the failure's type
        self.error_count = self.error_count + 1

        #if self.error_count > 5:
        #    with open(self.err_file, 'a') as fd:
        #        fd.write(repr(failure) + "\r\n")

    def url_to_local_path(self, url):
        url_path = urlparse(url).path
        local_folder = urllib.request.url2pathname(url_path)
        local_folder = self.folder + local_folder

        final_file = url_path.rsplit('/', 1)[-1]
        extension = final_file.rfind('.')
        if extension == -1:
            image_folder = os.path.join(local_folder, "images", final_file)
            filename = os.path.join(local_folder, 'index.html')
        else:
            # it has something like default.aspx
            filename = local_folder
            local_folder = local_folder.rstrip(final_file)
            image_folder = os.path.join(local_folder, "images", final_file.rstrip(final_file[extension:]))

        # create website folder and images folders if they don't exist
        if not os.path.exists(local_folder):
            os.makedirs(local_folder)
        
        if not os.path.exists(image_folder):
            os.makedirs(image_folder)

        remote_filename = filename[len(self.folder) : ]
        return filename, remote_filename, image_folder


    def parse(self, response):
        self.print_dbg("Entered Parse Function")

        try:

            response = response.replace(encoding='utf8', body=response.text.encode('utf8'))

            self.count = self.count + 1
            
            local_filename, remote_filename, image_folder = self.url_to_local_path(response.url)

            self.print_dbg("Downloading URL: {} to {}, {}".format(response.url, local_filename, image_folder))

            with open(local_filename, 'wb') as f:
                f.write(response.body)

                if self.s3:
                    s3file = self.s3path + remote_filename
                    self.print_dbg("Uploading to S3 bucket {}. s3 file= {} ".format(self.s3bucket, s3file))
                    self.s3.upload_file(local_filename, self.s3bucket, s3file)
            
            # response.replace(encoding='UTF-8')

            self.print_dbg("Extracting Image Links")
            pageImages = LinkExtractor(
                tags=['img'], attrs=['src'], deny_extensions=[], canonicalize=False)

            self.print_dbg("Done Extracting Image Links")

            # self.print_dbg(COLORS.OKBLUE + "Images:", pageImages.extract_links(response), COLORS.ENDC)

            for image in pageImages.extract_links(response):
                
                # Will download the images - it caches images and does not download same url twice

                self.print_dbg("Looping over image: " + image.url)

                image_url = image.url
                image_url_hash = hashlib.sha224(str(image_url).encode('utf-8')).hexdigest()
                filename = sanitize_filename(image_url[image_url.rfind("/")+1:])
                image_path = os.path.join(image_folder, filename)
                cache_file = os.path.join(os.path.join(self.cache_folder, "image_cache"), image_url_hash)
                file_exists = os.path.exists(image_path)

                if os.path.exists(cache_file) and not file_exists:
                    # This file has been cached, no need to redownload, just copy
                    copyfile(cache_file, image_path)
                elif not file_exists: # do not redownload if image exists on drive
                    try:
                        self.print_dbg("Downloading image:" + image_url)

                        # with eventlet.Timeout(10):
                        #    image_content = requests.get(image_url, timeout=(5, 10), verify=False).content
                        r = http.request('GET', image_url, 
                                         preload_content=False,
                                         timeout=urllib3.Timeout(connect=5.0, read=10.0))

                        with open(image_path, 'wb') as out:
                            while True:
                                data = r.read(65536)
                                if not data:
                                    break
                                out.write(data)

                        r.release_conn()
                
                        # Write the image
                        #f = open(image_path, 'wb')
                        #f.write(image_content)
                        #f.close()
                        
                        self.print_dbg("Image download complete. Written to: " + image_path)

                        # Copy to cache 
                        copyfile(image_path, cache_file)
                    except Exception as e:
                        self.print_dbg(COLORS.FAIL + "Exception: " + str(e) + COLORS.ENDC)

                if self.s3:
                    remote_imgname = image_path[len(self.folder) : ]
                    s3imgfile = self.s3path + remote_imgname
                    self.print_dbg("Uploading image to S3 bucket: {} ".format(s3imgfile))
                    self.s3.upload_file(image_path, self.s3bucket, s3imgfile)


            self.pages_parsed = self.pages_parsed + 1

            self.print_dbg("Extracting Links")
            link_extractor = LinkExtractor(allow=self.url_path + ".*")
            for link in link_extractor.extract_links(response):

                self.print_dbg(COLORS.OKGREEN + "Yielding: ->" + link.url + COLORS.ENDC)
                yield scrapy.Request(
                    link.url,
                    callback=self.parse,
                    errback=self.errback_httpbin
                )
                # self.print_dbg("Yielded")
                # response.follow(link, self.parse)
        except Exception as e:
            
            self.print_dbg(COLORS.FAIL + "Exception in parse: " + str(e) + COLORS.ENDC)

            self.print_dbg((response.headers, response.body[:1024]))
            # If we have parsed a lot of pages, reduce the error count
            # from exceptions here
            if self.pages_parsed > 20:
                self.error_count = self.error_count - 1

            self.error_count = self.error_count + 1
            # self.errback_httpbin("Exception in parse" + str(e))
