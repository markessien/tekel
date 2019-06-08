import scrapy, os, urllib, time, os.path, requests, requests_cache, urllib.parse

from tekellib.parser import Parser
from tekellib.settings import DATA_FOLDER, COLORS
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from selenium import webdriver
from scrapy.linkextractors import LinkExtractor
from pathvalidate import sanitize_filename

class FbMiddleware(object):

    def __init__(self):
        self.driver = webdriver.Firefox() # Or whichever browser you want

    # Here you get the request you are making to the urls which your LinkExtractor found and use selenium to get them and return a response.
    def process_request(self, request, spider):
        self.driver.get(request.url)

        """
        try:
            element = self.driver.find_element_by_xpath("//a[@class='_4-eo _2t9n']")
            print("Found items")
            #if element:
            #    print("\r\n\r\nFound Element!!!\r\n\r\n")
            element.click()
        except:
            print("Item _4-eo not found")
            pass
        """

    
        print("Scrolling to end of page")
        len_of_page = self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);var lenOfPage=document.body.scrollHeight;return lenOfPage;")
        match=False
        while (match == False):
            last_count = len_of_page
            time.sleep(1)
            print("Scrolling to end of page")
            len_of_page = self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);var lenOfPage=document.body.scrollHeight;return lenOfPage;")
            if last_count==len_of_page:
                match=True

        body = self.driver.page_source
        return scrapy.http.HtmlResponse(self.driver.current_url, body=body, encoding='utf-8', request=request)


class TekelFbScraper(scrapy.Spider):
    name = "TekelFbScraper"
    
    custom_settings = {
        'HTTPCACHE_ENABLED' : True,
        'HTTPCACHE_EXPIRATION_SECS' : 0, # Set to 0 to never expire
        'HTTPCACHE_DIR' : os.path.join(DATA_FOLDER, "cache"),
        'HTTPCACHE_GZIP' : True,
        'DOWNLOADER_MIDDLEWARES' : {'tekellib.tekelspider_fb.FbMiddleware': 543, 'scrapy.downloadermiddlewares.httpcache.HttpCacheMiddleware': 300},
        'EXTENSIONS' : {'scrapy.extensions.closespider.CloseSpider': 300},
        'CLOSESPIDER_PAGECOUNT' : 500
    }


    def __init__(self, full_url, folder):
        
        self.parsed_page = 0
        self.folder = folder
        self.start_url = full_url
        self.start_urls = [
            full_url
        ]
        self.cur_scrape_pos = 0

        requests_cache.install_cache(os.path.join(DATA_FOLDER, 'tekel_cache'))

        if not os.path.exists(self.folder):
            os.makedirs(self.folder)

    def handle_single_photo(self, response):
        soup = BeautifulSoup(response.body, "lxml")
        img_links = soup.find_all("a", "ci")
        for img_link in img_links:
            # print(img_link)
            image_url = img_link['href']
            print("Found fullsize image: " + image_url)
            image_content = requests.get(image_url).content

            # filename = sanitize_filename(image_url[image_url.rfind("/")+1:])
            filename = Parser.image_url_to_filename(image_url)
            image_file = os.path.join(self.folder, "photos", filename)
            f = open(image_file, 'wb')
            f.write(image_content)
            f.close()

    def parse(self, response):

        meta = response.meta

        # "index", "about", "reviews", "posts", "photos", "events", "videos", "ads"
        order = ["index", "photos"]

        print("Position: " + str(self.cur_scrape_pos))

        if "single_photo" in meta:
            filename  = os.path.join(self.folder, str(meta["single_photo"]))
            image_folder = os.path.join(self.folder, "photos")
        elif "single_photo_zoom" in meta:
            filename  = os.path.join(self.folder, str(meta["single_photo_zoom"]))
            image_folder = os.path.join(self.folder, "photos")
        else:
            filename  = os.path.join(self.folder, order[self.cur_scrape_pos] + ".html")
            image_folder = os.path.join(self.folder, order[self.cur_scrape_pos])

        print("Parsing: " + filename)

        with open(filename, 'wb') as f:
            f.write(response.body)

        html = response.body


        if "single_photo" in meta:
            self.handle_single_photo(response)

        elif order[self.cur_scrape_pos] == "photos":
            # print("scrapingphotos")
            if not os.path.exists(image_folder):
                os.makedirs(image_folder)

            soup = BeautifulSoup(html, "lxml")
            sub_pages = soup.find_all("a", {'rel': True})
            img_number = 1
            for page in sub_pages:
                if img_number < 100:
                    #print(page.get('rel'))
                    if "theater" in page.get('rel'):
                        mobile_url = page['href'].replace("https://www.", "https://m.")
                        print("URL:" + mobile_url)
                    
                        yield scrapy.Request(
                            urllib.parse.urljoin(self.start_url, mobile_url),
                            callback=self.parse,
                            meta={'single_photo': "photo_" + str(img_number) + ".html", 'photo_nmbr' : img_number}
                        )
                        
                        img_number = img_number + 1
                        # print(page['href'])
            """
            imgs = soup.findAll('img')
            for img in imgs:
                # print(img['src'] + ":")
                try:
                    image_content = requests.get(img['src']).content
                    filename = sanitize_filename(os.path.basename(urlparse(img['src']).path))
                    f = open(os.path.join(image_folder, filename), 'wb')
                    f.write(image_content)
                    f.close()

                    if 'alt' in img:
                        print(img['alt'])
                    pass
                except Exception as e:
                    print("Exception when downloading images: " + str(e))
            """

        if not "single_photo" in meta:
            print("Incrementing")
            self.cur_scrape_pos = self.cur_scrape_pos + 1

        if self.cur_scrape_pos < len(order) and not "single_photo" in meta:
            yield scrapy.Request(
                self.start_url + order[self.cur_scrape_pos],
                callback=self.parse
            )
