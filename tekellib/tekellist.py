import os
import sys
import time
import glob
import math
import json
import boto3
import sqlite3
import mysql.connector

from tekelfeature   import TekelFeature, TekelType
from tekelobject    import TekelObject
from tekelspider    import TekelScraper
from scrapy.crawler import CrawlerProcess
from time           import gmtime, strftime
from prettytable    import PrettyTable

try:
    import pandas as pd
    from fuzzywuzzy import fuzz, process
except:
    print("Pandas not available")

sys.path.append('../bin/') # for geckodriver


class COLORS:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


class TekelList(object):
    """ This class represents a list of Tekel Objects. The
        lists are stored in a pandas object

    Attributes:
        data:           The pandas object. Avoid accessing directly
        list_name:      The name of this list
        feature_list:   The list of features of the class (a.k.a the columns)
    """

    def __init__(self, list_name = "", feature_list = None, db_type="sqlite"):

        self.idx = 0 # This is used for the iterator
        self.list_name = list_name
        self.data = pd.DataFrame()
        self.feature_list = feature_list
        self.states = None
        self.db_type = db_type
        self.cursor = None
        self.db_connection = None
        self.db_file = None
        self.table_name = None
        self.log_file = None

    def commit_db(self):
        self.db_connection.commit()

    def print_dbg(self, s):
        print(str(s))

        if self.log_file:
            with open(self.log_file, "a") as myfile:
                myfile.write(strftime("%Y-%m-%d %H:%M:%S", gmtime()) + ":>  " + str(s) + "\r\n")

    def close_db(self):
        self.db_connection.commit()
        self.db_connection.close()
        self.db_connection = None
    
    def db_exec(self, str):
        self.print_dbg("Running Query: " + str)
        self.cursor.execute(str)

    def get_cursor(self):

        if self.db_type == "sqlite":
            self.db = sqlite3.connect(db_file, timeout=10, check_same_thread=False)
            self.cursor = self.db.cursor()
            die # sqlite support is not working
        else:
            if self.db_connection == None:
                self.db_connection = mysql.connector.connect(user=self.db_user, 
                                            password=self.db_password,
                                            host=self.db_host,
                                            database=self.db_name,
                                            ssl_disabled=self.ssl_disabled)

            if self.cursor == None:
                self.cursor = self.db_connection.cursor(buffered=True)

        return self.cursor

    def flatten_json(self, path):
        # this function will take an inner json inside an item
        # and move it to the root of the list. Notrimplemented
        # but there is an implemented version in add_from_json
        pass


    def rename_column(self, original_name, new_name):
        
        # rename pandas object
        self.data.rename(columns={original_name: new_name}, inplace=True)

        # rename the associated features
        for f in self.feature_list:
            if f.feature_name == original_name:
                f.change_name(new_name)
                break

    def rename_columns(self, original_names, new_names):
        for i, column_name in enumerate(original_names):
            self.rename_column(column_name, new_names[i])
    
    def load_from_json(self, json_data_text, path_to_list_node):
        print("Not yet implemented")
        # clear the data, then call add_from_json
        pass

    # Will load the list from a json file. It expects a json file
    # made up of repeating nodes, each with level of flat data.
    # Path should be like this /root/item/item
    def add_from_json(self, json_data_text, path_to_list_node, items_to_flatten=None, on_added=None):
        
        # load the data to a dictionary
        self.json_data = json.loads(json_data_text)
        json_data = self.json_data

        # walk to the node.
        for path_item in path_to_list_node.split("/"):
            if len(path_item) > 0:
                json_data = json_data[path_item]
        

        # loop through the node items
        for json_item in json_data:

            # We flatten the json
            if items_to_flatten:
                for to_flatten in items_to_flatten:
                    sub_node = json_item
                    # to_flatten is a list of paths like this /home/geometry
                    for path_item in to_flatten.split("/"):
                        if len(path_item) > 0:
                            sub_node = sub_node[path_item]

                    json_item = {**json_item, **sub_node} # merge the dicts


            data = []
            columns = []

            item_dict = {}
            # Each item is made of key, value. The keys will form the columns
            for key, value in json_item.items():
                item_dict[str(key)] = str(value)
                columns.append(key)

            data.append(item_dict)

            # Create the pandas DataFrame 
            df = pd.DataFrame(data, columns = columns)

            if self.feature_list == None:
                self.feature_list = []
                for c in columns:
                    self.feature_list.append(TekelFeature(c, TekelType.Unknown))
            
            if len(columns) > len(self.feature_list):
                self.feature_list = []
                for c in columns:
                    self.feature_list.append(TekelFeature(c, TekelType.Unknown))

            tobj = TekelObject(df, self.feature_list)
            if on_added:
                tobj = on_added(tobj)
            self.add(tobj)
            
        

    # Will load a single file
    def load_from_csv(self, file_name, delimiter=None, columns=None):
        """ Loads data from a provided CSV file. It will auto
            detect column mames.

        Args:
            file_name: A properly formatted CSV file
        Returns:
           None
        """
        df = pd.read_csv(file_name, delimiter=delimiter, names=columns, keep_default_na=False)
        
        # print("Columns:" + str(df.columns.values()))
        self.feature_list = list(map(lambda x: TekelFeature(x, TekelType.String, False, x), df.columns.values))
        self.data = df
        self.print_dbg("Loaded file: " + file_name + " Shape:" + str(self.data.shape))

    def add(self, item):
        self.data = self.data.append(item.get_dataframe())
        c = len(self.data.index) - 1

        if self.feature_list == None:
            self.feature_list = []
        
        for feature in item.features:
            
            found = False
            for f in self.feature_list:
                if f.feature_name == feature.feature_name:
                    found = True
                    break
            
            if found == False:
                self.feature_list.append(feature)

        return c

    def save_item_to_db(self, item_index, table_name):
        self.print_dbg("Index: " + str(item_index))
        obj = TekelObject(self.data.iloc[item_index], self.feature_list)
        self.print_dbg("Saving: " + str(obj.get_value_s("hotel_name")))

        cnx = mysql.connector.connect(user=self.db_user, password=self.db_password,
                                      host=self.db_host,
                                      database=self.db_name,
                                      ssl_disabled=self.ssl_disabled)

        sql = "INSERT INTO %s(%s)" % (table_name, self.comma_features())
        sql = sql + "VALUES(%s)" % (obj.comma_values())
        self.print_dbg(sql)

        cursor = cnx.cursor()
        cursor.execute(sql)
        
        cnx.commit()
        cnx.close()

        # query_str = "SELECT * FROM %s WHERE id='%s'" % (table_name, objkey)
        #    self.cursor.execute(query_str)
        #    scanitem = self.cursor.fetchone() #retrieve the first row

        #    if not scanitem:
        # self.db.commit()
        
        


    def find(self, query_string):

        result = self.data.query(query_string)
        if result.empty:
            return None

        return TekelObject(result.iloc[0], self.feature_list)

    def set_db_details(self, db_host, db_port, db_name, db_user, db_password, db_file=None):
        self.db_host = db_host
        self.db_name = db_name
        self.db_password = db_password
        self.db_port = db_port
        self.db_user = db_user
        self.ssl_disabled = True
        self.db_file = None

        self.print_dbg("---")
        self.print_dbg("---")
        self.print_dbg("---")

    def add_column(self, column_name, default_value=None):
        self.data[column_name] = default_value
        self.sync_features()

    def sync_features(self):
        self.feature_list = list(map(lambda x: TekelFeature(x, TekelType.Unknown, False, x), self.data.columns.values))

    def update(self, id_feature_name, id_value, value_column, new_value, only_db = False):
        self.print_dbg("Update: " + str(id_value) + ". Change " + str(value_column) + " to " + str(new_value))

        if only_db == False:
            try:
                self.data.set_index(id_feature_name, inplace=True)
            except:
                pass

            item = self.data.loc[int(id_value)]
            # item_index = self.data.loc[self.data[id_feature_name] == id_value, value_column].index[0]
            item_index = item.index[0]
            self.data.at[item_index, value_column] = new_value
    
        self.cursor = self.get_cursor()
        
        query_str = "UPDATE " + self.table_name + " SET " + str(value_column) + "='" + str(new_value) + "' WHERE " + id_feature_name + "='" + str(id_value) + "'"
        self.print_dbg(query_str)
        self.db_exec(query_str)
        self.commit_db()

    def count(self, filter=None, from_db = True):
        
        if from_db == False:
            print("In-Memory Counting Not implemented")
            return

        cursor = self.get_cursor()

        if filter:
            query_str = "SELECT COUNT(*) FROM " + self.table_name + " WHERE " + filter
        else:
            query_str = "SELECT COUNT(*) FROM " + self.table_name

        cursor.execute(query_str)

        a = cursor.fetchall()
        x = a[0][0]
        return x

    def __getitem__(self, key):
        """ Used to allow [ ] access of elements

        Args:
            key: Numericakl index of item needed

        Returns:
           the TekelObject
        """
        return TekelObject(self.data[key])

    def name(self):
        return self.list_name
    
    def verbose(self, str):
        self.print_dbg(str)
    
    def comma_features(self, fields = None):
        comma_features = ""

        for i, feature in enumerate(self.feature_list):
            if not feature.table_column_name in fields:
                continue

            if i: comma_features = comma_features + ","
            comma_features = comma_features + feature.table_column_name
        return comma_features

    def load_from_db(self, table_name, order_by = None, where = None, limit=None):
    
        self.table_name = table_name
        cursor = self.get_cursor()
    
        if self.feature_list is None:

            self.db_exec('select * from ' + table_name + ' LIMIT 1')

            feature_names = list(map(lambda x: x[0], cursor.description))
            self.feature_list = list(map(lambda x: TekelFeature(x, TekelType.String, False, x), feature_names))
    
        self.data = pd.DataFrame(columns=self.feature_names())
        
        result_list = []

        query_str = "SELECT * FROM %s" % (table_name)
        
        if not where == None:
            query_str = query_str + " WHERE " + where

        if not order_by == None:
            query_str = query_str +  " ORDER BY " + order_by.strip()

        if not limit == None:
            query_str = query_str + " LIMIT " + str(limit)

        self.print_dbg("Query: " + query_str)
        self.cursor.execute(query_str)
        for i, row in enumerate(self.cursor.fetchall()):
            result_list.append(row)

            
        self.data = pd.DataFrame.from_records(result_list, columns=self.feature_names())
        self.data.set_index("id")
        
        self.print_dbg(self.data.head())
        # self.close_db()



    def get_as_json(self, start = 0, count = 10, columns=None):
        
        columns_list = list(map(lambda x: x.strip(), columns.split(",")))

        result = []
        for index, row in self.data.iterrows():
            tobj = TekelObject(row, self.feature_list)

            values_list = list(map(lambda x: tobj.get_value_s(x), columns_list))
            result.append(values_list)
            count -= 1
            if count == 0:
                break

        return result

    def generate_descriptions(self):
        
        for index, row in self.data.iterrows():
            tobj = TekelObject(row, self.feature_list)
            tobj.generate_description()
    
    
    """
	This class will take a folder and fine the images in it.
	Useful for a case where a website has been scraped.
	""" 
    
    def extract_images(self, img_folder, base_url):

        found_images = {}
        images = []
        for root, dir, files in os.walk(img_folder):
            for item in fnmatch.filter(files, "*"):

                if item.lower().endswith(('.jpg', '.jpeg', '.gif', '.png')):

                    img_path = os.path.join(root, item)[len(img_folder):]
                    self.print_dbg("TTT:" + img_path)
                    img_url = base_url + img_path.replace('\\', '/')
                    im = Image.open(os.path.join(root, item))
                    width, height = im.size

                    hash = str(imagehash.average_hash(im))
                    if not hash in found_images:
                        found_images[hash] = True

                    if width > 250:
                        image = {"filepath" : os.path.join(root, item), "width" : width, "height" : height, "url" : img_url, 'hash' : hash}
                        images.append(image)

        return images

    def set_s3_details(self, bucket, subfolder):
        self.s3bucket = bucket
        self.s3subfolder = subfolder

    def scrape_website(self, obj, field_with_name, field_with_url, field_for_progress, target_folder, backup_to_s3, cache_folder, prefix_folder, group, img_callback, html_callback, use_webdriver, check_if_downloaded):
        # function will download the full website
        self.print_dbg(COLORS.OKGREEN + "Starting website download" + COLORS.ENDC)
        self.print_dbg("Object: " + str(obj))

        self.update("id", obj.get_value_s("id"), field_for_progress, "2")
        self.commit_db()
        
        if backup_to_s3 == False:
            self.s3bucket = None

        url = obj.get_value_s(field_with_url)
        item_folder = obj.get_file_name(field_with_name)
        first_letter = item_folder[0:1].lower()
        website_folder = os.path.join(target_folder, first_letter, item_folder, prefix_folder)
        self.print_dbg(COLORS.OKBLUE + "Downloading: " + url + " to " + website_folder + COLORS.ENDC)

        crawl_complete_file = os.path.join(website_folder, item_folder + ".crawlcomplete")
        crawling_file = os.path.join(website_folder, item_folder + ".crawling")
        error_file = os.path.join(website_folder, item_folder + ".crawlerror")
        
        if not os.path.exists(website_folder):
            os.makedirs(website_folder)

        should_download = True
        if check_if_downloaded:
            if os.path.exists(crawl_complete_file):
                should_download = False

        if (should_download):    
                with open(crawling_file, 'w') as fd:
                        fd.write(str(time.time()))
                
                if os.path.exists(error_file):
                        os.remove(error_file)

                process = CrawlerProcess({'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)'}) 

                
                TekelScraper.log_file = self.log_file


                process.crawl(TekelScraper, 
                                full_url=url, 
                                folder=website_folder, 
                                err_file=error_file, 
                                cache_folder = cache_folder,
                                s3bucket=self.s3bucket, 
                                s3path=self.s3subfolder + '/' + group + "/" + item_folder + "/" + prefix_folder,
                                use_webdriver=use_webdriver)
                process.start()

                os.remove(crawling_file)

                # if error file was written, crawl not complete
                if os.path.exists(error_file):
                        self.print_dbg(COLORS.FAIL + "Last crawl ended with an error" + COLORS.ENDC)
                        
                with open(crawl_complete_file, 'w') as fd:
                        fd.write(str(time.time()))

                self.update("id", obj.get_value_s("id"), field_for_progress, "1")
                self.commit_db()
                # subprocess.run(["scrape.bat"])

        else:
                self.update("id", obj.get_value_s("id"), field_for_progress, "1")
                self.commit_db()
                self.print_dbg("Hotel website already downloaded")
                # subprocess.run(["scrape.bat"])
				
    def set_db_details_from_env(self):
        self.set_db_details(os.getenv("DB_HOST"), 
                                os.getenv("DB_PORT"), 
                                os.getenv("DB_NAME"), 
                                os.getenv("DB_USER"), 
                                os.getenv("DB_PASSWORD"))

    def save_to_db(self, table_name, unique_id_feature = None, fields_to_save=None):

        self.table_name = table_name
        cursor = self.get_cursor()
        
        for index, row in self.data.iterrows():
            # print("Features: " + str(self.feature_list))
            # print("Columns: " + self.data.columns)
            tobj = TekelObject(row, self.feature_list)

            scanitem = None
            if unique_id_feature:
                query_str = "SELECT * FROM %s WHERE %s='%s'" % (table_name, unique_id_feature, tobj.get_value_s(unique_id_feature))
                self.cursor.execute(query_str)
                scanitem = self.cursor.fetchone() #retrieve the first row

            if not scanitem:
                sql = "INSERT INTO %s(%s)" % (table_name, self.comma_features(fields_to_save))
                sql = sql + "VALUES(%s)" % (tobj.comma_values(fields_to_save))
                self.print_dbg(sql)
                cursor.execute(sql)
            else:
                self.print_dbg("Item already in DB .Updating!")
                print(self.feature_list)
                # update
                sql = "UPDATE %s SET %s WHERE %s='%s'" % (table_name, tobj.comma_features_values(fields_to_save), unique_id_feature, tobj.get_value_s(unique_id_feature))
                print(sql)
                cursor.execute(sql)

            if index % 10 == 0:
                self.commit_db() # commit every 10th

        self.commit_db()

    def match_with(self, another_list, equivalent_features, progress_callback, ignore_words = None):
        """ Takes another list and looks  through to match all items that
            are the same. 

        Args:
            another_list: A second tekel list to match against
            equivalent_features: A list of dictionaries, mapping features that are equivalent
                                 between the two lists. E.g tells it that the addresses are supposed
                                 to be the same between them
            progress_callback: a function you want called for progress. This call can take long so this
                                could be used to update the user
        Returns:
           A list of dictionaries with the indeces from one list that match the other - e.g ["12" : "d4f5", "26" : "t5f2"]
        """

        result_list = []

        # Loop through all items in the list
        for index, row in self.data.iterrows():

            # Convert into easier to use TekelObject
            tobj1 = TekelObject(row, self.feature_list, self.states)

            best_match = None
            best_score = 0
            for index, otherRow in another_list.data.iterrows():
                tobj2 = TekelObject(otherRow, another_list.feature_list, self.states)

                # At this point, we want to record similarity
                score = tobj1.calculate_similarity(tobj2, equivalent_features, ignore_words)
                
                if score > best_score:
                    best_score = score
                    best_match = tobj2
            
            # self.print_dbg(str(best_score))
            if best_score > 60:
                result_list.append({tobj1.unique_id() : best_match.unique_id()})
                progress_callback(1, tobj1, best_match, index, self.total() * another_list.total())
            else:
                progress_callback(0, tobj1, best_match, index, self.total() * another_list.total())

        return result_list

    def feature_names(self):
        n = []
        for f in self.feature_list:
            n.append(f.feature_name)
        return n

    def tabulate(self):
        t = PrettyTable(self.feature_names())

        for item in self:
            t.add_row(item.as_list(20))
        
        return t

    def first(self):
        return TekelObject(self.data.iloc[ 0 , : ], self.feature_list)

    def __iter__(self):
       return self
    
    def __next__(self):
        self.idx += 1
        try:
            
            return TekelObject(self.data.iloc[ self.idx-1 , : ], self.feature_list) # [self.idx-1]
        except IndexError:
            self.idx = 0
            raise StopIteration  # Done iterating.