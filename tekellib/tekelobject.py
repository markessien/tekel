import textwrap

from tekelfeature import TekelType, TekelFeature
from prettytable import PrettyTable

try:
    from fuzzywuzzy import fuzz, process
    from slugify import slugify
except:
    pass
    
try:
    import pandas as pd
except:
    print("Pandas not available")

class TekelObject(object):
    """ This class represents a single Tekel Object. The
        data is stored in a pandas object. This class wraps
        the pandas object so there is easy access to the
        data in it

    Attributes:
        item: The pandas object. Avoid accessing directly
        features: All the features
        object_id: A unique ID for this object
    """

    def __init__(self, item, features):
        self.item = item  # This item is a pandas row
        self.features = features
        self.object_id = None
        self.primary_feature_index = 0

    def set_val(self, feature_name, value):
        if (self.item is None):
            self.item = pd.DataFrame([[value]], columns = [feature_name]) 

        self.item[feature_name] = value

        # print("Feat: " + str(self.features))
        if self.features == None:
            self.features = []

        for feature in self.features:
            if feature.feature_name == feature_name:
                return

        f = TekelFeature(feature_name, TekelType.Unknown)
        self.features.append(f)

        """
        if feature_name in self.item.columns:
            

        else:
            df1 = df1.assign(e=p.Series(np.random.randn(sLength)).values)
        """

    def get_dataframe(self):
        return self.item

    def __getitem__(self, key):
        return self.get_value_s(key)

    def __iter__(self):
       return self

    def __str__(self):
        return str(self.get_value(self.features))


    def get_file_name(self, from_column):
        # print(str(self.features))
        # print(self.get_value_s("name"))

        return slugify(self.get_value_s(from_column)).lower()

    def get_value_s(self, feature_name):
        """
        Returns the value for the feature passed in
        TODO: Optimise this to avoid this loop. A lookup table
              will make it run way better
        """ 
        return str(self.item[feature_name])

        print(self.features)
        i = 0
        for i, f in enumerate(self.features):
            # print(str(f.column_name) + " : " + str(feature_name))
            if f.table_column_name == feature_name:
                break

        # print
        return self.item.iat(i)

    def get_value(self, feature):
        """
        Returns the value for the feature passed in
        TODO: Optimise this to avoid this loop. A lookup table
              will make it run way better
        """
        for i, f in enumerate(self.features):
            if f == feature:
                break

        return self.item[i]

    def as_list(self, max_width=-1):
        thelist = []
        
        for i in range(len(self.item)):
            if max_width > 0:
                thelist.append(textwrap.shorten(str(self.item[i]), max_width))
            else:
                thelist.append(self.item[i])

        return thelist

    def some_fields(self, field_list):
        res = ""
        for i, f in enumerate(field_list):
            if i:
                res = res + ", " + self.item[f] 
            else:
                res = self.item[f] 

        return res

    def feature_names(self):
        n = []
        for f in self.features:
            n.append(f.feature_name)
        return n

    def tabulate(self):
        t = PrettyTable(self.feature_names())
        t.add_row(self.as_list())
        return t

    def comma_features_values(self,  fields = None):

        
        comma = ""
        # print("Columns:" + self.item["hotel_name"])
        # print("Features:" + str(self.features))
        # print(str(self.item))
        for i in range(len(self.item)):
            
            if not self.features[i].table_column_name in fields:
                continue

            if i: comma = comma + ","
            
            feature = self.features[i].table_column_name
            value = str(self.item[feature]).replace('"', '""')
            value = value.replace("'", "")
            
            comma = comma + feature + "='" + value + "'"

        return comma       

    def comma_values(self, fields = None):
        comma = ""
        # print("Features:" + str(self.features))
        for i in range(len(self.item)):
            
            if not self.features[i].table_column_name in fields:
                continue

            if i: comma = comma + ","
            
            if self.features[i].feature_type == TekelType.UniqueDigitID:
                comma = comma + '"' + str(int(self.item[i])) + '"'
            else:
                s = str(self.item[i]).replace('"', '""')
                comma = comma + '"' + s + '"'
        return comma

    def calculate_similarity(self, other_object, equivalent_features, ignore_words):
        """ Gives a numerical value that indicates how similar this object is
             to the other object passed in 

        Args:
            other_object:
            equivalent_features: A list of dictionaries, mapping features that are equivalent
                                 between the two lists. Items on left must be from this object
                                 while items on right must be from the other object
        Returns:
           A number indivating similarity
        """

        # Retrieve the two pandas objects
        # pdframe1 = self.item
        # pdframe2 = other_object.item


        # We will calculate the similarity of each feature separately, depending
        # on the type. 100 = closest similarity and 0 most dissimilar
        similarity_per_feature = []

        # The features are grouped same as the columns in the dataframe.
        # So we can use the features to know how to analyse each object
        i = 0
        is_similar = False
        number_of_relevant_features = 0 # used to average the scores
        total_score = 0
        compare = TekelComparison()
        for feature_pair in equivalent_features:
            feature_left, feature_right = feature_pair

            value_left = self.get_value(feature_left)
            value_right = other_object.get_value(feature_right)
                
            # print(str(value_left) + " : " + str(value_right))
            # print("Feature: " + str(feature_left) + " : " + str(value_left) + " Feature_Right: " + str(feature_right) + " V: " + str(value_right))
            
            similarity_per_feature.append(-1)
            
            if not value_left or not value_right:
                continue
            
            if (feature_left.feature_type == TekelType.State and feature_right.feature_type == TekelType.State):
                similarity_per_feature[i] = compare.compare_states(value_left, value_right, self.states)
                # print("Match between " + value_left + " and " + value_right + " is " + str(similarity_per_feature[i]))

                # if the resullt is -1, it means we cannot find the result. We hence choose not to
                # use the state as a comparison factor, so we don't make any mistake
                if similarity_per_feature[i] == 0: 
                    total_score = total_score + similarity_per_feature[i]
                    number_of_relevant_features = number_of_relevant_features + 1

            if (feature_left.feature_type == TekelType.String and feature_right.feature_type == TekelType.String):
                # For strings, use levenshtein distance via fuzzywuzzy lib
                
                # Strip the ignored words, e.g 'hotel' or so on
                value_right_final = value_right.lower()
                value_left_final = value_left.lower()
                for ignore_word in ignore_words:
                    value_right_final = value_right_final.replace(ignore_word, '')
                    value_left_final = value_left_final.replace(ignore_word, '')

                similarity_per_feature[i] = fuzz.ratio(value_left_final, value_right_final)
                # print(str(similarity_per_feature[i]))
                if (similarity_per_feature[i] > 70):
                    is_similar = True
                total_score = total_score + similarity_per_feature[i]
                number_of_relevant_features = number_of_relevant_features + 1
            
            if feature_left.feature_type == TekelType.Address and feature_right.feature_type == TekelType.Address:
                # For now, just use levenshtein
                try:
                    similarity_per_feature[i] = fuzz.ratio(value_left, value_right)
                except:
                    pass
                if (similarity_per_feature[i] < 40): 
                    is_similar = False

                total_score = total_score + similarity_per_feature[i]
                number_of_relevant_features = number_of_relevant_features + 1

            i = i + 1

        # print(str(similarity_per_feature) + str(total_score) + ":" + str(number_of_relevant_features))
        if number_of_relevant_features > 0:
            total_score = total_score / number_of_relevant_features
            # if total_score > 60:
            #    print(str(similarity_per_feature) + str(total_score) + ":" + str(number_of_relevant_features) + ":" + str(other_object.get_value(other_object.primary_feature)))
            return total_score
        else:
            return 0

