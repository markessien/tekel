from enum import Enum

class TekelType(Enum):
    Digit = 1
    Date = 2
    UniqueString = 3
    Currency = 4
    String = 5
    Unknown = 6
    UniqueID = 7
    Longitude = 8
    Latitude = 9
    Address = 10
    Float = 11
    UniqueDigitID = 12
    State = 13
    City = 14
    Phone = 15
    Type = 16

class TekelFeature():
    """ This class represents a feature or attribute of a Tekel Object

    Attributes:
        featuretype:
        featurename: 
        is_primary: 
        table_column:
    """

    def __init__(self, feature_name, feature_type, is_primary = False, table_column_name = ""):
        self.feature_type = feature_type
        self.feature_name = feature_name
        self.is_primary = is_primary
        self.table_column_name = table_column_name

        if self.table_column_name == None or self.table_column_name == "":
            self.table_column_name = self.feature_name

    def change_name(self, new_feature_name):
        if self.table_column_name == self.feature_name:
            self.table_column_name = new_feature_name
        self.feature_name = new_feature_name

    def __repr__(self):
       return self.__str__()

    def __str__(self):
        return str(self.feature_name)

    def generate_description(self, obj):
        """ Generates a description for this particular feature

        Returns:
           The unique Id of the dataset
        """
        pass

    def sql_text(self, prename_str = ""):
        self.table_column_name = ''.join(prename_str.split()) + ''.join(self.feature_name.split())

        if self.feature_type == TekelType.String:
            return self.table_column_name + " TEXT NOT NULL"
        
        if self.feature_type == TekelType.Longitude or self.feature_type == TekelType.Latitude:
            return self.table_column_name + " REAL"

        return self.table_column_name + " TEXT NOT NULL"