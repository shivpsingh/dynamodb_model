import sys
from boto3.dynamodb.conditions import Key
from dynamodb_json import json_util as db_json
from .ddb import DDBApi, DB_SettingsHelper


class Model(type):

    class Key:
        """
            Class to obtain key type for dynamodb
        """

        class PartitionKey:
            pass

        class SortKey:
            pass

        PARTITION_KEY = PartitionKey()
        SORT_KEY = SortKey()

    class Column:

        _partition_key = None
        _sort_key = None

        def __init__(
            self,
            default_type=None,
            default_value=None,
            key_type=None
        ):
            if default_type:
                if default_value and type(default_value) != default_type:
                    raise TypeError('Default Type and Value Mismatch')

            self._type = default_type
            self._value = default_value
            self._default_type = default_type
            self._default_val = default_value
            self._is_partition_key = False
            self._is_sort_key = False

            if Model.Column._partition_key and isinstance(
                key_type, Model.Key.PartitionKey
            ):
                raise TypeError('PARTITION KEY Can only be One')
            if Model.Column._sort_key and isinstance(
                key_type, Model.Key.SortKey
            ):
                raise TypeError('SORT KEY Can only be One')

            if isinstance(key_type, Model.Key.PartitionKey):
                Model.Column._partition_key = Model.Key.PARTITION_KEY
                self._is_partition_key = True
            elif isinstance(key_type, Model.Key.SortKey):
                Model.Column._sort_key = Model.Key.SORT_KEY
                self._is_sort_key = True

        def get_value(self):
            return (
                self._type(self._value) if (
                    self._type and self._value
                ) else self._value
            )

        def get_default_val(self):
            return (
                self._default_type(self._default_val) if (
                    self._default_type and self._default_val
                ) else self._default_val
            )

        def set_value(self, value):
            self._value = value

        def is_partition_key(self):
            return self._is_partition_key

        def is_sort_key(self):
            return self._is_sort_key

    @classmethod
    def __prepare__(meta, name, bases, **kwds):
        meta.Column._partition_key = None
        meta.Column._sort_key = None
        return dict()

    def __new__(model_attr, class_name, parent_class, attributes):

        # Fill DB Column
        db_columns = dict()

        # Keys and Column List
        PARTITION_KEY = None
        SORT_KEY = None
        COLUMNS = list()
        DEFAULT_VAL_COLS = dict()

        # Filling Attributes
        for key, value in attributes.items():
            if isinstance(value, Model.Column):
                if value.is_partition_key():
                    PARTITION_KEY = key
                elif value.is_sort_key():
                    SORT_KEY = key
                else:
                    COLUMNS.append(key)
                    DEFAULT_VAL_COLS[key] = value.get_default_val()

                db_columns[key] = value.get_value()

        attributes.update(**db_columns)

        # Custom Methods for Class

        # Returns a dictionary of all the db_columns defined in the model
        attributes['to_dict'] = lambda self: {
            key: self.__getattribute__(key) for key in db_columns.keys()
        }

        # Returns a dictionary of required db_columns defined in the model
        attributes['cust_dict'] = lambda self, list_of_cols: {
            key: self.__getattribute__(key) for key in list_of_cols
        }

        # Populates all the fields with the dictionary provided
        # If key exists in DB Column will be populated else ignored
        attributes['populate'] = lambda self, **kwargs: ([
            self.__setattr__(
                key,
                val
            ) if key in db_columns else None for key, val in kwargs.items()
        ])

        # Populates the fields other than keys with the dictionary provided
        # If key exists in DB Column will be populated else ignored
        attributes['populate_cols'] = lambda self, **kwargs: ([
            self.__setattr__(
                key,
                val
            ) if key in COLUMNS else None for key, val in kwargs.items()
        ])

        def set_cols_none(self, list_of_cols_to_ignore=[]):
            """
            Set the columns value to none ignore the fields passed
            in list_of_cols_to_ignore
            """
            for key in COLUMNS:
                if (
                    list_of_cols_to_ignore
                ) and key not in list_of_cols_to_ignore:
                    self.__setattr__(key, None)

        attributes['set_cols_none'] = set_cols_none

        def set_cols_to_default(self, list_of_cols_to_ignore=[]):
            """
            Set the columns value to their default value ignore the
            fields passed in list_of_cols_to_ignore
            """
            for key, val in DEFAULT_VAL_COLS.items():
                if (
                    list_of_cols_to_ignore
                ) and key not in list_of_cols_to_ignore:
                    self.__setattr__(key, val)

        attributes['set_cols_to_default'] = set_cols_to_default

        # Returns the string representation of the DB Column and Values
        attributes['__str__'] = lambda self: str({
            key: self.__getattribute__(key) for key in db_columns.keys()
        })

        attributes['reset_cols'] = lambda self, **kwargs: ([
            self.__setattr__(
                key,
                None
            ) if key in COLUMNS else None for key, val in kwargs.items()
        ])

        # Checks if SETTINGS_CLASS is defined in class or not
        if 'SETTINGS_CLASS' not in attributes:
            try:
                DB_Sttings = type('DB_Sttings', (DB_SettingsHelper,), {
                    'DDB_MAX_RETRIES': attributes['DDB_MAX_RETRIES'],
                    'DDB_RETRY_SLEEP_TIME':
                    attributes['DDB_RETRY_SLEEP_TIME'],
                    'DDB_ENDPOINT_URL': attributes['DDB_ENDPOINT_URL'],
                    'DbTableName': attributes['DbTableName'],
                    'AWSRegion': attributes['AWSRegion'],
                })
                settings_class = DB_Sttings()
            except KeyError as err:
                print('ERROR: Class Must define the key {}'.format(str(err)))
                raise
        else:
            settings_class = attributes['SETTINGS_CLASS']

        # Initialize DB Adapter
        db_adapter = DDBApi(settings_class)

        # Save Method to save the current Instance into DB
        attributes['save'] = lambda self, list_of_cols=None: (
            db_adapter.add_row(
                self.cust_dict(list_of_cols)
            ) if list_of_cols else db_adapter.add_row(self.to_dict())
        )

        # Fetch a Single Row based on the attributes provided
        def fetch_row(self, conditional_items=None,
                      key_condition_expression=None,
                      filter_expression=None, attributes_to_fetch=[],
                      sort_key=True):
            if not key_condition_expression:
                if sort_key:
                    key_condition_expression = Key(
                        PARTITION_KEY
                    ).eq(
                        self.__getattribute__(PARTITION_KEY)
                    ) & Key(
                        SORT_KEY
                    ).eq(
                        self.__getattribute__(SORT_KEY)
                    )
                else:
                    key_condition_expression = Key(
                        PARTITION_KEY
                    ).eq(
                        self.__getattribute__(PARTITION_KEY)
                    )
            if not conditional_items:
                conditional_items = [
                    PARTITION_KEY, SORT_KEY
                ] if sort_key else [
                    PARTITION_KEY
                ]

            result = db_adapter.fetch_row(
                attributes_to_fetch=attributes_to_fetch,
                conditional_items=conditional_items,
                key_condition_expression=key_condition_expression,
                filter_expression=filter_expression,
                sort_key=sort_key
            )

            # Return the fetched row
            return db_json.loads(result)

        # Mapping function to the class
        attributes['fetch_row'] = fetch_row

        def fetch_all_rows(self, **kwargs):
            response = db_adapter.fetch_all_rows(**kwargs)
            return db_json.loads(response)

        # Scan all the row from DB
        attributes['fetch_all_rows'] = fetch_all_rows

        # Delete Previous entries and update with new values
        def fetch_and_populate_cols(
            self,
            conditional_items=None,
            key_condition_expression=None,
            filter_expression=None,
            attributes_to_fetch=[],
            sort_key=True
        ):

            self.set_cols_none()
            result = self.fetch_row(
                conditional_items=conditional_items,
                key_condition_expression=key_condition_expression,
                filter_expression=filter_expression,
                attributes_to_fetch=attributes_to_fetch,
                sort_key=sort_key
            )
            if result:
                self.populate_cols(**result)
            return db_json.loads(result)

        # Fetch the row and populate its cols -- leave keys
        attributes['fetch_and_populate_cols'] = fetch_and_populate_cols

        # Function to delete a row based on the object
        def delete_row(self, key=None, sort_key=True):
            if not key:
                if sort_key:
                    key = {
                        PARTITION_KEY:
                        self.__getattribute__(PARTITION_KEY),
                        SORT_KEY:
                        self.__getattribute__(SORT_KEY),
                    }
                else:
                    key = {
                        PARTITION_KEY:
                        self.__getattribute__(PARTITION_KEY)
                    }
            # Returns the Delete Response
            response = db_adapter.delete_row(item=key)
            return db_json.loads(response)

        # Mapping function to the class
        attributes['delete_row'] = delete_row

        # Function to fetch all rows based on list of dict keys provided
        def fetch_rows_on_keys(self, list_of_dict_keys=[],
                               return_consumed_capacity='NONE'):
            request_items = {
                settings_class.DbTableName: {
                    'Keys': list_of_dict_keys
                }
            }
            # Returns the fetch Response
            response = db_adapter.batch_get_item(
                request_items=request_items,
                return_consumed_capacity=return_consumed_capacity
            )
            return db_json.loads(response)

        # Mapping function to the class
        attributes['fetch_rows_on_keys'] = fetch_rows_on_keys

        # Function to get all the values based on partition key
        def query_on_partition_key(self, value, limit=None, select=None):
            response = db_adapter.query_items(
                {
                    'KeyConditionExpression': Key(PARTITION_KEY).eq(value),
                    'Limit': limit or sys.maxsize,
                    'Select': select or 'ALL_ATTRIBUTES'
                }
            )
            return db_json.loads(response)

        # Mapping function to the class
        attributes['query_on_partition_key'] = query_on_partition_key

        # Function to query table with a complete query obj
        def query_table(self, query_obj):
            response = db_adapter.query_items(query_obj)
            return db_json.loads(response)

        # Mapping function to the class
        attributes['query_table'] = query_table

        def update_row(self, update_values=None, delete_none=True):
            # Returns the Delete Response
            if delete_none:
                obj = db_adapter.del_empty_key_values(self.to_dict())
            else:
                obj = self.to_dict()
            response = db_adapter.update_row(input_values=obj)
            return db_json.loads(response)

        # Mapping function to the class
        attributes['update_row'] = update_row

        return super().__new__(
            model_attr,
            class_name,
            parent_class,
            attributes
        )
