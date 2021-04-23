"""
    DynamoDB Helper Classes
"""

# Imports
import abc
import time

# AWS Imports
import boto3
import botocore
from boto3.dynamodb.conditions import Key


class DB_SettingsHelper(abc.ABC):
    """
        Abstract Class for specifying the required parameters
        to initialize the DDBHelper Class.
    """

    @property
    def DbTableName(self):
        pass

    @DbTableName.setter
    @abc.abstractmethod
    def DbTableName(self, DbTableName):
        self.DbTableName = DbTableName

    @property
    def DDB_MAX_RETRIES(self):
        pass

    @DDB_MAX_RETRIES.setter
    @abc.abstractmethod
    def DDB_MAX_RETRIES(self, DDB_MAX_RETRIES):
        self.DDB_MAX_RETRIES = DDB_MAX_RETRIES

    @property
    def DDB_RETRY_SLEEP_TIME(self):
        pass

    @DDB_RETRY_SLEEP_TIME.setter
    @abc.abstractmethod
    def DDB_RETRY_SLEEP_TIME(self, DDB_RETRY_SLEEP_TIME):
        self.DDB_RETRY_SLEEP_TIME = DDB_RETRY_SLEEP_TIME

    @property
    def AWSRegion(self):
        pass

    @AWSRegion.setter
    @abc.abstractmethod
    def AWSRegion(self, AWSRegion):
        self.AWSRegion = AWSRegion


class DDBError(Exception):
    """
        Exception Class for DynamoDB Errors
    """
    pass


class DDBApi:
    """
        Generic APIs to interface with dynamodb
    """

    def __init__(self, settings: DB_SettingsHelper):
        """Initialize DDBApi variables

        Args:
            settings (DB_SettingsHelper):
                Settings Helper Class with required params
        """

        self._settings = settings
        self._resource = boto3.resource(
            service_name="dynamodb",
            region_name=self._settings.AWSRegion
        )

        self._client = boto3.client(
            service_name="dynamodb",
            region_name=self._settings.AWSRegion
        )
        self._table = self._resource.Table(
            self._settings.DbTableName
        )

    @classmethod
    def del_empty_key_values(cls, obj):
        for key, value in list(obj.items()):
            if value is None or value == '':
                obj.pop(key)
            elif isinstance(value, dict):
                cls.del_empty_key_values(value)

        return obj

    def _construct_update_expression(self, update_attributes):

        counter = 1
        var = "#var{0}= :{1},"
        var_key = "#var{0}"
        update_expression = "SET "
        attribute_value_key = ":{0}"

        expression_attribute_names = {}
        expression_attribute_values = {}

        for attribute in update_attributes:
            expression_attribute_names[var_key.format(counter)] = attribute
            expression_attribute_values[
                attribute_value_key.format(attribute)] = update_attributes[
                attribute]
            update_expression += str(var.format(counter, attribute))
            counter += 1

        return update_expression.rstrip(
            ','), expression_attribute_names, expression_attribute_values

    def _get_partition_and_sort_key(self, items):
        keys = dict()
        # If partition key is not present in the input, then throw error
        if self.partition_key not in items:
            raise DDBError('Partition key not present in the request.')

        if self.sort_key is not None and self.sort_key not in items:

            # If there is no local index also, then return error
            if self.local_index_name is None:
                raise DDBError('Keys not present in the request.')
            else:
                # Fetching data using local index
                if self.local_index_partition_key in items \
                        and self.local_index_sort_key in items:
                    local_index_attributes = dict()
                    local_index_attributes[
                        'partition_key'] = self.local_index_partition_key
                    local_index_attributes[
                        'sort_key'] = self.local_index_sort_key

                    local_index_values = dict()
                    local_index_values['partition_key'] = items[
                        self.local_index_partition_key]
                    local_index_values['sort_key'] = items[
                        self.local_index_sort_key]

                    api_data = self.get_item_by_secondary_index(
                        self.local_index_name,
                        local_index_attributes,
                        local_index_values
                    )
                    keys[self.partition_key] = api_data[self.partition_key]
                    keys[self.sort_key] = api_data[self.sort_key]
                    return keys

                else:
                    raise DDBError(
                        'Keys not present in the request. '
                        'Local secondary index keys also missing'
                    )

        else:
            query_key = dict()
            # If partition key is present in the input
            query_key[self.partition_key] = items[self.partition_key]

            # if sort key is not necessary, then fetch data
            # using partition key only
            if self.sort_key is not None:
                query_key[self.sort_key] = items[self.sort_key]

            return query_key

    def _put_item(self, key, update_expression, update_expression_values,
                  update_attribute_name):
        return self._table.update_item(
            Key=key,
            UpdateExpression=update_expression,
            ExpressionAttributeValues=update_expression_values,
            ExpressionAttributeNames=update_attribute_name
        )

    def fetch_row(self, attributes_to_fetch, conditional_items,
                  key_condition_expression, filter_expression, sort_key=True):

        # Fetch All the records if no partition key is found.
        if self.partition_key not in conditional_items:
            return self.fetch_all_rows()

        else:
            query_db_response = self.query_db(
                filter_expression, key_condition_expression
            )

            if not sort_key:
                return query_db_response['Items']

            if len(query_db_response['Items']) == 0:
                return None

            response_item = query_db_response['Items'][0]

            if len(attributes_to_fetch) == 0:
                return response_item
            else:
                keys_to_keep = set.intersection(
                    set(attributes_to_fetch),
                    response_item.keys()
                )
                return {k: response_item[k] for k in keys_to_keep}

    def update_row(self, input_values):

        items = dict(input_values)
        update_keys = self._get_partition_and_sort_key(items)

        for item in list(items):
            if item in update_keys:
                del items[item]

        update_expression, update_attribute_name, update_expression_values \
            = self._construct_update_expression(items)

        return self._put_item(
            update_keys, update_expression,
            update_expression_values,
            update_attribute_name
        )

    def get_item(self, key):
        if key is None:
            raise DDBError(
                "No attribute provided for get_item operation"
            )

        retries = 0
        max_retries = self._settings.DDB_MAX_RETRIES
        while retries <= max_retries:
            try:
                response = self._table.get_item(Key=key).get("Item", None)
                break
            except KeyError as e:
                raise DDBError(
                    "The item with id '{}' does not exist".format(key)
                )
            except botocore.exceptions.ClientError as e:
                code = e.response["Error"]["Code"]
                if code == "ProvisionedThroughputExceededException" \
                        or code == "ThrottlingException":
                    if retries == max_retries:
                        raise DDBError(
                            'Get operation failed. \
                                Error Code: {}. Error: {}'.format(
                                e.response["Error"]["Code"],
                                e.response["Error"]["Message"]
                            )
                        )

                    time.sleep(
                        self._settings.DDB_RETRY_SLEEP_TIME ** retries
                    )
                    retries += 1
                else:
                    raise DDBError(
                        'Get operation failed. \
                            Error Code: {0}.Error: {1}'.format(
                            e.response["Error"]["Code"],
                            e.response["Error"]["Message"]
                        )
                    )
        return response

    def delete_row(self, key):
        try:

            response = self._table.delete_item(Key=key)

            if response["ResponseMetadata"]["HTTPStatusCode"] != 200:
                raise DDBError(
                    "Error trying to delete item. {}").format(str(response)
                                                              )

        except botocore.exceptions.ClientError as e:
            raise DDBError(
                'Delete operation failed. Error Code: {0} Error: {1}'.format(
                    e.response["Error"]["Code"],
                    e.response["Error"]["Message"])
            )
        except KeyError as e:
            raise DDBError(
                '''Delete operation failed. The required attribute {0}
                is not provided'''.format(e.args[0])
            )

        return response

    def add_row(self, item):
        self.del_empty_key_values(item)

        retries = 0
        max_retries = self._settings.DDB_MAX_RETRIES
        while retries <= max_retries:
            try:
                response = self._table.put_item(Item=item)
                if response["ResponseMetadata"]["HTTPStatusCode"] != 200:
                    raise DDBError(
                        "Error trying to insert item. {}").format(
                        str(response)
                    )
                break
            except botocore.exceptions.ClientError as e:
                code = e.response["Error"]["Code"]
                if code == "ProvisionedThroughputExceededException" \
                        or code == "ThrottlingException":
                    if retries == max_retries:
                        raise DDBError('Add operation failed. \
                                        Error Code: {}. Error: {}'.format(
                            e.response["Error"]["Code"],
                            e.response["Error"]["Message"]
                        ))

                    time.sleep(
                        self._settings.DDB_RETRY_SLEEP_TIME ** retries
                    )
                    retries += 1
                else:
                    raise DDBError('Add operation failed. \
                                    Error Code: {}. Error: {}'.format(
                        e.response["Error"]["Code"],
                        e.response["Error"]["Message"]
                    ))

            return response

    def get_item_by_secondary_index(self,
                                    index_name,
                                    key_attributes,
                                    values):

        if key_attributes is None:
            raise DDBError(
                "No attribute provided for get_item_by_secondary_index "
                "operation"
            )

        retries = 0
        max_retries = self._settings.DDB_MAX_RETRIES
        while retries <= max_retries:
            try:
                # response = self._table.get_item(Key=key).get("Item", None)
                response = self._table.query(
                    IndexName=index_name,
                    KeyConditionExpression=Key(
                        key_attributes['partition_key']).eq(
                        values['partition_key']) & Key(
                        key_attributes['sort_key']).eq(values['sort_key'])
                )
                break
            except KeyError as e:
                raise DDBError(
                    "The item with id '{}' does not exist".format(
                        values['partition_key'])
                )
            except botocore.exceptions.ClientError as e:
                code = e.response["Error"]["Code"]
                if code == "ProvisionedThroughputExceededException" \
                        or code == "ThrottlingException":
                    if retries == max_retries:
                        raise DDBError(
                            'Get operation failed. \
                                Error Code: {}. Error: {}'.format(
                                e.response["Error"]["Code"],
                                e.response["Error"]["Message"]
                            )
                        )

                    time.sleep(
                        self._settings.DDB_RETRY_SLEEP_TIME ** retries
                    )
                    retries += 1
                else:
                    raise DDBError(
                        'Get operation failed. \
                            Error Code: {}. Error: {}'.format(
                            e.response["Error"]["Code"],
                            e.response["Error"]["Message"]
                        )
                    )
        return response

    def get_table_structure(self, table_name):
        return self._client.describe_table(TableName=table_name)

    def fetch_all_rows(self, **kwargs):
        if kwargs.get('LastEvaluatedKey', None) is None:
            return self._table.scan()
        else:
            return self._table.scan(
                ExclusiveStartKey=kwargs.get('LastEvaluatedKey')
            )

    def query_db(self, filter_expression, key_condition_expression):
        if filter_expression is not None:
            return self._table.query(
                KeyConditionExpression=key_condition_expression,
                FilterExpression=filter_expression
            )
        else:
            return self._table.query(
                KeyConditionExpression=key_condition_expression
            )

    def batch_get_item(self, request_items, return_consumed_capacity='NONE'):

        resultset = []
        response = dict()
        while True:
            response = self._resource.batch_get_item(
                RequestItems=(
                    response.get('UnprocessedKeys', None) or request_items
                ),
                ReturnConsumedCapacity=return_consumed_capacity
            )
            resultset.append(response.get('Responses', {}))
            if response.get('UnprocessedKeys') == {}:
                break
            else:
                response = response.get('UnprocessedKeys', {})

        return resultset

    def query_items(self, query_obj):
        limit = query_obj.get('Limit')

        def query(query_obj):
            count = 0
            items = []
            while True:
                result = self._table.query(**query_obj)
                items.extend(result.get('Items', {}))
                count = count + result['Count']

                lastkey = result.get('LastEvaluatedKey')
                if not lastkey or (limit and len(items) >= limit):
                    break
                query_obj.update({'ExclusiveStartKey': lastkey})
            return items, count

        items, count = query(query_obj)

        if query_obj.get('Select') == 'COUNT':
            return count
        return items
