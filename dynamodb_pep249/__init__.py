import logging
import pynamodb


logger = logging.getLogger(__name__)

apilevel = '0.0.1'


class Error(Exception):
    pass


class Warning(Exception):
    pass


class InterfaceError(Error):
    pass


class DatabaseError(Error):
    pass


class InternalError(DatabaseError):
    pass


class OperationalError(DatabaseError):
    pass


class ProgrammingError(DatabaseError):
    pass


class IntegrityError(DatabaseError):
    pass


class DataError(DatabaseError):
    pass


class NotSupportedError(DatabaseError):
    pass


def dynamodb_value_to_python_value(v):
    k, v = list(v.items())[0]
    if k == 'S':
        return v
    elif k == 'N':
        return int(v)
    else:
        return v


class Cursor(object):
    # TODO: get batch size maximum
    arraysize = 1

    def __init__(self, *args, **kwargs):
        self.executed = False
        self.closed = False
        self._rows = []
        super(Cursor, self).__init__(*args, **kwargs)

    @property
    def description(self):
        name = None
        type_code = None
        return (name, type_code, None, None, None, None, None)

    @property
    def rowcount(self):
        if not self.executed:
            return -1

        # FIXME:
        return 0

    def close(self):
        # TODO:
        self.closed = True

    def _get_items(self, operation, operation_type):
        cursor = self.connection.cursor()
        cursor.execute((operation_type, operation[1]), None)
        for row in cursor.fetchall():
            yield row

    def _do_delete_item(self, operation, params, operation_type):
        for row in self._get_items(operation, operation_type):
            cursor = self.connection.cursor()
            r = {
                'TableName': operation[1]['TableName'],
                'Key': {k: v for k, v in row.items()
                        if k in params['primary_key_fields']},
            }
            cursor.execute(('DeleteItem', r), None)
            logger.debug(row)

    def _do_update_item(self, operation, params, operation_type):
        AttributeUpdates = operation[1].pop('AttributeUpdates')
        for row in self._get_items(operation, operation_type):
            cursor = self.connection.cursor()
            r = {
                'TableName': operation[1]['TableName'],
                'Key': {k: v for k, v in row.items()
                        if k in params['primary_key_fields']},
                'AttributeUpdates': AttributeUpdates,
            }
            cursor.execute(('UpdateItem', r), None)
            logger.debug(row)

    def _deserialize_results(self, params, results):
        # Convert into rows of tuples
        if params['format']['type'] == 'tuple':
            self._rows = []
            if params['format'].get('fields', None):
                for item in results['Items']:
                    self._rows.append(tuple([
                        # FIXME: could convert data better
                        dynamodb_value_to_python_value(item.get(field))
                        for field in params['format']['fields']
                    ]))
            else:
                for item in results['Items']:
                    self._rows.append(tuple([
                        dynamodb_value_to_python_value(v) for k, v in item.items()
                    ]))

            self._iter = iter(self._rows)

        # Just raw output
        else:
            self._rows = results['Items']
            self._iter = iter(self._rows)

    def execute(self, operation, params=None):
        """
        tuplelize param converts data into a row of tuples
        """

        params = params or {}

        logger.debug('Operation: {} {}'.format(operation, params))

        if 'format' not in params:
            params['format'] = {'type': 'raw'}

        operation_name = operation[0]

        # DynamoDB doesn't support Delete queries/scans. Simulate it here
        if operation_name in ['DeleteItemQuery']:
            self._do_delete_item(operation, params, 'Query')
            return
        elif operation_name in ['DeleteItemScan']:
            self._do_delete_item(operation, params, 'Scan')
            return
        elif operation_name in ['UpdateItemQuery']:
            self._do_update_item(operation, params, 'Query')
            return
        elif operation_name in ['UpdateItemScan']:
            self._do_update_item(operation, params, 'Scan')
            return

        # TODO: handle BatchWriteItem with over 25 items

        try:
            r = self.connection.dyconn.dispatch(operation[0], operation[1])
        except pynamodb.exceptions.VerboseClientError as e:
            if e.response['Error']['Code'] == 'ResourceInUseException':
                raise IntegrityError('{Code}: {Message}'.format(**e.response['Error']))
            else:
                raise

        logger.debug('Result: {}'.format(r))

        if operation_name in ['Scan', 'Query']:
            self._deserialize_results(params, r)
        else:
            self._rows = [r]
            self._iter = iter(self._rows)

    def executemany(self, operation, seq_of_parameters):
        for params in seq_of_parameters:
            self.execute(operation, params)

    def fetchone(self):
        # TODO: An Error (or subclass) exception is raised if the previous
        #       call to .execute*() did not produce any result set or no call
        #       was issued yet.
        try:
            return next(self._iter)
        except StopIteration:
            return None

    def fetchmany(self, size=None):
        # TODO: DynamoDB has a max size for batches
        # TODO: An Error (or subclass) exception is raised if the previous
        #       call to .execute*() did not produce any result set or no call
        #       was issued yet.
        rows = []
        for i, row in enumerate(self._iter):
            rows.append(row)
            if size and size < i:
                break
        return rows

    def fetchall(self):
        return [row for row in self._iter]

    def setinputsizes(self, size):
        raise Exception

    def setoutputsizes(self, size, column=None):
        raise Exception

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


class Connection(object):
    def __init__(self, *args, **kwargs):
        self.cursors = []
        super(Connection, self).__init__(*args, **kwargs)

    def close(self):
        raise Exception

    def commit(self):
        raise Exception

    def rollback(self):
        raise Exception

    def cursor(self):
        cursor = Cursor()
        cursor.connection = self
        self.cursors.append(cursor)
        return cursor


def connect(params):
    conn = Connection()
    dynamodb_server = '{}:{}'.format(params['host'], params['port'])
    conn.dyconn = pynamodb.connection.Connection(host=dynamodb_server)
    return conn
