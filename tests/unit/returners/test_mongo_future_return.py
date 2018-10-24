# -*- coding: utf-8 -*-
'''
    :codeauthor: Mike Place (mp@saltstack.com)


    tests.unit.returners.smtp_return_test
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
'''

# Import Python libs
from __future__ import absolute_import

# Import Salt Testing libs
from tests.support.mixins import LoaderModuleMockMixin
from tests.support.unit import TestCase, skipIf
from tests.support.mock import NO_MOCK, NO_MOCK_REASON, MagicMock, patch

# Import salt libs
import salt.returners.mongo_future_return as mongo

# Import third party libs
try:
    import pymongo
    version = pymongo.version
    version = '.'.join(version.split('.')[:2])
    HAS_PYMONGO = True
except ImportError:
    HAS_PYMONGO = False


@skipIf(NO_MOCK or not HAS_PYMONGO, 'pymongo is not available')
class MongoReturnerConnectionReuseTestCase(TestCase, LoaderModuleMockMixin):
    '''
    Test SMTP returner
    '''
    def setup_loader_modules(self):
        return {
            mongo: {
                '__opts__': {
                    'host': ['1.1.1.1'],
                    'port': 27017,
                    'db': 'salt',
                    'indexes': 'true',
                    'reuse_connection': True
                }
            }
        }

    def test_pymongo_connection_reusing(self):
        with patch('pymongo.database.Database.command') as pymongo_database_command \
                , patch('pymongo.collection.Collection.create_index') as pymongo_collection_create_index :
            pymongo_database_command.return_value = {}
            pymongo_collection_create_index.return_value = {}
            first_conn, db = mongo._get_conn(None)
            second_conn, db = mongo._get_conn(None)
            self.assertEqual(id(first_conn), id(second_conn))


@skipIf(NO_MOCK or not HAS_PYMONGO, 'pymongo is not available')
class MongoReturnerConnectionTestCase(TestCase, LoaderModuleMockMixin):
    '''
    Test SMTP returner
    '''
    def setup_loader_modules(self):
        return {
            mongo: {
                '__opts__': {
                    'host': ['1.1.1.1'],
                    'port': 27017,
                    'db': 'salt',
                    'indexes': 'false',
                    'reuse_connection': False
                }
            }
        }

    def test_pymongo_connection_not_reusing(self):
        with patch('pymongo.database.Database.command') as pymongo_database_command\
                , patch('pymongo.collection.Collection.create_index') as pymongo_collection_create_index :
            pymongo_collection_create_index.return_value = {}
            pymongo_database_command.return_value = {}
            first_conn, db = mongo._get_conn(None)
            second_conn, db = mongo._get_conn(None)
            self.assertNotEqual(id(first_conn), id(second_conn))

