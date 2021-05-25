'''
Created on 12. 11. 2018

@author: esner
'''
import unittest
import mock
import os
from freezegun import freeze_time

from component import Component
from salesforce.soql_query import SoqlQuery


class TestComponent(unittest.TestCase):

    # set global time to 2010-10-10 - affects functions like datetime.now()
    @freeze_time("2010-10-10")
    # set KBC_DATADIR env to non-existing dir
    @mock.patch.dict(os.environ, {'KBC_DATADIR': './non-existing-dir'})
    def test_run_no_cfg_fails(self):
        with self.assertRaises(ValueError):
            comp = Component()
            comp.run()

    def test_standard_query_parsing(self):
        query = "SELECT Id,  IsDeleted, MasterRecordId,  User_PB_ID__c,  FirstName,  LastName,  Name,  Title,  Company,  Street,  City,  State, Seniority__c FROM Lead"
        parsed_fields = SoqlQuery._get_fields_from_query(query, "Lead")
        expected_fields = ['Id', 'IsDeleted', 'MasterRecordId', 'User_PB_ID__c', 'FirstName', 'LastName', 'Name',
                           'Title', 'Company', 'Street', 'City', 'State', 'Seniority__c']
        self.assertEqual(parsed_fields, expected_fields)

    def test_no_space_query_parsing(self):
        query = "SELECT Id,Name,ProductCode,Description,IsActive,CreatedDate,CreatedById,LastModifiedDate,LastModifiedById,SystemModstamp,Family,ExternalDataSourceId,ExternalId,DisplayUrl,QuantityUnitOfMeasure FROM Product2"
        parsed_fields = SoqlQuery._get_fields_from_query(query, "Product2")
        expected_fields = ['Id', 'Name', 'ProductCode', 'Description', 'IsActive', 'CreatedDate', 'CreatedById',
                           'LastModifiedDate', 'LastModifiedById', 'SystemModstamp', 'Family', 'ExternalDataSourceId',
                           'ExternalId', 'DisplayUrl', 'QuantityUnitOfMeasure']
        self.assertEqual(parsed_fields, expected_fields)

    def test_period_query_parsing(self):
        query = "select user.id, user.Email, user.Alias, user.FirstName, user.LastName, user.profile.name, user.Username, user.IsActive, user.Team__c, UserRole.Name, Territory__c FROM user, user.profile"
        parsed_fields = SoqlQuery._get_fields_from_query(query, "user")
        expected_fields = ['id', 'Email', 'Alias', 'FirstName', 'LastName', 'name', 'Username', 'IsActive', 'Team__c',
                           'UserRole_Name', 'Territory__c']
        self.assertEqual(parsed_fields, expected_fields)


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
