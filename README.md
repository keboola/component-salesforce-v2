#Salesforce Extractor

The component exports data from Salesforce based on a SOQL query or an object you provide 
and saves it into the out/tables directory.

**Table of contents:**  
  
[TOC]

# Configuration

## Authorization

- **User Name** - (REQ) your user name, when exporting data from sandbox don't forget to add .sandboxname at the end
- **Password** - (REQ) your password
- **Security Token** - (REQ) your security token, don't forget it is different for sandbox
- **sandbox** - (REQ) true when you want to export data from sandbox

## Fetching whole Objects

- **Object** - Salesforce object identifier, eg. Account.

## Fetching using SOQL queries

- **SOQL query** - Salesforce SOQL query, eg. SELECT Id, FirstName, LastName FROM Contact

## Load type
If set to Incremental update, the result tables will be updated based on primary key. 
Full load overwrites the destination table each time.
### Incremental fetching 

Incremental fetching allows the fetching of only the records that have been modified since the previous run of
the component. This is done by specifying an incremental field in the object that contains data on when it wast last modified.


**Example:**

```json
{
  "parameters": {
    "username": "fakeuser@fakemail.com",
    "#password": "password",
    "#security_token": "token",
    "sandbox" : false,
    "object" : "",
    "soql_query" : "select Id,FirstName,LastName,isdeleted,lastmodifieddate from Contact",
    "is_deleted" : false,
    "incremental" : true,
    "incremental_field" : "lastmodifieddate",
    "incremental_fetching" : true
  }
}
```


