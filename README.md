# Salesforce Extractor

The component exports data from Salesforce based on a SOQL query or an object you provide 
and saves it into the out/tables directory.

**Table of contents:**  
  
[TOC]
# Prerequisites 

## Choose an authorization method

In order to authorize the component you can select one of the two following methods of authorization:
* With a username, password, and **Security Token** 
* With a username, password, and **Connected App** 
* With a Consumer Key and a Consumer Secret using **OAuth 2.0 Client Credentials Flow**

In the below sections we describe how to get a security token, and how to set up a connected app.

### How to obtain a Security Token

If you do not have your Security Token, you will have to reset your current one following the steps outlined in the
[Salesforce Documentation](https://help.salesforce.com/s/articleView?id=sf.user_security_token.htm&type=5).

### How to set up a Connected App

In the Salesforce Setup menu navigate to Platform Tools > Apps > App Manager.

On the top left you should see a button labeled *New Connected App* and click on it.

In New Connected App fill in :

* *Connected App Name* : name the app distinctively, e.g. Keboola Connection App
* *API Name* :  will be automatically generated from the App name
* *Contact Email* : your email

Next in the API(Enable OAuth Settings) check the *Enable OAuth Settings* box. More options should appear. Enable the 
*Enable for Device Flow* box and the *Callback URL* should be automatically set. Next select the following scopes in the 
'Selected OAuth Scopes' section :

*  Manage user data via APIs (api)
*  Access unique user identifiers (openid)
*  Perform requests at any time (refresh_token, offline_access)

Once filled in, click the *save* button on the top of the form. Now your App should be created, click the shown *Continue*
button to advance.

Next find you newly created App in the  Platform Tools > Apps > App Manager. Once you see your app, in the row that contains the
right App Name click on the downward facing arrow on the right and click *Manage*, you should now be able to see the
Connection App Policies. On the top you should see an *Edit Policies* button, click it.

In the *OAuth Policies* find the *IP Relaxation* and select the  *Relax IP restrictions*.
Leave everything else the same and click *save*

Now finally to get the *Consumer Key* and *Consumer Secret* go back to Platform Tools > Apps > App Manager, find the 
App and in the row that contains the right App Name click on the downward facing arrow on the right and click *View*.
In the *API (Enable OAuth Settings)* > *Consumer Key and Secret* you should see a button *Manage Consumer Details*, click on it.

You might be prompted to fill in a Verification Code. Fill it in.

Now you should see the Consumer details. Save the Key and Secret to a password manager or other secure space.

### Enabling Client Credentials Flow

To enable client credentials flow, you need to create a Connected App in Salesforce. This process is described in **How to set up a Connected App** section above.
The only difference will be checking **Enable Client Credentials Flow** for the Connected App.

You also need to fill in the **domain** field for this type of Authorization.

# Configuration

## Authorization

- **Login Method** (login_method) - Select either "security_token" or "connected_app", Default : "security_token"
- **Username** (username) - (REQ) your username, when exporting data from sandbox don't forget to add .sandboxname at the end
- **Password** (#password) - (REQ) your password
- **Sandbox** (sandbox) - (REQ) true when you want to export data from sandbox

If "security_token" login method is selected:
- **Security Token** (#security_token) - (REQ) your security token, don't forget it is different for sandbox

If "connected_app" login method:
- **Consumer Key** (#consumer_key) - (REQ) The Consumer Key of your Connected App
- **Consumer Secret** (#consumer_secret) - (REQ) The Consumer Secret of your Connected App
- 
## Row configuration
 - Query type (query_type_selector) - [REQ] Either "Object" or  "Custom SOQL"
 - Get deleted records (is_deleted) - [OPT] Fetch records that have been deleted
 - API version (api_version) - [OPT] Specify the version of API you want to extract data from

### Fetching whole Objects

- **Object** - Salesforce object identifier, eg. Account.

### Fetching using SOQL queries

- **SOQL query** - Salesforce SOQL query, eg. SELECT Id, FirstName, LastName FROM Contact

## Load type
If set to Incremental update, the result tables will be updated based on primary key. 
Full load overwrites the destination table each time.

### Incremental fetching 

Incremental fetching allows the fetching of only the records that have been modified since the previous run of
the component. This is done by specifying an incremental field in the object that contains data on when it wast last modified.


**Example: With Security Token**

```json
{
  "parameters": {
    "username": "user@keboola.com",
    "#password": "password",
    "#security_token": "token_here",
    "sandbox": false,
    "api_version" : "52.0",
    "object": "Contact",
    "soql_query": "select Id, FirstName,LastName,isdeleted,lastmodifieddate from Contact",
    "is_deleted": false,
    "loading_options": {
      "incremental": true,
      "pkey": [
        "Id"
      ],
      "incremental_field": "lastmodifieddate",
      "incremental_fetch": true
    }
  }
}
```


