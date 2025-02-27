# Salesforce Extractor

The component exports data from Salesforce using an SOQL query or a specified object you provide 
and saves it in the `out/tables` directory.

**Table of contents:**  
  
[TOC]
# Prerequisites 

## Choose an Authorization Method

To authorize the component, select one of the following authentication methods:
* With a username, password, and **Security Token** 
* With a username, password, and **Connected App** 
* With a Consumer Key and a Consumer Secret using **OAuth 2.0 Client Credentials Flow**

The following sections explain how to obtain a security token and set up a Connected App.

### Obtaining a Security Token

If you do not have your Security Token, you will have to reset your current one by following the steps outlined in the
[Salesforce Documentation](https://help.salesforce.com/s/articleView?id=sf.user_security_token.htm&type=5).

### Setting Up a Connected App

In the Salesforce Setup menu, navigate to *Platform Tools* > *Apps* > *App Manager*.

Click the **New Connected App** button at the top left.

In the *New Connected App*, fill in the following fields:

* **Connected App Name**: Name the app distinctively, e.g., Keboola App.
* **API Name**:  This will be automatically generated from the App Name.
* **Contact Email**: Your email address.

Next, in the API (*Enable OAuth Settings*), check the **Enable OAuth Settings** box. More options will appear. Enable the 
**Enable for Device Flow** box, and the **Callback URL** should be automatically set. Then, in the 
*Selected OAuth Scopes* section, select the following scopes:

*  Manage user data via APIs (`api`)
*  Access unique user identifiers (`openid`)
*  Perform requests at any time (`refresh_token`, `offline_access`)

Once completed, click the **Save** button at the top of the form. Your app should now be created. Click the **Continue**
button to proceed.

Next, find your newly created app in  *Platform Tools > Apps > App Manager*. Locate the app in the list, click the dropdown arrow next to its name,
and select **Manage**. In the *Connection App Policies*, click **Edit Policies**.

Under *IP Relaxation*, select **Relax IP restrictions**.
Leave everything else unchanged and click **Save**.

Finally, to retrieve the **Consumer Key** and **Consumer Secret**, return to *Platform Tools > Apps > App Manager*. Locate your app,
click the dropdown arrow next to its name, and select **View**.
Under *API (Enable OAuth Settings)*, find **Consumer Key and Secret** and click **Manage Consumer Details**.

You may be prompted for a verification code. Enter the code to view your Consumer details. Save the Key and Secret in a password manager or another secure location.

### Enabling Client Credentials Flow

To enable Client Credentials Flow, create a Connected App in Salesforce by following the **How to set up a Connected App** section above.
The only difference is that you must check **Enable Client Credentials Flow** for the Connected App.

Additionally, you need to provide the **domain** field for this type of authorization.

# Configuration

## Authorization

- **Login Method** (`login_method`) - Choose between `security_token` or `connected_app`. Default: `security_token`.
- **Username** (`username`) - (REQ) Your Salesforce username. If exporting data from a sandbox, append `.sandboxname` to the username.
- **Password** (`#password`) - (REQ) Your Salesforce password.
- **Sandbox** (`sandbox`) - (REQ) Set to `true` to export data from a Saleforce sandbox.

If the `security_token` login method is selected:
- **Security Token** (`#security_token`) - (REQ) Your security token. Note that it is different for sandboxes.

If the `connected_app` login method is selected:
- **Consumer Key** (`#consumer_key`) - (REQ) The Consumer Key of your Connected App.
- **Consumer Secret** (`#consumer_secret`) - (REQ) The Consumer Secret of your Connected App.
- 
## Row Configuration
 - **Query Type** (`query_type_selector`) - [REQ] Select `Object` or `Custom SOQL`.
 - **Get Deleted Records** (`is_deleted`) - [OPT] Fetch records that have been deleted.
 - **API Version** (`api_version`) - [OPT] Select the Salesforce API version to use for data extraction.

### Fetching Full Objects (with Native Data Types Support)

- **Object** - Salesforce object identifier, e.g., `Account`.

### Fetching Data with SOQL Queries (All Columns Stored as Strings)

- **SOQL query** - Salesforce SOQL query, e.g., `SELECT Id`, `FirstName`, `LastName FROM Contact`

## Load Type
If `Incremental` is enabled, result tables will be updated based on the primary key. 
A full load overwrites the destination table each time. 
You can override the default table name using the `output_table_name` parameter and specify a Storage bucket using `bucket_name`.

### Incremental Fetching 

Incremental fetching allows the retrieval of only records that have been modified since the component's last run. 
To enable incremental fetching, specify an incremental field that tracks when records were last modified.

**Example: Using Security Token**

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


