# Salesforce Extractor

The component exports data from Salesforce based on a SOQL query or an object you provide 
and saves it into the out/tables directory.

**Table of contents:**  
  
[TOC]
# Prerequisites 

## Choose an authorization method

In order to authorize the component you can select one of the following methods of authorization:
* With a username, password, and **Security Token**
* With a username, password, and **External Client App** (formerly Connected App)
* With a Consumer Key and a Consumer Secret using **OAuth 2.0 Client Credentials Flow**

In the sections below we describe how to obtain a security token and how to set up an External Client App.

> **Note:** Salesforce Spring '26 replaced Connected Apps with **External Client Apps**. If you already have a
> Connected App set up from before Spring '26, it continues to work unchanged — no migration is required. The
> Keboola configuration fields (`consumer_key`, `consumer_secret`, `domain`) are identical for both app types.

### How to obtain a Security Token

If you do not have your Security Token, you will have to reset your current one following the steps outlined in the
[Salesforce Documentation](https://help.salesforce.com/s/articleView?id=sf.user_security_token.htm&type=5).

### How to set up an External Client App

1. In Salesforce Setup, use Quick Find to navigate to **App Manager**.
2. Click **New External Client App**.
3. Fill in the **External Client App Name**, **API Name**, and **Contact Email**.
4. Set the **Distribution State** to **Local** (for use within your current org only).
5. Click **Save**.
6. Scroll down to the **API (Enable OAuth Settings)** section and check **Enable OAuth Settings**.
7. Enter a **Callback URL** (e.g., `https://localhost` — a placeholder is required even if not used).
8. Select the following **OAuth Scopes** and move them to *Selected OAuth Scopes*:
   * Manage user data via APIs (api)
   * Access unique user identifiers (openid)
   * Perform requests at any time (refresh_token, offline_access)
9. Click **Save**.
10. Go to the **Policies** tab of your External Client App, click **Edit**, and set **IP Relaxation** to
    **Relax IP restrictions**.
11. To retrieve the Consumer Key and Secret, go to the **Settings** tab, click **OAuth Settings**, then click
    **Consumer Key and Secret** (you may be prompted for identity verification). Save the Key and Secret to a
    password manager or other secure place.

### Enabling Client Credentials Flow

This method enables server-to-server authentication without user credentials. It requires a **Consumer Key**, a
**Consumer Secret**, and your Salesforce **Domain** (e.g., `https://your-org.my.salesforce.com`).

To create an External Client App with Client Credentials Flow:

1. Follow **steps 1–9** in the *How to set up an External Client App* section above to create the app and
   configure OAuth settings.
2. Under **Flow Enablement**, additionally check **Enable Client Credentials Flow**.
3. Click **Save**.
4. Go to the **Policies** tab and click **Edit**.
5. Set **Permitted Users** to **Admin approved users are pre-authorized**.
6. In the **Run As (Username)** field, enter the Salesforce username whose permissions the integration will use.
7. Click **Save**.
8. Retrieve the Consumer Key and Secret from the **Settings** tab → **OAuth Settings** → **Consumer Key and Secret**.
9. Provide your Salesforce domain in the Keboola configuration (e.g., `https://your-org.my.salesforce.com`).

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

### Fetching whole Objects (with Native Data Types support)

- **Object** - Salesforce object identifier, eg. Account.

### Fetching using SOQL queries (all columns are typed as string)

- **SOQL query** - Salesforce SOQL query, eg. SELECT Id, FirstName, LastName FROM Contact

## Load type
If set to Incremental update, the result tables will be updated based on primary key. 
Full load overwrites the destination table each time. 
It can override the table name with the parameter **output_table_name** and the bucket with the parameter **bucket_name**.

### Incremental fetching 

Incremental fetching allows the fetching of only the records that have been modified since the previous run of
the component. This is done by specifying an incremental field in the object that contains data on when it wast last modified.

#### Incremental Overlap

To prevent missing records due to clock skew or delayed commits in distributed systems, you can configure an overlap period using the `incremental_overlap_seconds` parameter. When set, this parameter subtracts the specified number of seconds from the last watermark, ensuring that records near the boundary are re-fetched.

**Note:** When using overlap, duplicate records may be fetched. However, since incremental mode is configured with a primary key, duplicates will be automatically deduplicated during the loading process.

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
      "incremental_fetch": true,
      "incremental_overlap_seconds": 60
    }
  }
}
```


