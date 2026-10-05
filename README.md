# Salesforce Extractor

The component exports data from Salesforce based on a SOQL query or an object you provide 
and saves it into the out/tables directory.

**Table of contents:**  
  
[TOC]
# Prerequisites 

## Choose an authorization method

In order to authorize the component you can select one of the following methods of authorization:
* With a Consumer Key and a Consumer Secret using **OAuth 2.0 Client Credentials Flow** - recommended
* With a username, password, and **Security Token** - **deprecated**, Salesforce retires it in Summer '27
* With a username, password, and **External Client App** (formerly Connected App) - **deprecated**, Salesforce
  retires it on **2027-02-20**

Salesforce is retiring both username and password methods. See
[Retirement of the username and password login methods](#retirement-of-the-username-and-password-login-methods)
for the dates and the migration steps.

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

## Retirement of the username and password login methods

Salesforce is retiring the two login methods that send a username and a password. Only **Connected App OAuth
2.0 Client Credentials** (`connected_app_oauth_cc`) continues to work.

| Login method | What it uses | Stops working |
| --- | --- | --- |
| `connected_app` - Connected App with Username and Password | OAuth 2.0 username-password flow | **2027-02-20** |
| `security_token` - Security Token with Username and Password | SOAP API `login()` | Summer '27 release, about May 2027 |
| `connected_app_oauth_cc` - Connected App OAuth 2.0 Client Credentials | OAuth 2.0 client credentials flow | not affected |

Both retiring methods are already unavailable in new Salesforce orgs: the username-password flow in orgs created
in Summer '26 or later, and `login()` in orgs created in Winter '26 or later. A configuration that still uses a
retiring method prints a warning in its job log on every run.

Raising the API version does not help. `login()` exists only up to API version 64.0 and is retired as a whole,
and the username-password flow is retired for every version.

The default login method of a new configuration is still `security_token`. The Keboola config UI writes a
schema default into a stored configuration the first time a user opens and saves it, so changing the default
would rewrite every older configuration that has no `login_method` of its own into a method its stored
credentials cannot satisfy. Those configurations have to be backfilled with
`"login_method": "security_token"` before the default can move.

### Migrating to Client Credentials

1. Follow [Enabling Client Credentials Flow](#enabling-client-credentials-flow) on the Salesforce side. If you
   already use `connected_app`, you keep the same app: you only enable the Client Credentials Flow on it and set
   a **Run As** user.
2. In the Keboola configuration, set **Login Method** to *Connected App OAuth 2.0 Client Credentials*.
3. Fill in **Domain** with your Salesforce domain, for example `https://your-org.my.salesforce.com`.
4. Keep (or re-enter) the **Consumer Key** and **Consumer Secret**. A `connected_app` configuration already holds
   both, and they stay valid.
5. Run the configuration to confirm the login works. **Username**, **Password** and **Security Token** are not
   used by this method.

Because this method authenticates as the app's Run As user and not as the person who created the configuration,
check that the Run As user has access to every object the configuration reads.

# Configuration

## Authorization

- **Login Method** (login_method) - One of `connected_app_oauth_cc`, `connected_app` or `security_token`.
  Default: `security_token`. Use `connected_app_oauth_cc`; the other two are deprecated, see
  [Retirement of the username and password login methods](#retirement-of-the-username-and-password-login-methods).
- **Sandbox** (sandbox) - (REQ) true when you want to export data from sandbox

If `connected_app_oauth_cc` login method is selected (recommended):
- **Consumer Key** (#consumer_key) - (REQ) The Consumer Key of your External Client App or Connected App
- **Consumer Secret** (#consumer_secret) - (REQ) The Consumer Secret of your External Client App or Connected App
- **Domain** (domain) - (REQ) Your Salesforce domain, e.g. `https://your-org.my.salesforce.com`

If `security_token` login method is selected (DEPRECATED):
- **Username** (username) - (REQ) your username, when exporting data from sandbox don't forget to add .sandboxname at the end
- **Password** (#password) - (REQ) your password
- **Security Token** (#security_token) - (REQ) your security token, don't forget it is different for sandbox

If `connected_app` login method is selected (DEPRECATED):
- **Username** (username) - (REQ) your username
- **Password** (#password) - (REQ) your password
- **Consumer Key** (#consumer_key) - (REQ) The Consumer Key of your Connected App
- **Consumer Secret** (#consumer_secret) - (REQ) The Consumer Secret of your Connected App

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


