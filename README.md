# plugin-mzc-google-cost-datasource

Plugin for collecting GCP Billing data

---

## Secret Data

*Schema*

* project_id (str): project_id is a unique identifier for a project and is used only within the GCP console.
* private_key (str): When you add a GCP cloud account, you use a private key for a GCP service account
* token_uri (str): The OAuth 2.0 authorization server’s token endpoint URI.
* client_email (str): A service account's credentials include a generated email address
* collect (dict): You can specify the bucket you want to collect and a specific sub_billing_account_id.

```
project_id, private_key, token_uri and client_email can be obtained from api_key issued when creating service_account.  

```

<br>

*Example 1*
<pre>
<code>
{
    "project_id": "*****"
    "private_key": "*****",
    "token_uri": "https://oauth2.googleapis.com/token",
    "client_email": "*****@*****.iam.gserviceaccount.com",
    "collect": {
        "bucket": "bucket_name",
        "sub_billing_account_id": [
                                    "billing_account_id_1",
                                    "billing_account_id_2",
                                    "billing_account_id_3",
                                   ]
    }
}
</code>
</pre>

- If collect.sub_billing_account_id exists, it can be collected by specific sub_billing_account.

<br>

*Example 2*
<pre>
<code>
{
    "project_id": "*****"
    "private_key": "*****",
    "token_uri": "https://oauth2.googleapis.com/token",
    "client_email": "*****@*****.iam.gserviceaccount.com",
    "collect": {
        "bucket": "bucket_name"
    }
}
</code>
</pre>

- If collect.sub_billing_account_id is not set, all sub_billing_accounts that exist under a specific bucket can be
  collected.

<br>

## Options

Currently, not required.