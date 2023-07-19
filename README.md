# plugin-mzc-google-cost-datasource

Plugin for collecting GCP Billing data

---

## Secret Data

*Schema*

* project_id (str): project_id is a unique identifier for a project and is used only within the GCP console.
* private_key (str): When you add a GCP cloud account, you use a private key for a GCP service account
* token_uri (str): The OAuth 2.0 authorization server’s token endpoint URI.
* client_email (str): A service account's credentials include a generated email address
* bucket (str): Bucket of GCS (e.g. `spaceone-billing-data`)
* folders(list)`OPTIONAL`: Specifies a sub-specific folder within the bucket

```
project_id, private_key, token_uri and client_email can be obtained from api_key issued when creating service_account.  

```

<br>

*Example 1*

```python
{
    "project_id": "<project_id>",
    "private_key": "*****",
    "client_email": "<service_account_name>@<project_id>.iam.gserviceaccount.com",
    "token_uri": "https://oauth2.googleapis.com/token",
    "bucket": "spaceone-billing-data"
}
```

- Collect all cost data under `spaceone-billing-data`.

<br>

*Example 2*
```python
{
    "project_id": "<project_id>",
    "private_key": "*****",
    "client_email": "<service_account_name>@<project_id>.iam.gserviceaccount.com",
    "token_uri": "https://oauth2.googleapis.com/token",
    "bucket": "spaceone-billing-data",
    "folders":[
        "<organization name_1 (spaceone domain name)>",
        "<organization name_2 (spaceone domain name)>",
        "<organization name_3 (spaceone domain name)>"
    ]
}
```

- When `folders` are set, cost data is collected for a specific subdivision of the `spaceone-billing-data` bucket.

<br>

## Options

Currently, not required.