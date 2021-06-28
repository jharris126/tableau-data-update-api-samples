# Tableau Update Data Source PATCH Endpoint Examples

## Summary

The Tableau Update Data Source PATCH endpoint for the REST API allows Hyper API users to push delta extract refresh changes to Hyper data living on Tableau Server without the need to bring the entire .hyper file down to your local environment and publish it back up. This repo
holds a few simple examples to help python Hyper API users understand what the API does and how to interact with it in as concise of 
code as possible that can be used as a template for building their own use cases.


## Setup

### Tableau Developer Official Docs
This endpoint is availabile on Tableau Server 2021.2 and up. Offical documentation for the end point is available [here](https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_concepts_update_data.htm).

### Python Package Requirements:
  - Tableau Server Client
    - `pip install tableauserverclient`
  - Tableau Hyper API
    - `pip install tableauhyperapi`
    
### Update Helper File With Your Info
In order to customize this example to work in your environment, 
update the block below from tableau_server_helper.py file with your info.

```python:
auth = TableauAuth('my-tableau-login', 'my-tableau-password', site_id='my-tableau-site')
server = Server('https://10ax.online.tableau.com', use_server_version=True)
new_ds_project = 'my-project'
new_ds_name = 'my-data-source-name'
```

## How To Use

- Basic Example
  - Install python dependencies above
  - Navigate to the "basic" folder
  - Update the helper file as shown above
    - `my-tableau-login` is your username/email
    - `my-tableau-site` is the site id or url name you wish to publish to
    - `my-project` is the name of the project you wish to publish to
    - `my-data-source-name` is the name you want to assign to the new data source to be created and subsequently updated
  - Run basic_initial_load.py and navigate Tableau Server/Online to the newly created data source based on the names you provided in the helper file. The data source should have 1 row with 2 Columns (colA = 'foo' and colB = 1).
  - Run the basic_incremental_load.py file and look at the same data source again. There should now be 2 row. The row where colA = 'foo' should now have colB = 3. You should also see a new row where colA = 'bar' and colB = 2 as well.
