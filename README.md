# Tableau Data Update API Examples

## Disclaimer - Does Not Work Yet!

Currently posting this code to solicit help from the Tableau DataDev Slack community.
Hopefully, this will be a working example before the new API's GA release.

## Summary

This is a very simple python example use case of the upcoming Tableau Data Update API.
Upon GA, this project will be updated with any syntax changes and more example use cases.


## Setup

### Tableau Online Developer Site
This is an API pre-release only available to members of the 
[Tableau DataDev Program](https://www.tableau.com/developer). Follow the link to join today!

### Python Package Requirements:
  - Tableau Server Client
    - `pip install tableauserverclient`
  - Tableau Hyper API
    - `pip install tableauhyperapi`
    
### Update Helper File With Your Info
In order to customize this example to work in your environment, 
update the block below from tableau_server_helper.py file with your info.

```python:
auth = TableauAuth('my_tableau_login', 'my_tableau_password', site_id='my_tableau_site')
server = Server('https://10ax.online.tableau.com', use_server_version=True)
ds_project = 'default'
```
