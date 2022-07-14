from os import environ
from tableauserverclient import TableauAuth, Server

my_tableau_login = environ['TABLEAU_LOGIN']
my_tableau_password = environ['TABLEAU_PASSWORD']
my_tableau_site = environ['TABLEAU_SITE']

tab_auth = TableauAuth(my_tableau_login, my_tableau_password, site_id=my_tableau_site)
server = Server('https://10ax.online.tableau.com', use_server_version=True)
new_ds_project = 'default'
new_ds_name = 'Data Update API Test'
