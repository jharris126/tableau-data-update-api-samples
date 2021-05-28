from tableauserverclient import TableauAuth, Server


auth = TableauAuth('my_tableau_login', 'my_tableau_password', site_id='my_tableau_site')
server = Server('https://10ax.online.tableau.com', use_server_version=True)
ds_project = 'default'
