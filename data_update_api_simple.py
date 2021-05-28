import tableauserverclient as TSC
from tableauhyperapi import HyperProcess, Connection, Telemetry, CreateMode, SchemaName, TableName, escape_name
import requests
import json
from datetime import datetime
from tableau_server_helper import auth, server, ds_project


# create a very basic hyper file with CREATE TABLE AS syntax
def create_init_hyper(init_file):
    with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        with Connection(endpoint=hyper.endpoint,
                        database=init_file,
                        create_mode=CreateMode.CREATE_AND_REPLACE) as connection:
            sql = f"CREATE TABLE {SchemaName('public')}.{TableName('test')} AS "
            sql += f"SELECT 1 AS {escape_name('foo')}, 2 AS {escape_name('bar')}"

            connection.execute_command(sql)


# publish this hyper file to Tableau Server
def publish_init_hyper(init_file):
    with server.auth.sign_in(auth):
        for proj in TSC.Pager(server.projects):
            if proj.name == ds_project:
                proj_id = proj.id

        ds = TSC.DatasourceItem(proj_id, name='Data Update API Test')
        resp = server.datasources.publish(ds, init_file, TSC.Server.PublishMode.Overwrite)
        return resp.id


# Create a New Hyper File with the the same schema as the previous but add 1 row to be updated and 1 to be inserted
def create_incr_hyper(incr_file):
    with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        with Connection(endpoint=hyper.endpoint,
                        database=incr_file,
                        create_mode=CreateMode.CREATE_AND_REPLACE) as connection:
            sql = f"CREATE TABLE {SchemaName('public')}.{TableName('test')} AS "
            sql += f"SELECT 1 AS {escape_name('foo')}, 3 AS {escape_name('bar')}"
            sql += " UNION ALL "
            sql += f"SELECT 4 AS {escape_name('foo')}, 5 AS {escape_name('bar')}"

            connection.execute_command(sql)


# use data update api to upsert the new records into the published hyper file on Tableau Server
def publish_incr_hyper(ds_luid, incr_file):

    # action batch descriptors translate to Hyper SQL DDL and tell Tableau Server how to upsert the new data
    actions = {
        'actions': [
            {
                'action': 'upsert',
                'target-table': 'test',
                'target-schema': 'public',
                'source-table': 'test',
                'source-schema': 'public',
                'condition': {'op': 'eq', 'target-col': 'foo', 'source-col': 'foo'}
            }
        ]
    }

    with server.auth.sign_in(auth):
        # get tableau auth token, must use JSON content-type, add a unique request ID
        header = {
            'X-Tableau-Auth': server.auth_token,
            'content-type': 'application/json',
            'accept': 'application/json',
            'RequestID': datetime.now().strftime('%m/%d/%Y, %H:%M:%S')
        }

        # create an upload session ID for large file uploads
        up_endpt = f'{server.baseurl}/sites/{server.site_id}/fileUploads/'
        up_sess = requests.post(up_endpt, headers=header).json()['fileUpload']['uploadSessionId']

        # This is where I'm stuck!
        # put together a patch request that passes the headers, actions hyper file, and upload ID to Tableau Server
        endpt = f'{server.baseurl}/sites/{server.site_id}/datasources/{ds_luid}/data?uploadSessionId={up_sess}'
        response = requests.patch(endpt, json.dumps(actions), headers=header).text
        print(response)


def main():

    # file names for initial load data and incremental update data
    init_file = 'data_update_test.hyper'
    incr_file = 'data_update_test_changes.hyper'

    # create and publish initial load hyper data, grab data source luid from newly created initial load data
    create_init_hyper(init_file)
    ds_luid = publish_init_hyper(init_file)

    # create updated data batch and push as incremental upsert to existing hyper file on Tableau Server
    create_incr_hyper(incr_file)
    publish_incr_hyper(ds_luid, incr_file)


if __name__ == '__main__':
    main()