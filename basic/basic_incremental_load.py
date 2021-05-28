import tableauserverclient as TSC
from tableauhyperapi import HyperProcess, Connection, Telemetry, CreateMode, SchemaName, TableName, escape_name
import requests
import json
from tableau_server_helper import tab_auth, server, new_ds_project, new_ds_name
import uuid


# does magic around chunk for sending hyper data to Tableau Server
def make_multipart(parts):
    mime_multipart_parts = []
    for name, (filename, blob, content_type) in parts.items():
        multipart_part = requests.packages.urllib3.fields.RequestField(name=name, data=blob, filename=filename)
        multipart_part.make_multipart(content_type=content_type)
        mime_multipart_parts.append(multipart_part)

    post_body, content_type = requests.packages.urllib3.filepost.encode_multipart_formdata(mime_multipart_parts)
    content_type = "".join(("multipart/mixed",) + content_type.partition(";")[1:])
    return post_body, content_type


# opens the hyper file and PUTs it to Tableau Server
def upload_hyper_data(upload_id, incr_file):
    chunk_size = 1024 * 1024 * 5  # 5MB chunks
    put_url = f'{server.baseurl}/sites/{server.site_id}/fileUploads/{upload_id}'

    with open(incr_file, 'rb') as f:
        while True:
            data = f.read(chunk_size)
            if not data:
                break
            payload, content_type = make_multipart(
                {
                    "request_payload": ('', '', 'text/xml'),
                    "tableau_file": ('file', data, 'application/octet-stream'),
                }
            )
            requests.put(
                put_url,
                data=payload,
                headers={'X-Tableau-Auth': server.auth_token, 'content-type': content_type},
            )


# Create a New Hyper File with the the same schema as the previous but add 1 row to be updated and 1 to be inserted
def create_incr_hyper(incr_file):
    with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        with Connection(endpoint=hyper.endpoint,
                        database=incr_file,
                        create_mode=CreateMode.CREATE_AND_REPLACE) as connection:
            sql = f"CREATE TABLE {SchemaName('public')}.{TableName('test')} AS "
            sql += f"SELECT 'foo' AS {escape_name('colA')}, 3 AS {escape_name('colB')} "
            sql += "UNION ALL "
            sql += f"SELECT 'bar' AS {escape_name('colA')}, 2 AS {escape_name('colB')}"

            connection.execute_command(sql)


# use data update api to upsert the new records into the published hyper file on Tableau Server
def publish_incr_hyper(incr_file):

    # action batch descriptors translate to Hyper SQL DDL and tell Tableau Server how to upsert the new data
    actions = {
        'actions': [
            {
                'action': 'upsert',
                'target-table': 'test',
                'target-schema': 'public',
                'source-table': 'test',
                'source-schema': 'public',
                'condition': {'op': 'eq', 'target-col': 'colA', 'source-col': 'colA'}
            }
        ]
    }

    with server.auth.sign_in(tab_auth):
        req_option = TSC.RequestOptions()
        req_option.filter.add(TSC.Filter(TSC.RequestOptions.Field.Name,
                                         TSC.RequestOptions.Operator.Equals,
                                         new_ds_name))
        req_option.filter.add(TSC.Filter(TSC.RequestOptions.Field.ProjectName,
                                         TSC.RequestOptions.Operator.Equals,
                                         new_ds_project))
        matching_datasources, pagination_item = server.datasources.get(req_option)
        ds_id = matching_datasources[0].id

        # get tableau auth token, must use JSON content-type, add a unique request ID
        header = {
            'X-Tableau-Auth': server.auth_token,
            'content-type': 'application/json',
            'accept': 'application/json',
            'RequestID': str(uuid.uuid4())
        }

        # create an upload session ID for large file uploads
        up_endpt = f'{server.baseurl}/sites/{server.site_id}/fileUploads/'
        upload_id = requests.post(up_endpt, headers=header).json()['fileUpload']['uploadSessionId']

        upload_hyper_data(upload_id, incr_file)

        # put together a patch request that passes the header, actions, hyper file, and upload ID to Tableau Server
        endpt = f'{server.baseurl}/sites/{server.site_id}/datasources/{ds_id}/data?uploadSessionId={upload_id}'
        response = requests.patch(endpt, json.dumps(actions), headers=header).json()
        print(response)


def main():

    # file names for incremental update data
    incr_file = 'data_update_test_changes.hyper'

    # create updated data batch and push as incremental upsert to existing hyper file on Tableau Server
    create_incr_hyper(incr_file)
    publish_incr_hyper(incr_file)


if __name__ == '__main__':
    main()