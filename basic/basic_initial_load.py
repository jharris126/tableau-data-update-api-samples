import tableauserverclient as TSC
from tableauhyperapi import HyperProcess, Connection, Telemetry, CreateMode, SchemaName, TableName, escape_name
from tableau_server_helper import tab_auth, server, new_ds_project, new_ds_name


# create a very basic hyper file with CREATE TABLE AS syntax
def create_init_hyper(init_file):
    with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        with Connection(endpoint=hyper.endpoint,
                        database=init_file,
                        create_mode=CreateMode.CREATE_AND_REPLACE) as connection:
            sql = f"CREATE TABLE {SchemaName('public')}.{TableName('test')} AS "
            sql += f"SELECT 'foo' AS {escape_name('colA')}, 1 AS {escape_name('colB')}"

            connection.execute_command(sql)


# publish this hyper file to Tableau Server
def publish_init_hyper(init_file):
    with server.auth.sign_in(tab_auth):
        for proj in TSC.Pager(server.projects):
            if proj.name == new_ds_project:
                proj_id = proj.id

        ds = TSC.DatasourceItem(proj_id, name=new_ds_name)
        resp = server.datasources.publish(ds, init_file, TSC.Server.PublishMode.Overwrite)
        return resp.id


def main():

    # file name for initial load data
    init_file = 'data_update_test.hyper'

    # create and publish initial load hyper data
    create_init_hyper(init_file)
    publish_init_hyper(init_file)


if __name__ == '__main__':
    main()