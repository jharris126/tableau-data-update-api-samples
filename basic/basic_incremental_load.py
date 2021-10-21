import tableauserverclient as TSC
from tableauhyperapi import HyperProcess, Connection, Telemetry, CreateMode, SchemaName, TableName, escape_name
from tableau_server_helper import tab_auth, server, new_ds_project, new_ds_name
import uuid


# Create a New Hyper File with the the same schema as the previous but add 1 row to be updated and 1 to be inserted
def create_incr_hyper(incr_file):
    with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        with Connection(endpoint=hyper.endpoint,
                        database=incr_file,
                        create_mode=CreateMode.CREATE_AND_REPLACE) as connection:

            sql = (
                f"CREATE TABLE {SchemaName('public')}.{TableName('test')} "
                f"({escape_name('colA')}, {escape_name('colB')}) AS "
                "VALUES ('foo', 3), ('bar', 2)"
            )
            connection.execute_command(sql)

    print(f'Incremental hyper file created as: "{incr_file}".')


# use data update api to upsert the new records into the published hyper file on Tableau Server
def publish_incr_hyper(incr_file):

    # action batch descriptors translate to Hyper SQL DDL and tells Tableau Server how to upsert the new data
    actions = [
        {
            'action': 'upsert',
            'target-table': 'test',
            'target-schema': 'public',
            'source-table': 'test',
            'source-schema': 'public',
            'condition': {'op': 'eq', 'target-col': 'colA', 'source-col': 'colA'}
        }
    ]

    with server.auth.sign_in(tab_auth):
        req_option = TSC.RequestOptions()
        req_option.filter.add(TSC.Filter(TSC.RequestOptions.Field.Name,
                                         TSC.RequestOptions.Operator.Equals,
                                         new_ds_name))
        req_option.filter.add(TSC.Filter(TSC.RequestOptions.Field.ProjectName,
                                         TSC.RequestOptions.Operator.Equals,
                                         new_ds_project))
        matching_datasources, pagination_item = server.datasources.get(req_option)
        ds = matching_datasources[0]

        # create a unique upload ID for the request
        req_id = str(uuid.uuid4())

        # TSC update_hyper_data takes in the datasource item, upload ID, actions, and hyper file and pushes to server
        job = server.datasources.update_hyper_data(ds, request_id=req_id, actions=actions, payload=incr_file)
        print(f"Update Data Job posted (luid: {job.id})")

        print("Waiting for job to complete...")
        job = server.jobs.wait_for_job(job)
        print(f"{job.type} job (luid: {job.id}) completed successfully at {job.completed_at}.")


def main():

    # file names for incremental update data
    incr_file = 'data_update_test_changes.hyper'

    # create updated data batch and push as incremental upsert to existing hyper file on Tableau Server
    create_incr_hyper(incr_file)
    publish_incr_hyper(incr_file)


if __name__ == '__main__':
    main()