from prefect import flow, task
from prefect_gcp.bigquery import GcpCredentials, BigQueryWarehouse

model_sql = {
    "linkedin_ads": "linkedin_ads_model.sql",
    "facebook_ads": "facebook_ads_model.sql",
    "google_ads": "google_ads_model.sql",
}

@task
def run_airbyte_sync(connectionId):
    source_type = 'xxx' # replace with your own source type
    return source_type

@task
def create_source_model(source_type, company_details):
    gcp_credentials = GcpCredentials.load(company_details['gcpCredsBlock'])
    client = gcp_credentials.get_bigquery_client()
    client.create_dataset("metricmaven_prod", exists_ok=True)

    with open(model_sql[source_type], "r") as file:
        query = file.read()

    
    with BigQueryWarehouse(gcp_credentials=gcp_credentials) as warehouse:

            warehouse.execute(
                model_sql[source_type].format(
                     clientid=company_details['clientid'], 
                     clientName=company_details['clientName'], 
                     projectId=company_details['projectId'])
            )

    return source_type

@flow
def run_source_model(connection, source_type, company_details):
    synced = run_airbyte_sync(connection)
    return create_source_model(synced, source_type, company_details)

if __name__ == "__main__":
    run_source_model.run()