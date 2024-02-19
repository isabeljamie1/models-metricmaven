from prefect import flow, task, deploy
from prefect_gcp.bigquery import GcpCredentials, BigQueryWarehouse
from prefect.blocks.system import JSON

from .client_source_flow import run_source_model

# These are the details we need to run the flow workflow

company_details = {
    "projectId": "xxx",
    "clientId": "xxx",
    "clientName": "xxx",
    "sourceTypes": ["xxx"],
    "connections": ["xxx"],
    "gcpCredsBlock": f"clientId-clientName-gcp-credentials",
    "companyName": "xxx",
    "companyId": "xxx",
    "companyDetailsBlock": "clientId-clientName-company-details",
}


@task
def get_client_details(company_details):
    '''
    The purpose of this task is to create a block in Prefect Cloud 
    that stores all of the details of the client including their sources and connections.

    We need to update the block value in Prefect so the flow can access each of the sources to build the models.

    '''
    source_type = source_type.lower().replace("-", "_")

    try:

        json_block = JSON.load(company_details['companyDetailsBlock'])

        json_block['sourceTypes'].append(source_type)
        json_block['sourceTypes'] = list(set(json_block['sourceTypes']))

        json_block['connections'].append(company_details['connections'])
        json_block['connections'] = list(set(json_block['connections']))

        json_block.save(company_details['companyDetailsBlock'], overwrite=True)
    
    except:

        json_block = JSON(company_details)
        
        json_block.save(company_details['companyDetailsBlock'])

        GcpCredentials(
            service_account_info="XXX" # replace with your own service account info
        ).save(company_details['gcpCredsBlock'])

    return json_block

@task
def run_agg_model(models_built, company_details):
    '''
    If a client has multiple sources, this step will aggregate the models into one model that can be used to analyze all of the data together.
    '''
    if 'facebook-ads' in models_built & 'google-ads' in models_built:

        with open('all_channels_combined.sql', "r") as file:
            query = file.read()

        with BigQueryWarehouse(gcp_credentials="clientid-clietname-gcp-credentials") as warehouse:

            warehouse.execute(
                query.format(
                    clientid=company_details['clientid'], 
                    clientName=company_details['clientName'], 
                    projectId=company_details['projectId']
                )
            )

        return models_built.append('all_channels_combined')
    
    else:
        return models_built

@flow(name=company_details['companyName'].replace(' ','-') + '-' + company_details['companyId'] + '-build-client-models')
def build_client_models(company_details):
    '''
    This flow build the models for a client based on the sources and connections they have.

    It submits each connection/source type to run source model to handle the creation of the model in BigQuery.
    '''
    updated_detail = get_client_details(company_details)
    models_built = []

    for connection, source_type in updated_detail['connections'], updated_detail['sourceTypes']:
        source_model = run_source_model(connection, source_type, company_details)
        models_built.append(source_model)

    if len(models_built) == 1:
        return models_built[0] + ' model built successfully.'
    
    else:
        return run_agg_model(models_built, company_details) + ' models built successfully.'
    

if __name__ == "__main__":
    # This will deploy or update the flow to Prefect Cloud.
    # in the future we will update this to pull from a repo instead of our app.
    build_client_models.deploy(
        name=f"{company_details['companyName'].replace(' ','-')}-deployment",
        work_pool_name="metricmaven-prod-pool", 
        image="my-image:my-tag",
        push=True
    )