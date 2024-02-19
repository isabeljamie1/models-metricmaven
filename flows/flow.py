from prefect import flow
from prefect_gcp.bigquery import GcpCredentials, BigQueryWarehouse

@flow
def bigquery_flow():
    all_rows = []
    gcp_credentials = GcpCredentials.load("BLOCK-NAME-PLACEHOLDER")

    client = gcp_credentials.get_bigquery_client()
    client.create_dataset("test_example", exists_ok=True)

    with BigQueryWarehouse(gcp_credentials=gcp_credentials) as warehouse:
        warehouse.execute(
            "CREATE TABLE IF NOT EXISTS test_example.customers (name STRING, address STRING);"
        )
        warehouse.execute_many(
            "INSERT INTO test_example.customers (name, address) VALUES (%(name)s, %(address)s);",
            seq_of_parameters=[
                {"name": "Marvin", "address": "Highway 42"},
                {"name": "Ford", "address": "Highway 42"},
                {"name": "Unknown", "address": "Highway 42"},
            ],
        )
        while True:
            # Repeated fetch* calls using the same operation will
            # skip re-executing and instead return the next set of results
            new_rows = warehouse.fetch_many("SELECT * FROM test_example.customers", size=2)
            if len(new_rows) == 0:
                break
            all_rows.extend(new_rows)
    return all_rows

if __name__ == "__main__":
    bigquery_flow.run()