import os
from prefect_gcp import GcpCredentials, CloudRunJob, GcsBucket
from prefect_gcp.bigquery import GcpCredentials, BigQueryWarehouse


# replace this PLACEHOLDER dict with your own service account info
service_account_info = {
  "type": "service_account",
  "project_id": "PROJECT_ID",
  "private_key_id": "KEY_ID",
  "private_key": "-----BEGIN PRIVATE KEY-----\nPRIVATE_KEY\n-----END PRIVATE KEY-----\n",
  "client_email": "SERVICE_ACCOUNT_EMAIL",
  "client_id": "CLIENT_ID",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://accounts.google.com/o/oauth2/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/SERVICE_ACCOUNT_EMAIL"
}

GcpCredentials(
    service_account_info=service_account_info
).save("companyname-companyid-gcp-credentials")

gcp_credentials = GcpCredentials.load(os.environ["companyname-companyid-gcp-credentials"])

# must be from GCR and have Python + Prefect
image = f"us-docker.pkg.dev/{os.environ['GCP_PROJECT_ID']}/test-example-repository/prefect-gcp:2-python3.11"  # noqa

cloud_run_job = CloudRunJob(
    image=image,
    credentials=gcp_credentials,
    region="us-central1",
)
cloud_run_job.save(os.environ["companyname-companyid-cloud-run-job"], overwrite=True)

bucket_name = "metricmaven-companyid-clientid-prefect-gcp-bucket"
cloud_storage_client = gcp_credentials.get_cloud_storage_client()
cloud_storage_client.create_bucket(bucket_name)
gcs_bucket = GcsBucket(
    bucket=bucket_name,
    gcp_credentials=gcp_credentials,
)
gcs_bucket.save(os.environ["companyname-companyid-cloud-storage-bucket"], overwrite=True)