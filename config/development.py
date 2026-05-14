### GCP ###
SCOPES             = ['https://www.googleapis.com/auth/devstorage.full_control']
GCP_PROJECT        = 'your-gcp-project-id'           # [SCRUBBED] replace with your GCP project ID
GCP_BUCKET         = 'your-nonprd-datalake-bucket'   # [SCRUBBED] replace with your non-prod GCS bucket name
GCP_BUCKET_PARQUET = 'gcp-storage-parquet'
GCP_BUCKET_MSSQL   = 'gcp-ingest-mssql'
GCP_REGION         = 'asia-southeast1'

# NOTE: GCP_BUCKET_APPLICATION is intentionally NOT defined here.
#       Each flow defines its own GCS_APPLICATION constant because this DB
#       hosts multiple BUs that map to different GCS path prefixes.

DB_TYPE         = 'mssql'
DB_DRIVER       = 'pyodbc'
DB_USER         = 'sqlserver'
DB_PORT         = '1433'
DB_NAME         = 'ERP'          # default DB; individual flows override with their own DB_NAME
DB_HOST_NAME    = 'your-db-hostname'                 # [SCRUBBED] replace with your MSSQL hostname/alias

SECRET_PROJECT_ID      = 'your-gcp-project-id'      # [SCRUBBED] replace with your GCP project ID
SECRET_DB_HOST         = 'your-secret-db-host-key'  # [SCRUBBED] replace with your Secret Manager key name for DB host
SECRET_DB_PASSWORD     = 'your-secret-db-password-key'  # [SCRUBBED] replace with your Secret Manager key name for DB password
SECRET_SERVICE_ACCOUNT = 'your-secret-sa-key'       # [SCRUBBED] replace with your Secret Manager key name for service account JSON
