from pathlib import Path
import tempfile, psycopg2
from google.cloud import storage

def _download(bucket: str, blob: str, dest: Path) -> str:
    """Download <gs://bucket/blob> → <dest>, return str(dest)."""
    storage.Client().bucket(bucket).blob(blob).download_to_filename(dest)
    return str(dest)

def get_db_connection_with_gcs_certs(
    dbname, user, password, host, port,
    bucket_name, client_cert_gcs, client_key_gcs, server_ca_gcs,
):
    tmp = Path(tempfile.mkdtemp(prefix="pg_ssl_"))   # e.g. /tmp/pg_ssl_abcd

    client_cert = _download(bucket_name, client_cert_gcs, tmp / "client-cert.pem")
    client_key  = _download(bucket_name, client_key_gcs,  tmp / "client-key.pem")
    server_ca   = _download(bucket_name, server_ca_gcs,   tmp / "server-ca.pem")

    return psycopg2.connect(
        dbname=dbname,
        user=user,
        password=password,
        host=host,                 # 10.247.163.124
        port=port,                 # 5432
        sslmode="verify-ca",
        sslcert=client_cert,
        sslkey=client_key,
        sslrootcert=server_ca,
    )
******************************
# On the Dataproc driver/worker:
ls -l /tmp/pg_ssl_*/*.pem
# Should show client-cert.pem, client-key.pem, server-ca.pem
***************
from google.cloud.sql.connector import Connector
import pg8000
connector = Connector()

conn = connector.connect(
    "PROJECT:REGION:INSTANCE",   # instance connection name
    "pg8000",
    user=user,
    password=password,
    db=dbname,
)
