# --- FIXED callable: pull operator kwargs correctly and handle missing keys gracefully ---
def _get_op_kwargs(kwargs: dict) -> dict:
    """Return the keyword arguments passed to PythonOperator.op_kwargs.
    Airflow passes them directly as kwargs; older code may expect kwargs['op_kwargs'].
    """
    if "op_kwargs" in kwargs and isinstance(kwargs["op_kwargs"], dict):
        return kwargs["op_kwargs"]
    # In Airflow 2.x, op_kwargs are merged into **kwargs, so just return kwargs minus Airflow context keys.
    # Strip some common context keys so we don't accidentally treat them as pipeline configs.
    context_keys = {
        "ti", "task_instance", "ds", "ts", "dag", "dag_run", "run_id", "logical_date",
        "execution_date", "prev_ds", "next_ds", "data_interval_start", "data_interval_end",
        "macros", "conf", "params"
    }
    return {k: v for k, v in kwargs.items() if k not in context_keys}

def _run_pipeline(**kwargs):
    """
    Python function to be executed by the PythonOperator.
    Downloads certificates, connects to DB, runs processing, and uploads result.
    """
    logging.basicConfig(level=getattr(logging, LOG_LEVEL.upper(), logging.INFO), format="%(levelname)s: %(message)s", force=True)

    # Pull operator kwargs safely (works whether or not they're nested under 'op_kwargs')
    op = _get_op_kwargs(kwargs)

    # Validate required keys early with readable errors
    required = [
        "bucket_name", "client_cert_gcs", "client_key_gcs", "server_ca_gcs",
        "dbname", "user", "password", "host", "port",
        "output_gs_uri", "output_fmt", "output_sheet_name", "output_auto_increment"
    ]
    missing = [k for k in required if k not in op]
    if missing:
        raise ValueError(f"Missing required op_kwargs: {missing}")

    # Define local paths for the downloaded certificates
    local_certs_dir = "/tmp/certs"
    os.makedirs(local_certs_dir, exist_ok=True)
    local_client_cert = os.path.join(local_certs_dir, "client-cert.pem")
    local_client_key = os.path.join(local_certs_dir, "client-key.pem")
    local_server_ca = os.path.join(local_certs_dir, "server-ca.pem")

    # Download certs from GCS
    download_cert_from_gcs(op["bucket_name"], op["client_cert_gcs"], local_client_cert)
    download_cert_from_gcs(op["bucket_name"], op["client_key_gcs"], local_client_key)
    download_cert_from_gcs(op["bucket_name"], op["server_ca_gcs"], local_server_ca)

    # Establish DB connection and run query
    with get_db_connection_with_gcs_certs(
        op["dbname"], op["user"], op["password"], op["host"], op["port"],
        local_client_cert, local_client_key, local_server_ca
    ) as conn:
        query = "SELECT * FROM pdipp.prvrostercnf_conformed_file_stats"
        result_df = pd.read_sql_query(query, con=conn)

    # Run the processing and writing pipeline
    df_final, written_uri = run_pipeline_from_df(
        result_df,
        gs_uri=op["output_gs_uri"],
        fmt=op["output_fmt"],
        sheet_name=op["output_sheet_name"],
        auto_increment=op["output_auto_increment"]
    )

    logging.info("Pipeline finished. Final output URI: %s", written_uri)
    # Returning a string will push it to XCom automatically
    return written_uri
