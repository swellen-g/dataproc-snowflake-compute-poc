output "project_id" {
  description = "GCP Project ID."
  value       = var.project_id
}

output "dataproc_cluster_name" {
  description = "Dataproc cluster name."
  value       = google_dataproc_cluster.benchmark_cluster.name
}

output "dataproc_cluster_region" {
  description = "Region of Dataproc cluster."
  value       = var.region
}

output "gcs_bucket_name" {
  description = "GCS bucket used for staging, scripts, and temporary data."
  value       = google_storage_bucket.poc_bucket.name
}

output "pyspark_script_uri" {
  description = "GCS URI of the deployed Snowflake compute pipeline script."
  value       = "gs://${google_storage_bucket.poc_bucket.name}/${google_storage_bucket_object.pyspark_script.name}"
}

output "quick_test_submit_command" {
  description = "Ready-to-run gcloud command to test the pipeline on Dataproc with synthetic data."
  value       = "gcloud dataproc jobs submit pyspark gs://${google_storage_bucket.poc_bucket.name}/${google_storage_bucket_object.pyspark_script.name} --cluster=${google_dataproc_cluster.benchmark_cluster.name} --region=${var.region} -- --test_mode --test_rows 1000000 --output_gcs_path gs://${google_storage_bucket.poc_bucket.name}/output/test_run/"
}

output "snowflake_pipeline_submit_command" {
  description = "Example gcloud command to run the pipeline reading from Snowflake and writing results back to Snowflake."
  value       = "gcloud dataproc jobs submit pyspark gs://${google_storage_bucket.poc_bucket.name}/${google_storage_bucket_object.pyspark_script.name} --cluster=${google_dataproc_cluster.benchmark_cluster.name} --region=${var.region} --packages=net.snowflake:spark-snowflake_2.12:2.16.0-spark_3.4,net.snowflake:snowflake-jdbc:3.16.0 -- --sf_url YOUR_ACCOUNT.snowflakecomputing.com --sf_user YOUR_USER --sf_database YOUR_DB --sf_schema YOUR_SCHEMA --sf_warehouse YOUR_WH --sf_source_table YOUR_SOURCE_TABLE --sf_target_table YOUR_TARGET_TABLE"
}
