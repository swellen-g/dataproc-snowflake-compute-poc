variable "project_id" {
  description = "The GCP Project ID where resources will be deployed."
  type        = string
}

variable "region" {
  description = "Google Cloud Region for the resources."
  type        = string
  default     = "us-central1"
}

variable "zone" {
  description = "Google Cloud Zone for Dataproc primary master."
  type        = string
  default     = "us-central1-a"
}

variable "cluster_name" {
  description = "Name of the benchmark Dataproc cluster."
  type        = string
  default     = "dataproc-poc-cluster"
}

variable "bucket_name" {
  description = "GCS bucket name for staging, data and scripts. Leave empty to autogenerate."
  type        = string
  default     = ""
}

variable "bigquery_dataset_id" {
  description = "BigQuery dataset ID for benchmark output."
  type        = string
  default     = "pyspark_benchmark_results"
}

variable "master_machine_type" {
  description = "Machine type for the Dataproc master node."
  type        = string
  default     = "e2-standard-4" # 4 vCPUs, 16 GB RAM
}

variable "worker_machine_type" {
  description = "Machine type for the Dataproc worker nodes."
  type        = string
  default     = "n2-standard-4" # 4 vCPUs, 16 GB RAM
}

variable "num_primary_workers" {
  description = "Number of primary (on-demand) worker nodes."
  type        = number
  default     = 2
}

variable "num_secondary_workers" {
  description = "Number of secondary (Spot / Preemptible) worker nodes for high-throughput, low-cost bursting."
  type        = number
  default     = 4
}

