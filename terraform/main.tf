terraform {
  required_version = ">= 1.3.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 5.0.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

locals {
  bucket_name = var.bucket_name != "" ? var.bucket_name : "${var.project_id}-dataproc-poc-${var.region}"
  common_labels = {
    workload    = "pyspark-poc"
    environment = "benchmark"
    cost-center = "data-platform"
  }
}

# ---------------------------------------------------------------------------------
# 1. Google Cloud Storage Bucket (Data, Scripts, and Temp Staging)
# ---------------------------------------------------------------------------------
resource "google_storage_bucket" "poc_bucket" {
  name                        = local.bucket_name
  location                    = var.region
  force_destroy               = true
  uniform_bucket_level_access = true

  versioning {
    enabled = false
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 14 # Auto-clean benchmark temp files after 14 days
    }
  }

  labels = local.common_labels
}

# Upload PySpark pipeline script directly into the bucket
resource "google_storage_bucket_object" "pyspark_script" {
  name   = "scripts/snowflake_dataproc_pipeline.py"
  bucket = google_storage_bucket.poc_bucket.name
  source = "${path.module}/../pyspark/snowflake_dataproc_pipeline.py"
}

# ---------------------------------------------------------------------------------
# 2. BigQuery Dataset (Target for Analytical Benchmark Output)
# ---------------------------------------------------------------------------------
resource "google_bigquery_dataset" "benchmark_dataset" {
  dataset_id                  = var.bigquery_dataset_id
  friendly_name               = "PySpark vs Snowflake Benchmark Dataset"
  description                 = "Destination dataset for comparing Dataproc PySpark vs Snowflake compute"
  location                    = var.region
  default_table_expiration_ms = 1209600000 # 14 days expiration for POC tables

  labels = local.common_labels
}

# ---------------------------------------------------------------------------------
# 3. Least-Privilege IAM Service Account for Dataproc Cluster
# ---------------------------------------------------------------------------------
resource "google_service_account" "dataproc_sa" {
  account_id   = "dataproc-poc-runner"
  display_name = "Dataproc POC Benchmark Runner Service Account"
}

resource "google_project_iam_member" "dataproc_worker" {
  project = var.project_id
  role    = "roles/dataproc.worker"
  member  = "serviceAccount:${google_service_account.dataproc_sa.email}"
}

resource "google_storage_bucket_iam_member" "bucket_admin" {
  bucket = google_storage_bucket.poc_bucket.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.dataproc_sa.email}"
}

resource "google_project_iam_member" "bq_editor" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.dataproc_sa.email}"
}

resource "google_project_iam_member" "bq_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.dataproc_sa.email}"
}

# ---------------------------------------------------------------------------------
# 4. Dataproc Autoscaling Policy (Optimized for Cost with Spot/Preemptibles)
# ---------------------------------------------------------------------------------
resource "google_dataproc_autoscaling_policy" "cost_optimized_policy" {
  policy_id = "dataproc-poc-autoscaler"
  location  = var.region

  worker_config {
    min_instances = 2
    max_instances = 4
    weight        = 1
  }

  secondary_worker_config {
    min_instances = 0
    max_instances = 16 # Burst compute capacity up to 16 Spot/Preemptible workers
    weight        = 2
  }

  basic_algorithm {
    yarn_config {
      scale_up_factor   = 0.5
      scale_down_factor = 1.0
      graceful_decommission_timeout = "60s"
    }
    cooldown_period = "120s"
  }
}

# ---------------------------------------------------------------------------------
# 5. Cost-Optimized Cloud Dataproc Cluster
# ---------------------------------------------------------------------------------
resource "google_dataproc_cluster" "benchmark_cluster" {
  name     = var.cluster_name
  region   = var.region
  labels   = local.common_labels

  cluster_config {
    staging_bucket = google_storage_bucket.poc_bucket.name

    # Master node configuration
    master_config {
      num_instances = 1
      machine_type  = var.master_machine_type
      disk_config {
        boot_disk_type    = "pd-balanced"
        boot_disk_size_gb = 50
      }
    }

    # Primary worker configuration (reliable baseline compute)
    worker_config {
      num_instances = var.num_primary_workers
      machine_type  = var.worker_machine_type
      disk_config {
        boot_disk_type    = "pd-balanced"
        boot_disk_size_gb = 100
      }
    }

    # Secondary worker configuration (Spot / Preemptible VMs: 60-80% discount!)
    preemptible_worker_config {
      num_instances  = var.num_secondary_workers
      preemptibility = "SPOT"
      disk_config {
        boot_disk_type    = "pd-balanced"
        boot_disk_size_gb = 100
      }
    }

    software_config {
      image_version = "2.2-debian12"
      override_properties = {
        # Optimize Spark YARN memory allocation and dynamic allocation
        "spark:spark.dynamicAllocation.enabled" = "true"
        "spark:spark.shuffle.service.enabled"   = "true"
        "spark:spark.sql.adaptive.enabled"      = "true"
      }
    }

    gce_cluster_config {
      service_account        = google_service_account.dataproc_sa.email
      service_account_scopes = ["https://www.googleapis.com/auth/cloud-platform"]
      internal_ip_only       = false
    }

    autoscaling_config {
      policy_uri = google_dataproc_autoscaling_policy.cost_optimized_policy.name
    }

    # Auto-deletion / idle shutdown safeguards so POC never over-spends
    lifecycle_config {
      idle_delete_ttl  = "1800s" # Auto-deletes cluster after 30 minutes of inactivity
      auto_delete_time = null
    }
  }

  depends_on = [
    google_project_iam_member.dataproc_worker,
    google_storage_bucket_iam_member.bucket_admin
  ]
}

