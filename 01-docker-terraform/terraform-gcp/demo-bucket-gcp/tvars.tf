variable "project" {
  description = "Project Name"
  default     = "terraform-demo-418822"

}

variable "location" {
  description = "Project location"
  default     = "US"

}

variable "region" {
  description = "Project region"
  default     = "us-central1"

}

variable "bq_dataset_name" {
  description = "BigQuery Dataset Name"
  default     = "demo_dataset"

}

variable "gcs_bucket_name" {
  description = "Storage Bucket Name"
  default     = "lolcay-demo-bucket"

}

variable "gcd_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"

}