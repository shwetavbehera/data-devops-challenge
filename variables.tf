variable "aws_region" {
  default = "eu-central-1"
}

variable "source_bucket" {
  default = "data-devops-challenge-source-data"
}

variable "destination_bucket" {
  default = "data-devops-challenge-destination-data"
}

variable "code_bucket" {
  default = "data-devops-challenge-code-shwetav123"
}

variable "glue_database" {
  default = "devops_challenge_db"
}

variable "crawler_name" {
  default = "devops-crawler"
}

variable "job_name" {
  default = "devops-etl-job"
}
