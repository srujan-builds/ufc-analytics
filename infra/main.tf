terraform {
  required_providers {
    snowflake = {
      source = "Snowflake-Labs/snowflake"
    }
  }
}


provider "snowflake" {
    organization_name = var.organization_name
    account_name      = var.account_name
    user              = "TERRAFORM_SVC"
    role              = "SYSADMIN"
    authenticator     = "SNOWFLAKE_JWT"
    private_key       = file(var.private_key_path)
    preview_features_enabled = [ "snowflake_stage_resource" , "snowflake_file_format_resource"]
}

resource "snowflake_database" "ufc_db" {
    name = var.database
}

resource "snowflake_warehouse" "ufc_wh" {
    name = var.warehouse
    warehouse_type = "STANDARD"
    warehouse_size = "XSMALL"
    auto_resume = true
    auto_suspend = 60
}

resource "snowflake_schema" "ufc_schemas" {
    for_each = var.schema_names
    database = snowflake_database.ufc_db.name
    name=each.value
}

resource "snowflake_stage" "ufc_stage" {
    name = var.internal_stage
    database = snowflake_database.ufc_db.name
    schema = snowflake_schema.ufc_schemas["RAW"].name
}

resource "snowflake_file_format" "file_format" {
    name = var.csv_file_format
    database = snowflake_database.ufc_db.name
    schema = snowflake_schema.ufc_schemas["RAW"].name
    format_type = "CSV"
    field_delimiter = ","
    skip_header = 1
    field_optionally_enclosed_by = "\""
}