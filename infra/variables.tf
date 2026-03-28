variable "organization_name" {
    description = "organization name"
    type = string
}

variable "account_name" {
    description = "account name"
    type = string
}

variable "private_key_path" {
    description = "Path to ssh key"
    type = string
}

variable "database" {
    description = "DB for UFC Analytics project"
    default = "UFC_ANALYTICS"
}

variable "warehouse" {
    description = "Warehouse for UFC Analytics project"
    default = "UFC_WH"
}

variable "schema_names" {
    type = set(string)
    description = "List of schemas to create in the UFC database"
    default = [ "RAW", "STG", "INT", "MARTS" ]
}

variable "internal_stage" {
    description = "An internal stage for uploading static files"
    default = "UFC_STAGE"
}