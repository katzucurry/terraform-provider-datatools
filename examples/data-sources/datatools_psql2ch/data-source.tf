data "datatools_psql2ch" "example" {
  postgres_columns = [{
    name                     = "key_id"
    type                     = "int4"
    character_maximum_length = 0
    is_primary_key           = true
    numeric_precision        = 32
    numeric_scale            = 0
  }]
}

output "ch_pk" {
  value = data.datatools_psql2ch.example.clickhouse_primarykey
}

output "ch_columns" {
  value = data.datatools_psql2ch.example.clickhouse_columns
}