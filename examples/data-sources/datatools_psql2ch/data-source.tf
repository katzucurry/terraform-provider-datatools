data "datatools_psql2ch" "example" {
  from = [{
    name                     = "key_id"
    type                     = "int4"
    character_maximum_length = 0
    is_primary_key           = true
    numeric_precision        = 32
    numeric_scale            = 0
  }]
}

output "test" {
  value = data.datatools_psql2ch.example.to
}