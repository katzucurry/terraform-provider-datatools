// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package provider

import (
	"testing"

	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
)

func TestAccPsql2ChDataSource(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			// Case1
			{
				Config: testAccPsql2ChDataSourceConfigCase1,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("data.datatools_psql2ch.test", "clickhouse_primarykey.0", "key"),
					resource.TestCheckResourceAttr("data.datatools_psql2ch.test", "clickhouse_columns.0.name", "key"),
					resource.TestCheckResourceAttr("data.datatools_psql2ch.test", "clickhouse_columns.0.type", "Int32"),
				),
			},
			// Case2
			{
				Config: testAccPsql2ChDataSourceConfigCase2,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("data.datatools_psql2ch.test", "clickhouse_primarykey.0", "key_id"),
					resource.TestCheckResourceAttr("data.datatools_psql2ch.test", "clickhouse_columns.0.name", "key_id"),
					resource.TestCheckResourceAttr("data.datatools_psql2ch.test", "clickhouse_columns.0.type", "Int32"),
				),
			},
			// Case3
			{
				Config: testAccPsql2ChDataSourceConfigCase3,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("data.datatools_psql2ch.test", "clickhouse_primarykey.0", "key_id"),
					resource.TestCheckResourceAttr("data.datatools_psql2ch.test", "clickhouse_primarykey.1", "key2_id"),
					resource.TestCheckResourceAttr("data.datatools_psql2ch.test", "clickhouse_columns.0.name", "key_id"),
					resource.TestCheckResourceAttr("data.datatools_psql2ch.test", "clickhouse_columns.0.type", "Int32"),
				),
			},
			// Bug in Dev with table in shop database product_history_change
			{
				Config: testAccBugDevProductHistoryChangeTable,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("data.datatools_psql2ch.test", "clickhouse_guessed_primarykey.0", "phc_id"),
					resource.TestCheckResourceAttr("data.datatools_psql2ch.test", "clickhouse_columns.0.name", "phc_id"),
					resource.TestCheckResourceAttr("data.datatools_psql2ch.test", "clickhouse_columns.0.type", "Int64"),
					resource.TestCheckResourceAttr("data.datatools_psql2ch.test", "clickhouse_columns.1.type", "Nullable(Int64)"),
				),
			},
			{
				Config: testAccPsql2ChDataSourceConfigCaseClickhouseToAthena1,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("data.datatools_psql2ch.test", "athena_columns.0.name", "key_id"),
					resource.TestCheckResourceAttr("data.datatools_psql2ch.test", "athena_columns.0.type", "int"),
					resource.TestCheckResourceAttr("data.datatools_psql2ch.test", "athena_columns.1.name", "key2_id"),
					resource.TestCheckResourceAttr("data.datatools_psql2ch.test", "athena_columns.1.type", "int"),
					resource.TestCheckResourceAttr("data.datatools_psql2ch.test", "athena_columns.2.name", "key3_id"),
					resource.TestCheckResourceAttr("data.datatools_psql2ch.test", "athena_columns.2.type", "decimal(32,0)"),
					resource.TestCheckResourceAttr("data.datatools_psql2ch.test", "athena_columns.3.name", "key_date"),
					resource.TestCheckResourceAttr("data.datatools_psql2ch.test", "athena_columns.3.type", "timestamp"),
				),
			},
		},
	})
}

const testAccPsql2ChDataSourceConfigCase1 = `
data "datatools_psql2ch" "test" {
  postgres_columns = [{
	name                     = "key"
	type                     = "int4"  
	character_maximum_length = 0
	is_primary_key           = true
	numeric_precision        = 32
	numeric_scale            = 0
	datetime_precision       = 0
	is_nullable 			 = false
  }]
}
`
const testAccPsql2ChDataSourceConfigCase2 = `
data "datatools_psql2ch" "test" {
  postgres_columns = [{
	name                     = "key_id"
	type                     = "int4"  
	character_maximum_length = 0
	is_primary_key           = true
	numeric_precision        = 32
	numeric_scale            = 0
	datetime_precision       = 0
	is_nullable 			 = true
  }]
}
`
const testAccPsql2ChDataSourceConfigCase3 = `
data "datatools_psql2ch" "test" {
  postgres_columns = [{
	name                     = "key_id"
	type                     = "int4"  
	character_maximum_length = 0
	is_primary_key           = true
	numeric_precision        = 32
	numeric_scale            = 0
	datetime_precision       = 0
	is_nullable 			 = false
  }, {
	name                     = "key2_id"
	type                     = "int4"  
	character_maximum_length = 0
	is_primary_key           = true
	numeric_precision        = 32
	numeric_scale            = 0
	datetime_precision       = 0
	is_nullable 			 = false
  }]
}
`

const testAccBugDevProductHistoryChangeTable = `
data "datatools_psql2ch" "test" {
	postgres_columns = [{
		name                     = "phc_id"
		type                     = "int8"  
		character_maximum_length = 0
		is_primary_key           = false
		numeric_precision        = 64
		numeric_scale            = 0
		datetime_precision       = 0
		is_nullable 			 = true
	  },{
		name                     = "phc_u_id"
		type                     = "int8"  
		character_maximum_length = 0
		is_primary_key           = false
		numeric_precision        = 64
		numeric_scale            = 0
		datetime_precision       = 0
		is_nullable 			 = true
	},{
		name                     = "phc_ps_id"
		type                     = "int8"  
		character_maximum_length = 0
		is_primary_key           = false
		numeric_precision        = 64
		numeric_scale            = 0
		datetime_precision       = 0
		is_nullable 			 = true
	},{
		name                     = "phc_as_id"
		type                     = "int8"  
		character_maximum_length = 0
		is_primary_key           = false
		numeric_precision        = 64
		numeric_scale            = 0
		datetime_precision       = 0
		is_nullable 			 = true
	},{
		name                     = "phc_date"
		type                     = "timestamp"  
		character_maximum_length = 0
		is_primary_key           = false
		numeric_precision        = 0
		numeric_scale            = 0
		datetime_precision       = 6
		is_nullable 			 = true
	},{
		name                     = "phc_column"
		type                     = "varchar"  
		character_maximum_length = 0
		is_primary_key           = false
		numeric_precision        = 0
		numeric_scale            = 0
		datetime_precision       = 255
		is_nullable 			 = true
	},{
		name                     = "phc_old_value"
		type                     = "varchar"  
		character_maximum_length = 0
		is_primary_key           = false
		numeric_precision        = 0
		numeric_scale            = 0
		datetime_precision       = 255
		is_nullable 			 = true
	},{
		name                     = "phc_new_value"
		type                     = "varchar"  
		character_maximum_length = 0
		is_primary_key           = false
		numeric_precision        = 0
		numeric_scale            = 0
		datetime_precision       = 255
		is_nullable 			 = true
	},{
		name                     = "phc_old_clob_value"
		type                     = "text"  
		character_maximum_length = 0
		is_primary_key           = false
		numeric_precision        = 0
		numeric_scale            = 0
		datetime_precision       = 0
		is_nullable 			 = true
	},{
		name                     = "phc_new_clob_value"
		type                     = "text"  
		character_maximum_length = 0
		is_primary_key           = false
		numeric_precision        = 0
		numeric_scale            = 0
		datetime_precision       = 0
		is_nullable 			 = true
	},{
		name                     = "phc_description"
		type                     = "varchar"  
		character_maximum_length = 0
		is_primary_key           = false
		numeric_precision        = 0
		numeric_scale            = 0
		datetime_precision       = 255
		is_nullable 			 = true
	},{
		name                     = "phc_foreign_key_name"
		type                     = "varchar"  
		character_maximum_length = 0
		is_primary_key           = false
		numeric_precision        = 0
		numeric_scale            = 0
		datetime_precision       = 255
		is_nullable 			 = true
	},{
		name                     = "phc_foreign_key_value"
		type                     = "int8"  
		character_maximum_length = 0
		is_primary_key           = false
		numeric_precision        = 64
		numeric_scale            = 0
		datetime_precision       = 0
		is_nullable 			 = true
	},{
		name                     = "phc_sys_created_on"
		type                     = "timestamp"  
		character_maximum_length = 0
		is_primary_key           = false
		numeric_precision        = 0
		numeric_scale            = 0
		datetime_precision       = 6
		is_nullable 			 = true
	}]
  }
  
`
const testAccPsql2ChDataSourceConfigCaseClickhouseToAthena1 = `
data "datatools_psql2ch" "test" {
	postgres_columns = [{
		name                     = "key_id"
		type                     = "int4"  
		character_maximum_length = 0
		is_primary_key           = true
		numeric_precision        = 32
		numeric_scale            = 0
		datetime_precision       = 0
		is_nullable 			 = false
	  }, {
		name                     = "key2_id"
		type                     = "int4"  
		character_maximum_length = 0
		is_primary_key           = true
		numeric_precision        = 32
		numeric_scale            = 0
		datetime_precision       = 0
		is_nullable 			 = false
	  },
	  {
		name                     = "key3_id"
		type                     = "numeric"  
		character_maximum_length = 0
		is_primary_key           = true
		numeric_precision        = 32
		numeric_scale            = 0
		datetime_precision       = 0
		is_nullable 			 = false
	  },
	  
	  {
		name                     = "key_date"
		type                     = "timestamp"  
		character_maximum_length = 0
		is_primary_key           = true
		numeric_precision        = 32
		numeric_scale            = 0
		datetime_precision       = 0
		is_nullable 			 = false
	  }
	  ]
}
`
