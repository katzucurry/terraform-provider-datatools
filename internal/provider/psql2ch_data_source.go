// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package provider

import (
	"context"
	"fmt"
	"strings"

	"github.com/hashicorp/terraform-plugin-framework/datasource"
	"github.com/hashicorp/terraform-plugin-framework/datasource/schema"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/hashicorp/terraform-plugin-log/tflog"
)

// Ensure provider defined types fully satisfy framework interfaces.
var _ datasource.DataSource = &Psql2ChDataSource{}

func NewPsql2ChDataSource() datasource.DataSource {
	return &Psql2ChDataSource{}
}

// Psql2ChDataSource defines the data source implementation.
type Psql2ChDataSource struct {
}

// Psql2ChDataSourceModel describes the data source data model.
type Psql2ChDataSourceModel struct {
	Id                          types.String       `tfsdk:"id"`
	PostgresColumns             []PsqlColumn       `tfsdk:"postgres_columns"`
	ClickhousePrimaryKey        types.String       `tfsdk:"clickhouse_primarykey"`
	ClickhouseGuessedPrimaryKey types.String       `tfsdk:"clickhouse_guessed_primarykey"`
	ClickhouseColumns           []ClickhouseColumn `tfsdk:"clickhouse_columns"`
}

type PsqlColumn struct {
	Name                   types.String `tfsdk:"name"`
	Type                   types.String `tfsdk:"type"`
	IsPrimaryKey           types.Bool   `tfsdk:"is_primary_key"`
	NumericPrecision       types.Int64  `tfsdk:"numeric_precision"`
	NumericScale           types.Int64  `tfsdk:"numeric_scale"`
	CharacterMaximumLength types.Int64  `tfsdk:"character_maximum_length"`
	DatetimePrecicion      types.Int64  `tfsdk:"datetime_precision"`
	IsNullable             types.Bool   `tfsdk:"is_nullable"`
}

type ClickhouseColumn struct {
	Name types.String `tfsdk:"name"`
	Type types.String `tfsdk:"type"`
}

func (d *Psql2ChDataSource) Metadata(ctx context.Context, req datasource.MetadataRequest, resp *datasource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_psql2ch"
}

func (d *Psql2ChDataSource) Schema(ctx context.Context, req datasource.SchemaRequest, resp *datasource.SchemaResponse) {
	resp.Schema = schema.Schema{
		// This description is used by the documentation generator and the language server.
		MarkdownDescription: "Example data source",

		Attributes: map[string]schema.Attribute{
			"id": schema.StringAttribute{
				MarkdownDescription: "PostgreSQL to Clickhouse converter identifier",
				Computed:            true,
			},
			"postgres_columns": schema.ListNestedAttribute{
				MarkdownDescription: "PostgreSQL to Clickhouse source PostgreSQL DDL schema",
				Required:            true,
				NestedObject: schema.NestedAttributeObject{
					Attributes: map[string]schema.Attribute{
						"name": schema.StringAttribute{
							MarkdownDescription: "PostgreSQL Column name",
							Required:            true,
						},
						"type": schema.StringAttribute{
							MarkdownDescription: "PostgreSQL Column type",
							Required:            true,
						},
						"is_primary_key": schema.BoolAttribute{
							MarkdownDescription: "PostgreSQL is primary key boolean",
							Required:            true,
						},
						"numeric_precision": schema.Int64Attribute{
							MarkdownDescription: "PostgreSQL numeric precision when apply",
							Optional:            true,
						},
						"numeric_scale": schema.Int64Attribute{
							MarkdownDescription: "PostgreSQL numeric scale when apply",
							Optional:            true,
						},
						"character_maximum_length": schema.Int64Attribute{
							MarkdownDescription: "PostgreSQL character length when apply",
							Optional:            true,
						},
						"datetime_precision": schema.Int64Attribute{
							MarkdownDescription: "Precison for timestamp",
							Optional:            true,
						},
						"is_nullable": schema.BoolAttribute{
							MarkdownDescription: "True if the column is nullable",
							Required:            true,
						},
					},
				},
			},
			"clickhouse_primarykey": schema.StringAttribute{
				MarkdownDescription: "PostgreSQL column identify as primary key",
				Computed:            true,
			},
			"clickhouse_guessed_primarykey": schema.StringAttribute{
				MarkdownDescription: "PostgreSQL column guessed as primary key",
				Computed:            true,
			},
			"clickhouse_columns": schema.ListNestedAttribute{
				MarkdownDescription: "PostgreSQL columns converted to Clickhouse columns",
				Computed:            true,
				NestedObject: schema.NestedAttributeObject{
					Attributes: map[string]schema.Attribute{
						"name": schema.StringAttribute{
							MarkdownDescription: "Columns name, same as PostgreSQL",
							Computed:            true,
						},
						"type": schema.StringAttribute{
							MarkdownDescription: "PostgreSQL column type converted to Clickhouse type",
							Computed:            true,
						},
					},
				},
			},
		},
	}
}

func (d *Psql2ChDataSource) Configure(ctx context.Context, req datasource.ConfigureRequest, resp *datasource.ConfigureResponse) {
	// Prevent panic if the provider has not been configured.
	if req.ProviderData == nil {
		return
	}
}

func (d *Psql2ChDataSource) Read(ctx context.Context, req datasource.ReadRequest, resp *datasource.ReadResponse) {
	var data Psql2ChDataSourceModel

	// Read Terraform configuration data into the model
	resp.Diagnostics.Append(req.Config.Get(ctx, &data)...)

	if resp.Diagnostics.HasError() {
		return
	}

	var columnNames []string
	var clickhouseColumns []ClickhouseColumn
	var primaryKey types.String
	var guessedPrimaryKey *types.String
	isGuessedPrimaryKey := false
	for _, column := range data.PostgresColumns {
		columnName := column.Name
		columnNames = append(columnNames, columnName.ValueString())
		if column.IsPrimaryKey.ValueBool() {
			primaryKey = columnName
		}
		if strings.HasSuffix(columnName.ValueString(), "_id") && guessedPrimaryKey == nil {
			guessedPrimaryKey = &columnName
			isGuessedPrimaryKey = true
		}
		clickhouseColumns = append(clickhouseColumns, ClickhouseColumn{
			Name: columnName,
			Type: types.StringValue(postgreSqlToClickhouseType(
				column.Type.ValueString(),
				column.NumericPrecision.ValueInt64(),
				column.NumericScale.ValueInt64(),
				column.DatetimePrecicion.ValueInt64(),
				column.IsNullable.ValueBool(),
				column.IsPrimaryKey.ValueBool(),
				isGuessedPrimaryKey,
			)),
		})
	}
	data.Id = types.StringValue(strings.Join(columnNames, "_"))
	data.ClickhousePrimaryKey = primaryKey
	if guessedPrimaryKey != nil {
		data.ClickhouseGuessedPrimaryKey = *guessedPrimaryKey
	}
	data.ClickhouseColumns = clickhouseColumns

	// Write logs using the tflog package
	// Documentation: https://terraform.io/plugin/log
	tflog.Trace(ctx, "read a data source")

	// Save data into Terraform state
	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
}

func postgreSqlToClickhouseType(psqlType string, numericPrecision int64, numericScale int64, datetimePrecicion int64, isNullable bool, isPrimaryKey bool, isGuessedPrimaryKey bool) string {
	clickhouseType := ""
	switch psqlType {
	case "int4", "int8":
		clickhouseType = "Int"
	case "numeric":
		if numericPrecision == 0 {
			numericPrecision = 76
		}
		clickhouseType = fmt.Sprintf("Decimal(%d, %d)", numericPrecision, numericScale)
	case "varchar", "text", "bpchar":
		clickhouseType = "String"
	case "timestamp", "timestamptz":
		clickhouseType = fmt.Sprintf("DateTime64(%d)", datetimePrecicion)
	case "date":
		clickhouseType = "Date"
	case "float4":
		clickhouseType = "Float32"
	case "float8":
		clickhouseType = "Float64"
	case "bool":
		clickhouseType = "Bool"
	default:
		clickhouseType = "NotImplementedType!"
	}
	if isNullable && (!isPrimaryKey || !isGuessedPrimaryKey) {
		clickhouseType = "Nullable(" + clickhouseType + ")"
	}
	return clickhouseType
}
