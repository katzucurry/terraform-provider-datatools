// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package provider

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/hashicorp/terraform-plugin-framework/attr"
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
	Id                                  types.String       `tfsdk:"id"`
	PostgresColumns                     []PsqlColumn       `tfsdk:"postgres_columns"`
	ClickhousePrimaryKey                []types.String     `tfsdk:"clickhouse_primarykey"`
	ClickhouseGuessedPrimaryKey         []types.String     `tfsdk:"clickhouse_guessed_primarykey"`
	ClickhouseColumns                   []ClickhouseColumn `tfsdk:"clickhouse_columns"`
	ClickhouseKafkaEngineColumns        []ClickhouseColumn `tfsdk:"clickhouse_kafkaengine_columns"`
	ClickhouseKafkaEngineColumnsMapping types.List         `tfsdk:"clickhouse_kafkaengine_columns_mapping"`
	AthenaColumns                       []AthenaColumn     `tfsdk:"athena_columns"`
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

type AthenaColumn struct {
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
			"clickhouse_primarykey": schema.ListAttribute{
				ElementType:         types.StringType,
				MarkdownDescription: "PostgreSQL columns list identify the primary key",
				Computed:            true,
			},
			"clickhouse_guessed_primarykey": schema.ListAttribute{
				ElementType:         types.StringType,
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
			"clickhouse_kafkaengine_columns": schema.ListNestedAttribute{
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
			"clickhouse_kafkaengine_columns_mapping": schema.ListAttribute{
				MarkdownDescription: "Mapping between kafka engine with avroconfluent format to clickhouse base types",
				Computed:            true,
				ElementType:         types.StringType,
			},
			"athena_columns": schema.ListNestedAttribute{
				MarkdownDescription: "Clickhouse to Athena PostgreSQL DDL schema",
				Computed:            true,
				NestedObject: schema.NestedAttributeObject{
					Attributes: map[string]schema.Attribute{
						"name": schema.StringAttribute{
							MarkdownDescription: "Athena Column name",
							Required:            true,
						},
						"type": schema.StringAttribute{
							MarkdownDescription: "Athena column type",
							Optional:            true,
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
	var clickhouseKafkaEngineColumns []ClickhouseColumn
	var clickhouseKafkaEngineColumnsMapping []attr.Value
	var primaryKey []types.String
	var guessedPrimaryKey *types.String
	for _, column := range data.PostgresColumns {
		isGuessedPrimaryKey := false
		columnName := column.Name
		columnNames = append(columnNames, columnName.ValueString())
		if column.IsPrimaryKey.ValueBool() {
			primaryKey = append(primaryKey, columnName)
		}
		if strings.HasSuffix(columnName.ValueString(), "_id") && guessedPrimaryKey == nil {
			guessedPrimaryKey = &columnName
			isGuessedPrimaryKey = true
		}
		err, clickhouseType := postgreSqlToClickhouseType(
			column.Type.ValueString(),
			column.NumericPrecision.ValueInt64(),
			column.NumericScale.ValueInt64(),
			column.DatetimePrecicion.ValueInt64(),
			column.IsNullable.ValueBool(),
			column.IsPrimaryKey.ValueBool(),
			isGuessedPrimaryKey,
		)
		if err != nil {
			resp.Diagnostics.AddError(
				"Unable to map PostgreSQL type",
				"An unexpected error occurred when mapping type: "+err.Error(),
			)
			return
		}
		clickhouseColumns = append(clickhouseColumns, ClickhouseColumn{
			Name: columnName,
			Type: types.StringValue(clickhouseType),
		})
		clickhouseKafkaEngineColumns = append(clickhouseKafkaEngineColumns, ClickhouseColumn{
			Name: columnName,
			Type: types.StringValue(postgreSqlToKafkaEngineClickhouseType(
				column.Type.ValueString(),
				column.DatetimePrecicion.ValueInt64(),
				column.IsNullable.ValueBool(),
				column.IsPrimaryKey.ValueBool(),
				isGuessedPrimaryKey,
			)),
		})
		clickhouseKafkaEngineColumnsMapping = append(clickhouseKafkaEngineColumnsMapping, mappingKafkaEngineTypes(columnName.ValueString(), column.Type.ValueString()))
	}
	var athenaColumns []AthenaColumn
	for _, column := range clickhouseColumns {
		athenaColumns = append(athenaColumns, AthenaColumn{
			Name: types.StringValue(column.Name.ValueString()),
			Type: types.StringValue(clickhouseToAthena(column.Type.ValueString())),
		})
	}
	data.Id = types.StringValue(strings.Join(columnNames, "_"))
	data.ClickhousePrimaryKey = primaryKey
	if guessedPrimaryKey != nil {
		data.ClickhouseGuessedPrimaryKey = []types.String{*guessedPrimaryKey}
	}
	data.ClickhouseColumns = clickhouseColumns
	data.ClickhouseKafkaEngineColumns = clickhouseKafkaEngineColumns
	clickhouseKafkaEngineColumnsMappingValues, diags := types.ListValue(types.StringType, clickhouseKafkaEngineColumnsMapping)
	if diags.HasError() {
		return
	}
	data.ClickhouseKafkaEngineColumnsMapping = clickhouseKafkaEngineColumnsMappingValues
	data.AthenaColumns = athenaColumns
	// Write logs using the tflog package
	// Documentation: https://terraform.io/plugin/log
	tflog.Trace(ctx, "read a data source")

	// Save data into Terraform state
	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
}

type NotImplementedType struct {
	PSQLType string
}

func (e *NotImplementedType) Error() string {
	return fmt.Sprintf("Type %s not implemented yet", e.PSQLType)
}

func postgreSqlToClickhouseType(psqlType string, numericPrecision int64, numericScale int64, datetimePrecicion int64, isNullable bool, isPrimaryKey bool, isGuessedPrimaryKey bool) (error, string) {
	var err error
	var clickhouseType string
	switch psqlType {
	case "int2":
		clickhouseType = "Int16"
	case "int4":
		clickhouseType = "Int32"
	case "int8":
		clickhouseType = "Int64"
	case "numeric":
		if numericPrecision == 0 {
			numericPrecision = 38
			numericScale = 19
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
		err = &NotImplementedType{
			PSQLType: psqlType,
		}
		return err, clickhouseType
	}
	if isNullable && !isPrimaryKey && !isGuessedPrimaryKey {
		clickhouseType = "Nullable(" + clickhouseType + ")"
	}
	return err, clickhouseType
}

func postgreSqlToKafkaEngineClickhouseType(psqlType string, datetimePrecicion int64, isNullable bool, isPrimaryKey bool, isGuessedPrimaryKey bool) string {
	var clickhouseType string
	switch psqlType {
	case "int2":
		clickhouseType = "Int16"
	case "int4":
		clickhouseType = "Int32"
	case "int8":
		clickhouseType = "Int64"
	case "numeric":
		clickhouseType = "String"
	case "varchar", "text", "bpchar":
		clickhouseType = "String"
	case "timestamp":
		clickhouseType = fmt.Sprintf("DateTime64(%d)", datetimePrecicion)
	case "timestamptz":
		clickhouseType = "String"
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
	if isNullable && !isPrimaryKey && !isGuessedPrimaryKey {
		clickhouseType = "Nullable(" + clickhouseType + ")"
	}
	return clickhouseType
}

func mappingKafkaEngineTypes(name string, psqlType string) types.String {
	var expression string
	switch psqlType {
	case "timestamptz":
		expression = "parseDateTime64BestEffortOrNull(`" + name + "`) as `" + name + "`"
	default:
		expression = "`" + name + "`"
	}
	return types.StringValue(expression)
}

func clickhouseToAthena(clichouseType string) string {
	nullable := regexp.MustCompile(`Nullable\((?P<Type>.+)\)`)
	if nullable.MatchString(clichouseType) {
		matches := nullable.FindStringSubmatch(clichouseType)
		clichouseType = matches[nullable.SubexpIndex("Type")]
	}
	decimalPS := regexp.MustCompile(`Decimal\((?P<Precision>\d+), (?P<Scale>\d+)\)`)
	decimalP := regexp.MustCompile(`Decimal\((?P<Precision>\d+)\)`)
	var athenaType string
	switch {
	case clichouseType == "Int16":
		athenaType = "int"
	case clichouseType == "Int32":
		athenaType = "int"
	case clichouseType == "Int64":
		athenaType = "int"
	case clichouseType == "String":
		athenaType = "string"
	case decimalPS.MatchString(clichouseType):
		matches := decimalPS.FindStringSubmatch(clichouseType)
		precision := matches[decimalPS.SubexpIndex("Precision")]
		scale := matches[decimalPS.SubexpIndex("Scale")]
		athenaType = fmt.Sprintf("decimal(%s,%s)", precision, scale)
	case decimalP.MatchString(clichouseType):
		matches := decimalP.FindStringSubmatch(clichouseType)
		precision := matches[decimalP.SubexpIndex("Precision")]
		athenaType = fmt.Sprintf("decimal(%s)", precision)
	case regexp.MustCompile(`DateTime64\(\d+\)`).MatchString(clichouseType):
		athenaType = "timestamp"
	case clichouseType == "Date":
		athenaType = "date"
	case clichouseType == "Float32":
		athenaType = "float"
	case clichouseType == "Float64":
		athenaType = "float"
	case clichouseType == "Bool":
		athenaType = "boolean"
	default:
		athenaType = "NotImplementedType!"
	}
	return athenaType

}
