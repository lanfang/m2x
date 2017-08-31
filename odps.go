package main

import (
	"fmt"
	"github.com/qjpcpu/schemalex/model"
	"strings"
)

func tableToOdpsSql(table model.Table) string {
	sql := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s(\n", table.Name())
	opts := table.Options()
	cols := table.Columns()
	for col := range cols {
		if col.Comment() != "" {
			sql += fmt.Sprintf("  %s %s COMMENT '%s',\n", col.Name(), colunmTypeToOdpsStringType(col.Type()), col.Comment())
		} else {
			sql += fmt.Sprintf("  %s %s,\n", col.Name(), colunmTypeToOdpsStringType(col.Type()))
		}
	}
	sql = strings.TrimSuffix(sql, ",\n") + ")\n"
	for opt := range opts {
		if opt.Key() == "COMMENT" && opt.Value() != "" {
			sql += fmt.Sprintf("COMMENT '%s'\n", opt.Value())
		}
	}
	sql += "PARTITIONED BY (date_flag STRING COMMENT '日期分区,格式为yyyy-mm-dd')\n"
	sql = strings.TrimSuffix(sql, "\n") + ";\n"
	return sql
}

func colunmTypeToOdpsStringType(c model.ColumnType) string {
	switch c {
	case model.ColumnTypeBit:
		return "BIGINT"
	case model.ColumnTypeTinyInt:
		return "BIGINT"
	case model.ColumnTypeSmallInt:
		return "BIGINT"
	case model.ColumnTypeMediumInt:
		return "BIGINT"
	case model.ColumnTypeInt:
		return "BIGINT"
	case model.ColumnTypeInteger:
		return "BIGINT"
	case model.ColumnTypeBigInt:
		return "BIGINT"
	case model.ColumnTypeReal:
		return "DOUBLE"
	case model.ColumnTypeDouble:
		return "DOUBLE"
	case model.ColumnTypeFloat:
		return "DOUBLE"
	case model.ColumnTypeDecimal:
		return "DOUBLE"
	case model.ColumnTypeNumeric:
		return "BIGINT"
	case model.ColumnTypeDate:
		return "DATETIME"
	case model.ColumnTypeTime:
		return "DATETIME"
	case model.ColumnTypeTimestamp:
		return "DATETIME"
	case model.ColumnTypeDateTime:
		return "DATETIME"
	case model.ColumnTypeYear:
		return "STRING"
	case model.ColumnTypeChar:
		return "STRING"
	case model.ColumnTypeVarChar:
		return "STRING"
	case model.ColumnTypeBinary:
		return "STRING"
	case model.ColumnTypeVarBinary:
		return "STRING"
	case model.ColumnTypeTinyBlob:
		return "STRING"
	case model.ColumnTypeBlob:
		return "STRING"
	case model.ColumnTypeMediumBlob:
		return "STRING"
	case model.ColumnTypeLongBlob:
		return "STRING"
	case model.ColumnTypeTinyText:
		return "STRING"
	case model.ColumnTypeText:
		return "STRING"
	case model.ColumnTypeMediumText:
		return "STRING"
	case model.ColumnTypeLongText:
		return "STRING"
	default:
		return "STRING"
	}
}
