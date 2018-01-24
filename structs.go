package main

import (
	"fmt"
	"github.com/hoisie/mustache"
	"github.com/qjpcpu/schemalex/model"
	"github.com/urfave/cli"
	"strings"
)

const GoModTempl = `
type {{mod_name}} struct {
{{#fields}}
    {{field}}       {{type}}        {{{tag}}}  {{{comment}}}
{{/fields}}
}
`

func tableToGoMod(c *cli.Context, table model.Table) string {
	cols := table.Columns()
	fields := []interface{}{}
	for col := range cols {
		comment := col.Comment()
		if comment != "" {
			comment = "// " + comment
		}
		fields = append(fields, map[string]interface{}{
			"field":   camelCase(col.Name()),
			"type":    colunmTypeToGoModType(col.Type()),
			"tag":     fmt.Sprintf("`json:\"%s\"`", col.Name()),
			"comment": comment,
		})
	}
	data := map[string]interface{}{
		"mod_name": camelCase(table.Name()),
		"fields":   fields,
	}
	tmpl, _ := mustache.ParseString(GoModTempl)
	return tmpl.Render(data)
}

func camelCase(n string) string {
	arr := strings.Split(n, "_")
	for i, token := range arr {
		arr[i] = strings.ToUpper(string([]rune(token)[0])) + string([]rune(token)[1:])
	}
	return strings.Join(arr, "")
}

func colunmTypeToGoModType(c model.ColumnType) string {
	switch c {
	case model.ColumnTypeBit:
		return "int64"
	case model.ColumnTypeTinyInt:
		return "int64"
	case model.ColumnTypeSmallInt:
		return "int64"
	case model.ColumnTypeMediumInt:
		return "int64"
	case model.ColumnTypeInt:
		return "int64"
	case model.ColumnTypeInteger:
		return "int64"
	case model.ColumnTypeBigInt:
		return "int64"
	case model.ColumnTypeReal:
		return "float64"
	case model.ColumnTypeDouble:
		return "float64"
	case model.ColumnTypeFloat:
		return "float64"
	case model.ColumnTypeDecimal:
		return "float64"
	case model.ColumnTypeNumeric:
		return "int64"
	case model.ColumnTypeDate:
		return "time.Time"
	case model.ColumnTypeTime:
		return "time.Time"
	case model.ColumnTypeTimestamp:
		return "time.Time"
	case model.ColumnTypeDateTime:
		return "time.Time"
	case model.ColumnTypeYear:
		return "string"
	case model.ColumnTypeChar:
		return "string"
	case model.ColumnTypeVarChar:
		return "string"
	case model.ColumnTypeBinary:
		return "string"
	case model.ColumnTypeVarBinary:
		return "string"
	case model.ColumnTypeTinyBlob:
		return "string"
	case model.ColumnTypeBlob:
		return "string"
	case model.ColumnTypeMediumBlob:
		return "string"
	case model.ColumnTypeLongBlob:
		return "string"
	case model.ColumnTypeTinyText:
		return "string"
	case model.ColumnTypeText:
		return "string"
	case model.ColumnTypeMediumText:
		return "string"
	case model.ColumnTypeLongText:
		return "string"
	default:
		return "string"
	}
}
