package main

import (
	"bytes"
	"encoding/json"
	"github.com/hoisie/mustache"
	"github.com/qjpcpu/schemalex/model"
	"github.com/urfave/cli"
)

const Templ = `{
    "job": {
        "content": [
            {
                "reader": {
                    "name": "mysqlreader",
                    "parameter": {
                        "username": "{{user}}",
                        "password": "{{password}}",
                        "column": {{{column}}},
                        "splitPk": "{{splitPk}}",
                        "connection": [
                            {
                                "table": ["{{table}}"],
                                "jdbcUrl": [
                                    "jdbc:mysql://{{host}}:{{port}}/{{db}}?parseTime=true&loc=Asia%2FShanghai"
                                ]
                            }
                        ]
                    }
                },
                "writer": {
                    "name": "otswriter",
                    "parameter": {
                        "endpoint":"http://{{{instance_name}}}.cn-hangzhou.ots.aliyuncs.com",
                        "accessId":"{{{access_id}}}",
                        "accessKey":"{{{access_key}}}",
                        "instanceName":"{{{instance_name}}}",
                        "table":"{{table}}",
                        "primaryKey" : {{{primary_keys}}},
                        "column" : {{{normals}}},
                        "writeMode" : "PutRow"
                    }
                }
            }
        ],
        "setting": {
            "speed": {
                "channel": 10
            }
        }
    }
}`

func tableToDataxOts(c *cli.Context, table model.Table) string {
	cols := table.Columns()
	var columns []string
	var splitPk string
	primaryKeys := []interface{}{}
	normals := []interface{}{}
	for col := range cols {
		columns = append(columns, col.Name())
		if col.IsPrimary() && (col.Type() == model.ColumnTypeBigInt || col.Type() == model.ColumnTypeInt || col.Type() == model.ColumnTypeInteger) {
			splitPk = col.Name()
			primaryKeys = append(primaryKeys, map[string]interface{}{
				"name": col.Name(),
				"type": colunmTypeToOtsStringType(col.Type()),
			})
		} else {
			normals = append(normals, map[string]interface{}{
				"name": col.Name(),
				"type": colunmTypeToOtsStringType(col.Type()),
			})
		}
	}
	data := map[string]interface{}{
		"host":          c.GlobalString("host"),
		"user":          c.GlobalString("user"),
		"password":      c.GlobalString("password"),
		"column":        toJSON(&columns),
		"splitPk":       splitPk,
		"table":         c.GlobalString("table"),
		"port":          c.GlobalString("port"),
		"db":            c.GlobalString("db"),
		"instance_name": c.String("instance_name"),
		"access_id":     c.String("id"),
		"access_key":    c.String("key"),
		"primary_keys":  toJSON(primaryKeys),
		"normals":       toJSON(normals),
	}
	tmpl, _ := mustache.ParseString(Templ)
	return prettyprint(tmpl.Render(data))
}

func toJSON(obj interface{}) string {
	data, _ := json.Marshal(obj)
	return string(data)
}

func colunmTypeToOtsStringType(c model.ColumnType) string {
	switch c {
	case model.ColumnTypeBit:
		return "int"
	case model.ColumnTypeTinyInt:
		return "int"
	case model.ColumnTypeSmallInt:
		return "int"
	case model.ColumnTypeMediumInt:
		return "int"
	case model.ColumnTypeInt:
		return "int"
	case model.ColumnTypeInteger:
		return "int"
	case model.ColumnTypeBigInt:
		return "int"
	case model.ColumnTypeReal:
		return "double"
	case model.ColumnTypeDouble:
		return "double"
	case model.ColumnTypeFloat:
		return "double"
	case model.ColumnTypeDecimal:
		return "double"
	case model.ColumnTypeNumeric:
		return "int"
	case model.ColumnTypeDate:
		return "string"
	case model.ColumnTypeTime:
		return "string"
	case model.ColumnTypeTimestamp:
		return "string"
	case model.ColumnTypeDateTime:
		return "string"
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

func prettyprint(b string) string {
	var out bytes.Buffer
	json.Indent(&out, []byte(b), "", "    ")
	return out.String()
}
