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
	var primaryKeys []interface{}
	var normals []interface{}
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
		return "INTEGER"
	case model.ColumnTypeTinyInt:
		return "INTEGER"
	case model.ColumnTypeSmallInt:
		return "INTEGER"
	case model.ColumnTypeMediumInt:
		return "INTEGER"
	case model.ColumnTypeInt:
		return "INTEGER"
	case model.ColumnTypeInteger:
		return "INTEGER"
	case model.ColumnTypeBigInt:
		return "INTEGER"
	case model.ColumnTypeReal:
		return "DOUBLE"
	case model.ColumnTypeDouble:
		return "DOUBLE"
	case model.ColumnTypeFloat:
		return "DOUBLE"
	case model.ColumnTypeDecimal:
		return "DOUBLE"
	case model.ColumnTypeNumeric:
		return "INTEGER"
	case model.ColumnTypeDate:
		return "STRING"
	case model.ColumnTypeTime:
		return "STRING"
	case model.ColumnTypeTimestamp:
		return "STRING"
	case model.ColumnTypeDateTime:
		return "STRING"
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

func prettyprint(b string) string {
	var out bytes.Buffer
	json.Indent(&out, []byte(b), "", "    ")
	return out.String()
}
