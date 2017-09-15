package main

import (
	"fmt"
	"github.com/hoisie/mustache"
	"github.com/qjpcpu/schemalex/model"
	"github.com/urfave/cli"
	"strings"
)

const Templ2Odps = `
{
    "job": {
        "content": [
            {
                "reader": {
                    "name":"odpsreader",
                    "parameter":{
                        "accessId": "{{{id}}}",
                        "accessKey": "{{{key}}}",
                        "column": {{{columns}}},
                        "odpsServer":"{{{odpsServer}}}",
                        "partition":{{{partition}}},
                        "project":"{{project}}",
                        "splitMode":"record",
                        "table":"{{otable}}"
                    }
                },
                "writer": {
                    "name": "mysqlwriter",
                    "parameter": {
                        "writeMode": "insert",
                        "username": "{{{user}}}",
                        "password": "{{{password}}}",
                        "column": {{{columns}}},
                        "preSql": [],
                        "connection": [
                            {
                                "jdbcUrl": "jdbc:mysql://{{host}}:{{port}}/{{db}}",
                                "table": ["{{table}}"]
                            }
                        ]
                    }
                }
            }
        ],
        "setting": {
            "speed": {
                "channel": {{channel}}
            }
        }
    }
}
`

const TemplfOdps = `
{
{{{backup}}}
    "job": {
        "content": [
            {
                "reader": {
                    "name": "mysqlreader",
                    "parameter": {
                        "username": "{{user}}",
                        "password": "{{{password}}}",
                        "column": {{{columns}}},
                        {{{where}}}
                        {{{splitPk}}}
                        "connection": [
                            {
                                "table": [
                                    "{{table}}"
                                ],
                                "jdbcUrl": [
                                    "jdbc:mysql://{{host}}:{{port}}/{{db}}"
                                ]
                            }
                        ]
                    }
                },
                "writer": {
                    "name": "odpswriter",
                    "parameter": {
                        "accessId": "{{{id}}}",
                        "accessKey": "{{{key}}}",
                        "column": {{{columns}}},
                        "odpsServer": "{{{odpsServer}}}",
                        {{{partition}}}
                        "project": "{{project}}",
                        "table": "{{otable}}",
                        "truncate": true
                    }
                }
            }
        ],
        "setting": {
            "speed": {
                "channel": {{channel}}
            }
        }
    }
}
`

func tableToDataxOdps(c *cli.Context, table model.Table) string {
	if !strings.Contains(c.String("otable"), ".") {
		return "otable格式为project.table"
	}
	arr := strings.Split(c.String("otable"), ".")
	project, otable := arr[0], arr[1]
	cols := table.Columns()
	var columns []string
	for col := range cols {
		columns = append(columns, col.Name())
	}

	data := map[string]interface{}{
		"host":          c.GlobalString("host"),
		"user":          c.GlobalString("user"),
		"password":      c.GlobalString("password"),
		"columns":       toJSON(&columns),
		"table":         c.GlobalString("table"),
		"port":          c.GlobalString("port"),
		"db":            c.GlobalString("db"),
		"instance_name": c.String("instance_name"),
		"odpsServer":    c.String("endpoint"),
		"id":            c.String("id"),
		"key":           c.String("key"),
		"project":       project,
		"otable":        otable,
		"partition":     []string{},
		"channel":       c.Int("channel"),
	}
	if partition := c.String("partition"); partition != "" {
		data["partition"] = toJSON([]string{partition})
	}
	tmpl, _ := mustache.ParseString(Templ2Odps)
	return prettyprint(tmpl.Render(data))
}

func tableToDataxRodps(c *cli.Context, table model.Table) string {
	if !strings.Contains(c.String("otable"), ".") {
		return "otable格式为project.table"
	}
	arr := strings.Split(c.String("otable"), ".")
	project, otable := arr[0], arr[1]
	cols := table.Columns()
	var columns []string
	var splitPk string
	for col := range cols {
		columns = append(columns, col.Name())
		if col.IsPrimary() && (col.Type() == model.ColumnTypeBigInt || col.Type() == model.ColumnTypeInt || col.Type() == model.ColumnTypeInteger) {
			splitPk = col.Name()
		}
	}

	data := map[string]interface{}{
		"host":          c.GlobalString("host"),
		"user":          c.GlobalString("user"),
		"password":      c.GlobalString("password"),
		"columns":       toJSON(&columns),
		"table":         c.GlobalString("table"),
		"port":          c.GlobalString("port"),
		"db":            c.GlobalString("db"),
		"instance_name": c.String("instance_name"),
		"odpsServer":    c.String("endpoint"),
		"id":            c.String("id"),
		"key":           c.String("key"),
		"project":       project,
		"otable":        otable,
		"channel":       c.Int("channel"),
	}
	if splitPk != "" {
		data["splitPk"] = fmt.Sprintf(`"splitPk": "%s",`, splitPk)
	}
	if pt := c.String("partition"); pt != "" {
		data["partition"] = fmt.Sprintf(`"partition":"%s",`, pt)
	}
	if where := c.String("where"); where != "" {
		data["where"] = fmt.Sprintf(`"where":"%s",`, where)
	}
	if tee := c.String("backup"); tee != "" {
		data["backup"] = fmt.Sprintf(`    "core": {
        "transport": {
            "exchanger": {
                "tee": "%s"
            }
        }
    },`, tee)
	}
	tmpl, _ := mustache.ParseString(TemplfOdps)
	return prettyprint(tmpl.Render(data))
}
