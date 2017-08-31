package main

import (
	"database/sql"
	"errors"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/qjpcpu/schemalex"
	"github.com/qjpcpu/schemalex/model"
	"github.com/urfave/cli"
	"os"
	"strings"
)

func main() {
	app := cli.NewApp()
	app.Usage = "读取mysql数据表并自动转化成X"

	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "host",
			Value: "127.0.0.1",
			Usage: "mysql host",
		},
		cli.StringFlag{
			Name:  "port, P",
			Value: "3306",
			Usage: "mysql port",
		},
		cli.StringFlag{
			Name:  "user,u",
			Usage: "mysql login user",
		},
		cli.StringFlag{
			Name:  "password,p",
			Usage: "mysql login password",
		},
		cli.StringFlag{
			Name:  "db, D",
			Usage: "db name",
		},
		cli.StringFlag{
			Name:  "table, t",
			Usage: "table name",
		},
	}

	app.Commands = []cli.Command{
		{
			Name:  "column",
			Usage: "显示列",
			Action: func(c *cli.Context) error {
				var table model.Table
				if tables, err := parseTables(c); err != nil {
					return err
				} else {
					table = tables[0]
				}
				cols := table.Columns()
				for col := range cols {
					fmt.Printf("%s\t%s\t%s\n", col.Name(), col.Type().String(), col.Comment())
				}
				return nil
			},
		},
		{
			Name:  "odps",
			Usage: "生成odps建表语句",
			Action: func(c *cli.Context) error {
				var table model.Table
				if tables, err := parseTables(c); err != nil {
					return err
				} else {
					table = tables[0]
				}
				fmt.Println(tableToOdpsSql(table))
				return nil
			},
		},
		{
			Name:  "dx-ots",
			Usage: "生成datax table store配置job",
			Action: func(c *cli.Context) error {
				var table model.Table
				if tables, err := parseTables(c); err != nil {
					return err
				} else {
					table = tables[0]
				}
				fmt.Println(tableToDataxOts(c, table))
				return nil
			},
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "instance_name, i",
					Value: "***********",
					Usage: "table store instance name",
				},
				cli.StringFlag{
					Name:  "id",
					Usage: "table store access id",
					Value: "**********",
				},
				cli.StringFlag{
					Name:  "key",
					Usage: "table store access key",
					Value: "**********",
				},
			},
		},
	}

	if err := app.Run(os.Args); err != nil {
		fmt.Fprintln(os.Stderr, err)
	}
}

func parseTables(c *cli.Context) ([]model.Table, error) {
	if table := c.GlobalString("table"); table == "" {
		return nil, errors.New("请指定表名")
	}
	db := c.GlobalString("db")
	if db == "" && !strings.Contains(c.GlobalString("table"), ".") {
		return nil, errors.New("请指定库名")
	}
	if db == "" {
		arr := strings.Split(c.GlobalString("table"), ".")
		c.GlobalSet("db", arr[0])
		c.GlobalSet("table", arr[1])
	}
	if c.GlobalString("user") == "" {
		return nil, errors.New("用户名为空")
	}
	connStr := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8mb4&parseTime=true&loc=Asia%%2FShanghai", c.GlobalString("user"), c.GlobalString("password"), c.GlobalString("host"), c.GlobalString("port"), c.GlobalString("db"))
	conn, err := sql.Open("mysql", connStr)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	result := conn.QueryRow("show create table " + c.GlobalString("table") + ";")
	var sql string
	var nonce string
	if err := result.Scan(&nonce, &sql); err != nil {
		return nil, err
	}
	return parseTablesFromSql(sql)
}

func parseTablesFromSql(sql string) ([]model.Table, error) {
	p := schemalex.New()
	stmts, err := p.ParseString(sql)
	if err != nil {
		return nil, err
	}
	var tables []model.Table
	for _, stmt := range stmts {
		if tbl, ok := stmt.(model.Table); ok {
			tables = append(tables, tbl)
		}
	}
	if len(tables) == 0 {
		return nil, errors.New("no create table sql found")
	}
	return tables, nil
}
