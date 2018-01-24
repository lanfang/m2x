mysql建表语句转换为odps建表语句
============================

#### install

```
go get github.com/qjpcpu/m2x
```

#### usage


```
NAME:
   m2x - 读取mysql数据表并自动转化成X

USAGE:
   m2x [global options] command [command options] [arguments...]

VERSION:
   0.0.0

COMMANDS:
     column    显示列
     gomod     生成go model
     odps      生成odps建表语句
     dx-odps   生成odps-mysql数据导出datax配置
     dx-rodps  生成mysql-odps数据导入datax配置
     dx-ots    生成datax table store配置job
     help, h   Shows a list of commands or help for one command

GLOBAL OPTIONS:
   --host value                mysql host (default: "127.0.0.1")
   --port value, -P value      mysql port (default: "3306")
   --user value, -u value      mysql login user
   --password value, -p value  mysql login password
   --db value, -D value        db name
   --table value, -t value     table name
   --file value, -f value      sql file name
   --help, -h                  show help
   --version, -v               print the version

e.g.
m2x  --host 10.11.200.1 -u USER -p PWD -t DB.TABLE gomod
m2x -f example.sql gomod
```
