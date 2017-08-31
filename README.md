mysql建表语句转换为odps建表语句
============================

#### install

```
# standard
go get github.com/qjpcpu/migrate-table
# centos linux
wget https://raw.githubusercontent.com/qjpcpu/migrate-table/master/release/migrate-table.linux -O migrate-table && chmod +x migrate-table
# macox
wget https://raw.githubusercontent.com/qjpcpu/migrate-table/master/release/migrate-table.osx -O migrate-table && chmod +x migrate-table
```

#### usage

forexample, statements.sql is:

```
CREATE TABLE `goods` (
  `goods_id` varchar(32) NOT NULL DEFAULT '' COMMENT '商品ID',
  `enabled` tinyint(4) unsigned NOT NULL DEFAULT '1' COMMENT '有效状态1：有效；2：无效',
  `sp_id` varchar(20) NOT NULL DEFAULT '' COMMENT '商户号',
  `market_price` bigint(20) NOT NULL DEFAULT '0' COMMENT '市场价',
  `create_time` datetime NOT NULL DEFAULT '0000-00-00 00:00:00' COMMENT '创建时间',
  `start_sell_time` datetime NOT NULL DEFAULT '0000-00-00 00:00:00' COMMENT '开售时间',
  `expired_time` datetime NOT NULL DEFAULT '0000-00-00 00:00:00' COMMENT '过期时间',
  `modify_time` datetime NOT NULL DEFAULT '0000-00-00 00:00:00' COMMENT '修改时间',
  PRIMARY KEY (`goods_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

```
cat statements.sql | migrate-table
migrate-table statements.sql
```

then output would be:

```
CREATE TABLE IF NOT EXISTS goods(
  goods_id STRING COMMENT '商品ID',
  enabled BIGINT COMMENT '有效状态1：有效；2：无效',
  sp_id STRING COMMENT '商户号',
  market_price BIGINT COMMENT '市场价',
  create_time DATETIME COMMENT '创建时间',
  start_sell_time DATETIME COMMENT '开售时间',
  expired_time DATETIME COMMENT '过期时间',
  modify_time DATETIME COMMENT '修改时间')
PARTITIONED BY (date_flag STRING COMMENT '日期分区,格式为yyyy-mm-dd');
```
