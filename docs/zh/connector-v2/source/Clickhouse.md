# Clickhouse

> Clickhouse 的连接器


## 支持的引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 主要功能

- [x] [批处理](../../concept/connector-v2-features.md)
- [ ] [流处理](../../concept/connector-v2-features.md)
- [ ] [精确一次](../../concept/connector-v2-features.md)
- [x] [column projection](../../concept/connector-v2-features.md)
- [ ] [并行读取](../../concept/connector-v2-features.md)
- [ ] [支持用户定义的拆分](../../concept/connector-v2-features.md)

> supports query SQL and can achieve projection effect.

## Description

从Clickhouse读取数据.

## 支持的数据源信息

使用Clickhouse连接器，需要下列依赖项,它们可以通过install-plugin.sh或从Maven中央存储库下载。

| Datasource | Supported Versions | Dependency                                                                               |
|------------|--------------------|------------------------------------------------------------------------------------------|
| Clickhouse | universal          | [Download](https://mvnrepository.com/artifact/org.apache.seatunnel/connector-clickhouse) |

## 数据类型映射

|                                                             Clickhouse Data Type                                                              | SeaTunnel Data Type |
|-----------------------------------------------------------------------------------------------------------------------------------------------|---------------------|
| String / Int128 / UInt128 / Int256 / UInt256 / Point / Ring / Polygon MultiPolygon                                                            | STRING              |
| Int8 / UInt8 / Int16 / UInt16 / Int32                                                                                                         | INT                 |
| UInt64 / Int64 / IntervalYear / IntervalQuarter / IntervalMonth / IntervalWeek / IntervalDay / IntervalHour / IntervalMinute / IntervalSecond | BIGINT              |
| Float64                                                                                                                                       | DOUBLE              |
| Decimal                                                                                                                                       | DECIMAL             |
| Float32                                                                                                                                       | FLOAT               |
| Date                                                                                                                                          | DATE                |
| DateTime                                                                                                                                      | TIME                |
| Array                                                                                                                                         | ARRAY               |
| Map                                                                                                                                           | MAP                 |

## 数据源参数

|       Name        |  Type  | Required |        Default         |                                                                                                                                                 Description                                                                                                                                                 |
|-------------------|--------|----------|------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| host              | String | Yes      | -                      | `ClickHouse` cluster address, the format is `host:port` , allowing multiple `hosts` to be specified. Such as `"host1:8123,host2:8123"` .                                                                                                                                                                    |
| database          | String | Yes      | -                      | The `ClickHouse` database.                                                                                                                                                                                                                                                                                  |
| sql               | String | Yes      | -                      | The query sql used to search data though Clickhouse server.                                                                                                                                                                                                                                                 |
| username          | String | Yes      | -                      | `ClickHouse` user username.                                                                                                                                                                                                                                                                                 |
| password          | String | Yes      | -                      | `ClickHouse` user password.                                                                                                                                                                                                                                                                                 |
| clickhouse.config | Map    | No       | -                      | In addition to the above mandatory parameters that must be specified by `clickhouse-jdbc` , users can also specify multiple optional parameters, which cover all the [parameters](https://github.com/ClickHouse/clickhouse-jdbc/tree/master/clickhouse-client#configuration) provided by `clickhouse-jdbc`. |
| server_time_zone  | String | No       | ZoneId.systemDefault() | The session time zone in database server. If not set, then ZoneId.systemDefault() is used to determine the server time zone.                                                                                                                                                                                |
| common-options    |        | No       | -                      | Source plugin common parameters, please refer to [Source Common Options](../source-common-options.md) for details.                                                                                                                                                                                          |

## 如何创建Clickhouse数据同步作业

下面的示例演示了如何创建一个数据同步作业，该作业从Clickhouse读取数据并在本地客户端上打印数据

```bash
# Set the basic configuration of the task to be performed
env {
  parallelism = 10
  job.mode = "BATCH"
}

# Create a source to connect to Clickhouse
source {
  Clickhouse {
    host = "localhost:8123"
    database = "default"
    sql = "select * from test where age = 20 limit 100"
    username = "xxxxx"
    password = "xxxxx"
    server_time_zone = "UTC"
    plugin_output = "test"
    clickhouse.config = {
      "socket_timeout": "300000"
    }
  }
}

# Console printing of the read Clickhouse data
sink {
  Console {
    parallelism = 1
  }
}
```

### 多表读取

> 这是一个多表读取案例


```bash


env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Clickhouse {
    host = "clickhouse:8123"
    database = "default"
    username = "default"
    password = ""
    server_time_zone = "UTC"
    plugin_output = "test"
    clickhouse.config = {
      "socket_timeout": "300000"
    }
    table_list = [
      {
        table_path = "t1"
        sql = "select * from t1 where 1=1 "

      },
      {
        table_path = "t2",
        sql = "select * from t2"
      }
    ]
  }
}

sink {
  Clickhouse {
    host = "clickhouse:8123"
    database = "default"
    table = "t3"
    username = "default"
    password = ""
  }
}
```

### Tips

> 1.[SeaTunnel Deployment Document](../../start-v2/locally/deployment.md).

