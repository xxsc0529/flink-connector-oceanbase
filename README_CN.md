# OceanBase Connectors for Apache Flink

[English](README.md) | 简体中文

[![Build Status](https://github.com/oceanbase/flink-connector-oceanbase/actions/workflows/push_pr.yml/badge.svg?branch=main)](https://github.com/oceanbase/flink-connector-oceanbase/actions/workflows/push_pr.yml?query=branch%3Amain)
[![Release](https://img.shields.io/github/release/oceanbase/flink-connector-oceanbase.svg)](https://github.com/oceanbase/flink-connector-oceanbase/releases)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

本仓库包含 OceanBase 的 Flink Connector。

## 功能

运行环境需要准备

- JDK 8
- Flink 1.15 或后续版本

本仓库提供了如下 Connector：

|               Connector                |                                                         描述                                                         |                             使用文档                             |
|----------------------------------------|--------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------|
| Flink Connector: OceanBase             | 该Connector通过OceanBase支持的JDBC驱动将数据写入OceanBase，支持MySQL 和 Oracle 兼容模式。                                                | [Sink](docs/sink/flink-connector-oceanbase_cn.md)            |
| Flink Connector: OceanBase Direct Load | 该Connector通过[旁路导入](https://www.oceanbase.com/docs/common-oceanbase-database-cn-1000000001428636)API将数据写入OceanBase。 | [Sink](docs/sink/flink-connector-oceanbase-directload_cn.md) |
| Flink Connector: OBKV HBase            | 该Connector通过[OBKV HBase API](https://github.com/oceanbase/obkv-hbase-client-java)将数据写入OceanBase。                   | [Sink](docs/sink/flink-connector-obkv-hbase_cn.md)           |

我们还提供了一个用于提交 Flink 端到端任务的命令行工具，详细信息请参阅 [CLI 文档](docs/cli/README_CN.md)。

### 其他外部项目

在其他的社区和组织中，也有一些项目可以用于通过 Flink 处理 OceanBase 中的数据。

|                                Project                                 | OceanBase 兼容模式 |                                                                      支持的功能                                                                       |
|------------------------------------------------------------------------|----------------|--------------------------------------------------------------------------------------------------------------------------------------------------|
| [Flink Connector JDBC](https://github.com/apache/flink-connector-jdbc) | MySQL, Oracle  | [Source + Sink](https://nightlies.apache.org/flink/flink-docs-release-1.19/zh/docs/connectors/table/jdbc/)                                       |
| [Flink CDC](https://github.com/ververica/flink-cdc-connectors)         | MySQL, Oracle  | [Source + CDC](https://nightlies.apache.org/flink/flink-cdc-docs-master/zh/docs/connectors/flink-sources/oceanbase-cdc/)                         |
| [Apache SeaTunnel](https://github.com/apache/seatunnel)                | MySQL, Oracle  | [Source](https://seatunnel.apache.org/docs/connector-v2/source/OceanBase), [Sink](https://seatunnel.apache.org/docs/connector-v2/sink/OceanBase) |

## 社区

当你需要帮助时，你可以在 [https://ask.oceanbase.com](https://ask.oceanbase.com) 上找到开发者和其他的社区伙伴。

当你发现项目缺陷时，请在 [issues](https://github.com/oceanbase/flink-connector-oceanbase/issues) 页面创建一个新的 issue。

## 许可证

更多信息见 [LICENSE](LICENSE)。
