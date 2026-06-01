# OceanBase Sink：按版本号条件 Upsert（MySQL 模式）

Flink SQL 的 `INSERT INTO` 无法直接写 `ON DUPLICATE KEY UPDATE ... IF(VALUES(version) > version, ...)`。在 **MySQL 兼容模式**下，可通过 Sink 选项让 Connector 自动生成带版本比较的 upsert SQL。

Oracle 兼容模式仍使用 `MERGE`，不支持本功能。

---

## 适用场景

- 主键冲突时，仅当**入参版本号大于库中版本**才更新非主键列（乐观锁 / 乱序回放保护）。
- 需要完全自定义 `ON DUPLICATE KEY UPDATE` 赋值列表时，使用自定义子句选项。

---

## 配置项

| 参数 | 必填 | 说明 |
|------|------|------|
| `sink.upsert.version-column` | 否 | 版本列物理名。配置后，对所有**非主键列**生成 `col=IF(VALUES(ver)>ver, VALUES(col), col)`。 |
| `sink.duplicate-key-update-clause` | 否 | 自定义赋值列表（**不含** `ON DUPLICATE KEY UPDATE` 前缀），例如 `` `data`=IF(VALUES(`version`)>`version`,VALUES(`data`),`data`) ``。 |

**互斥**：两个选项不能同时配置，建表校验会报错。

**默认行为**（两者均未配置）：非主键列使用 `col=VALUES(col)`；若 sink 列与主键完全相同则退化为 `INSERT IGNORE`。

仅 **MySQL 兼容模式**（`jdbc:mysql://` 或 MySQL 模式 OB）生效。

---

## 示例：按版本列自动比较

目标表（OceanBase / MySQL）：

```sql
CREATE TABLE t_orders (
  id      INT PRIMARY KEY,
  data    VARCHAR(255) NOT NULL,
  version INT NOT NULL
);
```

Flink SQL：

```sql
CREATE TABLE ob_sink (
  id      INT,
  data    STRING,
  version INT,
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' = 'oceanbase',
  'url' = 'jdbc:mysql://host:port/test?useUnicode=true&characterEncoding=UTF-8&useSSL=false',
  'username' = 'user@tenant',
  'password' = 'secret',
  'schema-name' = 'test',
  'table-name' = 't_orders',
  'sink.upsert.version-column' = 'version'
);

-- 普通 INSERT；冲突时由 Connector 生成条件 upsert
INSERT INTO ob_sink VALUES (1, 'v1', 1);
INSERT INTO ob_sink VALUES (1, 'v2', 2);
-- 若再写入 version=1，库中 version 已为 2，则 data 不会被旧版本覆盖
INSERT INTO ob_sink VALUES (1, 'stale', 1);
```

生成的 SQL 片段（示意）：

```sql
INSERT INTO `test`.`t_orders`(...) VALUES (...)
ON DUPLICATE KEY UPDATE
  `data`=IF(VALUES(`version`)>`version`,VALUES(`data`),`data`),
  `version`=IF(VALUES(`version`)>`version`,VALUES(`version`),`version`)
```

---

## 示例：自定义 UPDATE 子句

```sql
CREATE TABLE ob_sink ( ... ) WITH (
  ...
  'sink.duplicate-key-update-clause' = '`data`=IF(VALUES(`version`)>`version`,VALUES(`data`),`data`), `version`=IF(VALUES(`version`)>`version`,VALUES(`version`),`version`)'
);
```

列名须与物理表一致，建议用反引号包裹标识符。

---

## 行为说明

| 场景 | 结果 |
|------|------|
| 新主键 | 插入新行 |
| 冲突且 `VALUES(version) > version` | 更新非主键列 |
| 冲突且入参 version ≤ 库中 version | 保留库中值 |
| 未配置版本选项 | 冲突时始终用入参覆盖非主键列 |

版本列必须出现在 Sink DDL 列列表中，否则启动/生成 SQL 时报错。

---

## 集成测试

| 测试类 | 环境 |
|--------|------|
| `OceanBaseVersionUpsertSqlITCase` | Testcontainers 启动 `oceanbase/oceanbase-ce`（需本机 Docker 资源充足） |
| `OceanBaseVersionUpsertExternalITCase` | 外部实例，通过环境变量连接 |

外部测试环境变量（勿提交到仓库）：

| 变量 | 说明 |
|------|------|
| `OCEANBASE_VERSION_UPSERT_IT_JDBC_URL` | 完整 JDBC URL（含库名与参数） |
| `OCEANBASE_VERSION_UPSERT_IT_USERNAME` | 用户名，如 `test@tt1` |
| `OCEANBASE_VERSION_UPSERT_IT_PASSWORD` | 密码（可选，默认空） |
| `OCEANBASE_VERSION_UPSERT_IT_SCHEMA` | `schema-name`，默认 `test` |

```bash
export OCEANBASE_VERSION_UPSERT_IT_JDBC_URL='jdbc:mysql://host:port/test?useUnicode=true&characterEncoding=UTF-8&useSSL=false'
export OCEANBASE_VERSION_UPSERT_IT_USERNAME='test@tt1'
export OCEANBASE_VERSION_UPSERT_IT_PASSWORD='test'

cd flink-connector-oceanbase
mvn test -Dtest=OceanBaseVersionUpsertExternalITCase
```

单元测试：`OceanBaseMySQLDialectTest`、`MySQLDuplicateKeyUpdateClauseBuilderTest`、`OceanBaseConnectorOptionsUpsertTest`。

---

## 参考

- 主文档 Sink 配置：[flink-connector-oceanbase_cn.md](./flink-connector-oceanbase_cn.md)
- 英文说明：[ob-sink-version-upsert-usage-en.md](./ob-sink-version-upsert-usage-en.md)
