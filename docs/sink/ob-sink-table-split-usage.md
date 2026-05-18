# OceanBase Sink 按列分表

## 功能说明

Sink 可根据某一列的值，将数据写入不同的物理表。Flink 表 DDL 可包含该路由列，但路由列**不会**写入 OceanBase。

当路由列值为 **NULL** 或 **trim 后为空字符串** 时，数据写入配置的 `table-name`（原表），不拼接后缀或前缀。

## 配置项

|            参数             |   必填    |                  说明                   |
|---------------------------|---------|---------------------------------------|
| `table-name`              | 是       | 原表名，例如 `user_info`                    |
| `table-name.split-column` | 启用分表时必填 | 路由列名，须为 `STRING` / `CHAR` / `VARCHAR` |
| `table-name.split-affix`  | 启用分表时必填 | `suffix` 或 `prefix`                   |
| `table-name.split-concat` | 否       | 连接符，默认 `_`                            |

## 示例

```sql
CREATE TABLE user_info (
  name STRING,
  age INT,
  country STRING,
  split_column STRING,
  PRIMARY KEY (name) NOT ENFORCED
) WITH (
  'connector' = 'oceanbase',
  'url' = '...',
  'username' = '...',
  'password' = '...',
  'schema-name' = 'test',
  'table-name' = 'user_info',
  'table-name.split-column' = 'split_column',
  'table-name.split-affix' = 'suffix',
  'table-name.split-concat' = '_'
);
```

- `split_column = 'cn'` → 写入 `user_info_cn`
- `split_column = 'us'` → 写入 `user_info_us`
- `split_column` 为 NULL 或空 → 写入 `user_info`

物理表 `user_info`、`user_info_cn`、`user_info_us` 须事先在 OceanBase 中创建，且业务列结构一致（不含 `split_column`）。

## 限制

- 不可与 `partition.enabled=true` 同时启用（启动校验会报错）。
- 可与 `file-completion.kafka.notification-enabled=true` 同时使用；Flink 表需同时包含 split 列与完成通知列，且这些列均不会写入 OB。
- `url` 使用 `jdbc:mysql://` 时沿用默认 `com.mysql.cj.jdbc.Driver`；若使用 `jdbc:oceanbase://`，须设置 `driver-class-name=com.oceanbase.jdbc.Driver`。

## 集成测试

测试类：`OceanBaseTableSplitSqlITCase`。通过环境变量提供连接信息（勿提交到仓库）：

- `OCEANBASE_FILE_COMPLETION_IT_JDBC_URL`
- `OCEANBASE_FILE_COMPLETION_IT_USERNAME`
- `OCEANBASE_FILE_COMPLETION_IT_PASSWORD`（可选）
- `OCEANBASE_FILE_COMPLETION_IT_SCHEMA`（可选，默认 `test`）
- `OCEANBASE_TABLE_SPLIT_IT_TABLE_BASE`（可选，默认 `t_user_info_split_it`）

```bash
mvn -pl flink-connector-oceanbase -Dtest=OceanBaseTableSplitSqlITCase test
```

