# OceanBase Sink: Version-Conditional Upsert (MySQL Mode)

Flink SQL `INSERT INTO` cannot express `ON DUPLICATE KEY UPDATE ... IF(VALUES(version) > version, ...)`. In **MySQL compatible mode**, use sink options so the connector generates conditional upsert SQL automatically.

Oracle compatible mode uses `MERGE` and does not support these options.

---

## When to Use

- On primary-key conflict, update non-key columns only when the **incoming version is greater** than the stored version (out-of-order replay protection).
- For a fully custom assignment list, use `sink.duplicate-key-update-clause`.

---

## Options

| Option | Required | Description |
|--------|----------|-------------|
| `sink.upsert.version-column` | No | Physical version column name. Generates `col=IF(VALUES(ver)>ver, VALUES(col), col)` for every **non-primary-key** column. |
| `sink.duplicate-key-update-clause` | No | Custom assignment list **without** the `ON DUPLICATE KEY UPDATE` prefix. |

**Mutually exclusive**: both options cannot be set; validation fails at table creation.

**Default** (neither set): non-key columns use `col=VALUES(col)`; if all columns are keys, falls back to `INSERT IGNORE`.

Applies only in **MySQL compatible mode**.

---

## Example: Auto Version Compare

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

INSERT INTO ob_sink VALUES (1, 'v1', 1);
INSERT INTO ob_sink VALUES (1, 'v2', 2);
INSERT INTO ob_sink VALUES (1, 'stale', 1);  -- keeps v2 when version does not increase
```

---

## Integration Tests

| Class | Environment |
|-------|-------------|
| `OceanBaseVersionUpsertSqlITCase` | Testcontainers OceanBase CE |
| `OceanBaseVersionUpsertExternalITCase` | External instance via env vars |

Env vars: `OCEANBASE_VERSION_UPSERT_IT_JDBC_URL`, `OCEANBASE_VERSION_UPSERT_IT_USERNAME`, `OCEANBASE_VERSION_UPSERT_IT_PASSWORD` (optional), `OCEANBASE_VERSION_UPSERT_IT_SCHEMA` (optional, default `test`).

See [ob-sink-version-upsert-usage.md](./ob-sink-version-upsert-usage.md) (Chinese) for full details and SQL examples.
