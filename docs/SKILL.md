---
name: s3-daily-sync
description: S3 数据每日同步管道：下载 conversion SQL + rejected JSONL → MySQL 中转 → ClickHouse → 数据清洗 → 飞书通知
version: 1.3.0
---

# S3 每日数据同步

## 1️⃣ 边界层 — 这个 Skill 做什么 / 不做什么

### 适合的场景
- ✅ S3 → ClickHouse **每日增量同步**（最近1-7天）
- ✅ 对历史异常数据进行修复性重同步（如 0424/0425/0426 数据问题）
- ✅ ClickHouse 表结构重建（DROP + CREATE）
- ✅ 被拒数据合并（rejected JSONL → ClickHouse UPDATE）
- ✅ 同步完成后自动飞书通知

### 不适合的场景 ❌
- ❌ **全量迁移**（MySQL → ClickHouse 一次性全量移动）→ 用 `mysql-to-clickhouse-v2` skill
- ❌ **实时数据流**（每分钟/每小时滚动同步）→ 本 skill 为批处理设计
- ❌ **跨 S3 Bucket 同步**（源和目标不在同一个 bucket）→ 需修改代码
- ❌ **跨 ClickHouse 实例**（目标不是 `192.168.0.61:8127`）→ 需修改代码
- ❌ **只同步 rejected 而不同步 conversion** → 本 skill 两者捆绑执行

### 已知行为约束
- ⚠️ MySQL 同一 tid 在不同批次出现时，ClickHouse ReplacingMergeTree 会去重（保留最高 ver 版本）→ 这是**预期行为**，不是 Bug
- ⚠️ MySQL 行数 ≠ ClickHouse 行数是正常的，只要 ClickHouse 行数 ≤ MySQL 且 gcd_result 无 NULL 即可
- ⚠️ rejected 处理结果"0条已应用"通常是正常的（rejected 记录已在 conversion 里）

---

## 2️⃣ 身份层 — Skill 在数据管道中扮演什么角色

```
数据生产者（S3）          本 Skill              数据消费者（AF 分析）
┌─────────────┐          ┌──────────────┐          ┌──────────────┐
│ phx-adx-    │   每天   │              │  每天    │              │
│ adids/      │ ──────→ │ 数据同步管道  │ ──────→ │ conversion_  │
│ conversion/ │  UTC00:50│              │  09:50前 │ context_*    │
│ rejected/   │         │ S3 → CH 清洗 │          │ 数据可用      │
└─────────────┘          └──────────────┘          └──────────────┘
                              │
                              └─→ 飞书通知（ou_62828...）
```

**角色：** 数据管道操作员
- 输入：S3 上的 `.sql.gz` 和 `.jsonl.gz` 文件
- 输出：ClickHouse `conversion_context_{date}` 表（已清洗）+ 飞书摘要
- 位置：整个 AF 数据流的**最上游**，是所有分析 query 的数据来源

---

## 3️⃣ 质量标准层 — 什么状态算成功 / 失败

### ✅ 同步成功标准（必须全部满足）

| 检查项 | 合格标准 | 验证方法 |
|--------|---------|---------|
| Step1 S3 下载 | conversion + rejected 均存在 | 日志含 `[OK] S3 下载成功` |
| Step2 MySQL 导入 | 无报错，INSERT 成功 | 日志含 `[OK] MySQL 导入完成` |
| Step3 ClickHouse 同步 | synced ≥ 1 行 | 日志含 `[OK] ClickHouse 同步完成: N 条` |
| gcd_result 无 NULL | `sum(gcd_result IS NULL) = 0` | 同步完成后查 ClickHouse |
| 清洗后 Non-organic 存在 | `sum(gcd_result='Non-organic') > 0` | 飞书通知数据 |
| 飞书通知 | 发送成功 | 日志含 `[OK] 飞书消息已发送` |

### ❌ 同步失败标志

| 错误类型 | 日志关键词 | 处理方式 |
|---------|-----------|---------|
| S3 凭证无效 | `InvalidAccessKeyId` / `Unable to locate credentials` | 检查 AWS 环境变量 |
| MySQL 导入失败 | `[FAIL] MySQL 导入` | 检查 SQL 文件格式 |
| ClickHouse INSERT 0 行 | `[OK] ClickHouse 同步完成: 0 条` | 查 MySQL 是否有数据 |
| EPERM 权限错误 | `Operation not permitted` | 见故障排查 EPERM 节 |
| 飞书发送失败 | `open_id cross app` | 检查 open_id 是否正确 |

### ⚠️ 警告状态（不阻断，但需关注）

- `gcd_result IS NULL` 行数 > 0 → 说明 SELECT 列映射有问题
- ClickHouse 行数 < MySQL 行数 50% 以上 → 说明有批次去重或 INSERT 失败
- 清洗后 Non-organic = 0 → 说明该天可能无转化或数据有问题

---

## 4️⃣ 工作流层 — 执行步骤（强制有序）

```
Step 1 ─ S3 下载
  ├─ boto3 下载 conversion SQL.gz → /tmp
  ├─ boto3 下载 rejected JSONL.gz（3个文件）× N → /tmp
  └─ 解压 → copy 到 /Users/macmini/Documents/sql/
           │
Step 2 ─ 处理 conversion SQL
  ├─ gunzip 解压
  ├─ mysql < conversion.sql（内含 DROP/CREATE/INSERT）
  └─ 验证：mysql count(*) = 预期行数
           │
Step 3 ─ 同步 ClickHouse
  ├─ DROP TABLE conversion_context_{date}（防止累加）
  ├─ CREATE TABLE（ReplacingMergeTree(ver) ORDER BY tid）
  ├─ MySQL SELECT（显式23列，用文件传参）
  └─ ClickHouse INSERT（分批，每批200行）
           │
Step 4 ─ 处理 rejected JSONL（目前为 0 条已应用，为正常状态）
  ├─ gunzip 解压 3 个文件
  ├─ 解析 JSONL → 构建 {tid: gcd} 字典
  └─ 临时表 + LEFT JOIN 批量更新
           │
Step 5 ─ 数据清洗（gcd_cleanup_ch.py）
  ├─ ALTER UPDATE：将 error/404 等纠正为真实被拒原因
  └─ DELETE：删除 Organic 等垃圾记录
           │
Step 6 ─ 飞书通知
  └─ 发送摘要到 ou_62828fc1d02e7e0cba506563bf1a90a5
```

### 执行命令

```bash
# 默认同步昨天数据
python3 daily_sync.py

# 指定日期（支持 MMDD 或 YYYYMMDD）
python3 daily_sync.py 0424          # → 2026-04-24
python3 daily_sync.py 20260424      # → 2026-04-24
```

### 定时任务（每天 09:50 北京时间）

```bash
launchctl list | grep com.daily.sync   # 查看状态
launchctl start com.daily.sync         # 手动触发一次
launchctl unload ~/Library/LaunchAgents/com.daily.sync.plist  # 暂停
launchctl load ~/Library/LaunchAgents/com.daily.sync.plist   # 恢复
```

---

## 5️⃣ 硬规则层 — 绝对不可触犯的红线

> 以下规则经过多次故障验证，违反必然导致数据问题或管道中断。

### 🚫 规则 1：ClickHouse 同步前必须先 DROP TABLE

```python
# ❌ 错误：直接 INSERT INTO，会导致行累加
INSERT INTO conversion_context_20260424 VALUES (...)

# ✅ 正确：先 DROP 再 CREATE（已在 daily_sync.py 实现）
DROP TABLE conversion_context_20260424
CREATE TABLE conversion_context_20260424 (...)
INSERT INTO conversion_context_20260424 VALUES (...)
```

**原因：** 不 DROP 时重跑会在原有数据上累加，0426 当天数据从 125K 累积到 775K。

---

### 🚫 规则 2：MySQL 查询必须用文件传参，禁止 shell 字符串内嵌

```python
# ❌ 错误：shell=True + 复杂 SQL（含大字段）
r = sh(f"mysql ... -e \"SELECT ... WHERE params='{big_json}'\"")

# ✅ 正确：用文件传递 SQL
with open(sql_file, 'w') as f:
    f.write(f"SELECT ... FROM {table} LIMIT {batch} OFFSET {offset}")
r = sh(f"mysql ... < {sql_file} 2>/dev/null")
os.unlink(sql_file)
```

**原因：** MySQL 9.6 `--silent` + `-e` 在脚本环境返回版本字符串而非数据；shell 字符串传递大字段时出现编码问题。

---

### 🚫 规则 3：ClickHouse INSERT 必须分批，通过 @file 传参

```python
# ❌ 错误：单条 SQL 超过 ARG_MAX
INSERT INTO ... VALUES (...), (...), ...  # 10万行 → OSError: Argument list too long

# ✅ 正确：分批 200 行 + 写文件传参
INSERT_BATCH = 200
for chunk in split(values, INSERT_BATCH):
    write_sql_file(chunk)
    subprocess.run(['curl', ..., '--data-binary', '@/tmp/ch_query_{pid}.sql'])
```

**原因：** macOS ARG_MAX ≈ 262KB，单条 SQL 传参超限。

---

### 🚫 规则 4：rejected 更新必须用临时表 + JOIN，禁止逐条 ALTER UPDATE

```sql
# ❌ 错误：逐条 mutation
ALTER TABLE conversion_context_20260424 UPDATE gcd_result='ai_layer' WHERE tid='{tid}';

# ✅ 正确：批量 JOIN
CREATE TABLE rej_tmp_{date} AS SELECT tid, gcd_result FROM rejected_data;
INSERT INTO new_tbl SELECT t.* REPLACING t.gcd_result u.gcd_result
  FROM conversion_context_{date} t LEFT JOIN rej_tmp_{date} u USING tid;
ALTER TABLE conversion_context_{date} EXCHANGE PARTITION p WITH TABLE new_tbl;
```

**原因：** 逐条 ALTER UPDATE 产生数千个独立 mutation，串行执行需 50+ 小时；JOIN 批量执行 0.3 秒。

---

### 🚫 规则 5：禁止使用 --silent 标志配合 -e 查询

```bash
# ❌ 错误：--silent 导致 -e 查询失效（MySQL 9.6.0 特有）
mysql ... -N --silent -e "SELECT 1"

# ✅ 正确：去掉 --silent，用 2>/dev/null 抑制 warning
mysql ... -N -e "SELECT 1" 2>/dev/null
```

**原因：** MySQL 9.6.0 中 `--silent` 与 `-e` 同时使用时报 RC=1 并输出版本字符串。

---

## 6️⃣ 反模式层 — 新手常犯的错误

### ❌ 误区 1：把 MySQL 行数当参照物，认为 ClickHouse 行数必须相等

**错误理解：** "MySQL 有 193833 行，ClickHouse 只有 98382 行，肯定丢数据了！"

**正确理解：** ClickHouse ReplacingMergeTree 按 ORDER BY tid 去重，同一 tid 在 MySQL 多批次出现时只保留一条。这是**预期行为**，只要 gcd_result 无 NULL 就是正确的。

**验证方式：**
```sql
-- 查询 ClickHouse 最终去重后行数（加 FINAL 关键字）
SELECT count() FROM conversion_context_20260424 FINAL;

-- 验证 gcd_result 全部有效
SELECT sum(gcd_result IS NULL) FROM conversion_context_20260424;  -- 必须是 0
```

---

### ❌ 误区 2：在 launchd 环境手动运行 `rm` / `unlink`

**错误理解：** "手动删文件再重跑不就行了？"

**正确理解：** launchd 沙箱对带 `com.apple.provenance` 扩展属性的文件有删除限制，`os.unlink()` 会报 EPERM。正确做法是用 `shutil.copyfile()` 覆盖，不要尝试删除。

---

### ❌ 误区 3：以为"0 条 rejected 已应用"是错误

**正确理解：** rejected 处理为 0 条是**正常状态**。因为 rejected 数据本身已经在 conversion SQL 里了（包含在 conversion_context 表的 gcd_result 字段中），Step4 的作用是在 ClickHouse 层更新被拒记录，而实际上大多数 conversion 记录的 gcd_result 已经在 MySQL 层就正确了。

---

### ❌ 误区 4：重复执行脚本来"追加"最新数据

**错误理解：** "再跑一次会把昨天的数据加上去。"

**正确理解：** 每次运行都会先 DROP TABLE，再重新导入完整数据（包含该天所有记录）。重复跑是**幂等**的，不会累加。

---

## 7️⃣ 输出模式层 — 不同的输出形式

### 模式 A：飞书通知（每日自动）

**接收人：** `ou_62828fc1d02e7e0cba506563bf1a90a5`

**格式：** schema 2.0 interactive card（`msg_type: "interactive"`），支持 markdown 表格和粗体。

**内容结构：**
```
📊 YYYY-MM-DD 数据同步完成
✅ 成功步骤: 4

conversion_context_YYYYMMDD:
  总记录: 269,881
  Non-organic: 138,316

📛 被拒数: 8,085 (被拒率 3.0%)
  ai_layer: 333
  bayesian_network: 647
  ...

🧹 数据清洗已执行
```

---

### 模式 B：本地日志（always）

**路径：** `/Users/macmini/.hermes/skills/data-pipeline/s3-daily-sync/scripts/daily_sync.log`

**格式：**
```
[2026-04-27 09:50:01] [Step1] S3 下载...
  [OK] 下载 conversion → /tmp/...
  [OK] S3 下载成功
[2026-04-27 09:50:15] [Step2] 处理 conversion SQL...
  [OK] MySQL 导入完成
[2026-04-27 09:51:02] [Step3] ClickHouse 同步...
  [OK] ClickHouse 同步完成: 193833 条
```

---

### 模式 C：Debug 追踪（按需）

**触发：** 在 `daily_sync.py` 里临时加调试输出

**用途：** 查 MySQL subprocess RC=1、数据字段偏移、INSERT 丢行等问题

---

### 模式 D：ClickHouse 直接查询（验证用）

```sql
-- 验证数据完整性
SELECT count(), sum(gcd_result IS NOT NULL), sum(gcd_result='Non-organic')
FROM conversion_context_{date}

-- 按被拒类型分布
SELECT gcd_result, count() as cnt
FROM conversion_context_{date}
GROUP BY gcd_result
ORDER BY cnt DESC
```

---

## 8️⃣ 验收层 + 参考库层 — 交付质量保障

### ✅ 验收清单（每次同步完成后执行）

```
□ 1. 日志无 [FAIL] 关键字
□ 2. ClickHouse 行数 > 0（不是 0 条）
□ 3. gcd_result IS NULL 行数 = 0
□ 4. Non-organic 行数 > 0
□ 5. 飞书消息发送成功
□ 6. 清洗后数据行数 < 清洗前（说明清洗生效）
□ 7. daily_sync_launchd.log 无报错
```

### ✅ 发布前检查（修改 daily_sync.py 后执行）

```
□ 1. 语法检查：python3 -m py_compile daily_sync.py
□ 2. 单独跑一天数据验证
□ 3. 检查 ClickHouse gcd_result 无 NULL
□ 4. 对比 MySQL count 和 ClickHouse count（允许 ClickHouse ≤ MySQL）
□ 5. 确认 launchd.plist 日志路径正确
```

### 📚 参考库（已确认有效的案例）

| 日期 | MySQL 行 | CH 行 | Non-organic | 状态 | 备注 |
|------|---------|-------|-------------|------|------|
| 2026-04-24 | 193,833 | 98,382 | 95,413 | ✅ | MySQL 有重复 tid，去重后正确 |
| 2026-04-25 | 148,859 | 80,189 | 77,032 | ✅ | 正常同步 |
| 2026-04-26 | 125,093 | 69,884 | 67,553 | ✅ | 正常同步 |
| 2026-05-07 | — | 269,881 | 138,316 | ✅ | 新凭证 AKIAVFWXPZMLX2BLIYVE |
| **2026-05-08** | — | — | — | ✅ | **通知格式升级：post text → schema 2.0 interactive card** |

### 通知格式变更历史
- v1.0: `msg_type: "text"` + 纯文本
- **v1.3.1 (2026-05-08)**: `msg_type: "interactive"` + schema 2.0 card → 支持 markdown 粗体/表格
  - `send_feishu()` 改为构建 `{"schema":"2.0", "body":{"elements":[{"tag":"markdown",...}]}}`
  - `build_notify_text()` 改用纯 markdown 格式

### 🐛 Bug 历史（已解决，禁止回滚）

| Bug | 现象 | 根因 | 状态 | 日期 |
|-----|------|------|------|------|
| INSERT INTO 不 DROP | 行数累加 126K→775K | 每次 INSERT 累加 | ✅ 已修复 | 0427 |
| SELECT * 列偏移 | gcd_result 100% NULL | MySQL 23列 vs CH 24列 | ✅ 已修复 | 0427 |
| --silent + -e | MySQL 输出版本字符串 | MySQL 9.6 兼容问题 | ✅ 已修复 | 0427 |
| shell 字符串大字段 | MySQL subprocess RC=1 | 编码/转义问题 | ✅ 已修复 | 0427 |
| curl ARG_MAX | INSERT 报 Argument list too long | SQL 超 262KB | ✅ 已修复 | 0427 |

---

## 📁 文件结构

```
s3-daily-sync/
├── SKILL.md                    # 本文件（8层架构）
└── scripts/
    ├── daily_sync.py           # 主脚本（已修复所有已知 Bug）
    ├── daily_sync.sh           # launchd wrapper（含 PATH + AWS 环境）
    ├── daily_sync.log          # 历史执行日志
    ├── daily_sync_launchd.log  # launchd 标准输出
    └── gcd_cleanup_ch.py       # 数据清洗（来自 conversion-context-clean skill）
```

## 环境变量

| 变量 | 说明 | 当前值 |
|------|------|--------|
| `AWS_ACCESS_KEY_ID` | S3 Access Key | `AKIAVFWXPZMLX2BLIYVE` |
| `AWS_SECRET_ACCESS_KEY` | S3 Secret Key | `ZAZVNs...AF2Z` |
| `AWS_DEFAULT_REGION` | 区域 | `ap-southeast-1` |
| `S3_BUCKET` | Bucket 名 | `phx-adx-adids` |

## S3 数据源

| 类型 | S3 路径 | 本地路径 |
|------|---------|---------|
| conversion | `s3://phx-adx-adids/up-file/mocker-conversion/conversion_context_{YYYYMMDD}.sql.gz` | `/Users/macmini/Documents/sql/` |
| rejected | `s3://phx-adx-adids/up-file/rejected/mocker_rejected_*_{YYYY-MM-DD}.gz` | `/Users/macmini/Documents/sql/rejected/` |

> ⚠️ rejected 文件日期格式是 `YYYY-MM-DD`（连字符），不是 `YYYYMMDD`。

## ClickHouse 表结构

```sql
CREATE TABLE paocai.conversion_context_{date} (
  tid             String,
  platform        Nullable(String),
  ad_source       Nullable(String),
  offer_id        Nullable(String),
  site_id         Nullable(String),
  country         Nullable(String),
  pkg             Nullable(String),
  mmp             Nullable(String),
  random          Nullable(String),
  gaid            Nullable(String),
  sensor_size     Nullable(String),
  ua              Nullable(String),
  params          Nullable(String),     -- 大字段，INSERT 时分批
  callback        Nullable(String),
  gcd_result      Nullable(String),
  predict_result  Nullable(String),
  gp_click_ts     Nullable(Int64),
  gp_install_begin_ts Nullable(Int64),
  install_ts      Nullable(Int64),
  first_launch_date_ts Nullable(Int64),
  last_launch_date  Nullable(String),
  gmt_create      Nullable(String),
  gmt_update      Nullable(String),
  ip              Nullable(String),     -- 始终为 NULL，无此数据
  ver             UInt32                -- 批次号，用于 ReplacingMergeTree
) ENGINE = ReplacingMergeTree(ver)
ORDER BY tid;
```

## 数据清洗规则

**保留类型：**
- Non-organic、ai_layer、bayesian_network、fake_install_parameters
- device_farms、click_clusters、device_emulators
- 前缀匹配：`bots/*`、`behavioral_anomalies/*`、`validation_*/*`、`install_hijacking/*`、`click_flood/*`、`timestamp_anomalies`、`fake_device*`

**删除类型：** Organic（含被拒原因的 Organic）、及其他非保留类型
