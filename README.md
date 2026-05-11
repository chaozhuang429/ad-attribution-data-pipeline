# 广告归因数据全自动同步管道

> S3 → MySQL → ClickHouse 零运维每日同步管线，面向 AppsFlyer 广告归因反欺诈场景。

## 产品定位

面向广告归因平台的数据同步基础设施。每日自动将云端 20-30 万条归因安装记录同步到分析库，为上层反欺诈诊断、异常预警、策略测试提供可靠数据底座。

## 解决的核心问题

广告投放中，AppsFlyer 每日产出海量归因数据（conversion 安装记录 + rejected 被拒记录），需经 MySQL 中转入 ClickHouse 列存分析库。手工操作面临：

- **Schema 频繁差异** — 新旧日分区表列数不同（23 列 vs 24 列）、字段类型不兼容（Int32 vs String）
- **重复导入累加** — ReplacingMergeTree 去重逻辑不当时数据膨胀数倍
- **清洗规则散落** — 数万条脏数据（错误 gcd_result、Organic 混入）需逐类型清洗
- **命令行限制** — macOS ARG_MAX 导致单条 INSERT 超 10 万行时崩溃
- **沙箱权限陷阱** — launchd 定时环境对文件移动/删除有额外限制

本管道实现了从 S3 到 ClickHouse 的**全流程自动化**，经过 6 个生产 Bug 修复后已稳定运行。

## 核心功能模块

```
Step 1 ─ S3 下载
  ├─ boto3 下载 conversion SQL.gz + rejected JSONL.gz
  └─ 解压到本地中转目录

Step 2 ─ MySQL 导入
  ├─ 文件传参导入（避免 shell 字符串内嵌大字段）
  └─ 去 --silent 兼容 MySQL 9.6

Step 3 ─ ClickHouse 同步
  ├─ DROP TABLE（防止累加）
  ├─ CREATE TABLE (ReplacingMergeTree ORDER BY tid)
  ├─ 分批 INSERT（每批 200 行，@file 传参绕开 ARG_MAX）
  └─ JOIN 批量更新替代逐条 ALTER UPDATE

Step 4 ─ 数据清洗
  ├─ 纠正 error/404 为真实被拒原因
  └─ 删除 Organic 等无效记录

Step 5 ─ 即时通知
  └─ 推送同步摘要（总量、被拒分布、Non-organic 占比）
```

## 技术实现

| 模块 | 技术选型 | 关键决策 |
|------|---------|---------|
| 数据源 | AWS S3 (boto3) | 双文件同步：conversion + rejected |
| 中转层 | MySQL 9.6 | 文件传参替代 -e 内嵌，避开编码问题 |
| 分析库 | ClickHouse 23.11 | ReplacingMergeTree(ver) ORDER BY tid 自动去重 |
| 清洗 | Python + ALTER UPDATE | JOIN 临时表批量更新，0.3 秒替代 50+ 小时逐条 mutation |
| 调度 | macOS launchd | 每日 09:50 自动执行 |
| 通知 | 即时通讯 API | Schema 2.0 interactive card 格式 |

## 已解决的关键 Bug

| Bug | 现象 | 根因 | 修复方案 |
|-----|------|------|---------|
| INSERT 数据累加 | 12 万行膨胀至 77 万 | 未 DROP TABLE 直接 INSERT | 同步前先 DROP + CREATE |
| 列偏移 | gcd_result 100% NULL | MySQL 23 列 vs CH 24 列 SELECT * | 显式指定 23 列映射 |
| --silent 兼容 | MySQL 输出版本字符串 | MySQL 9.6 --silent + -e 冲突 | 去掉 --silent，2>/dev/null |
| ARG_MAX 超限 | INSERT 报 Argument list too long | 单条 SQL 超 262KB | 分批 200 行 + curl @file 传参 |
| launchd 权限 | shutil.move EPERM | com.apple.provenance 扩展属性 | shutil.copyfile 覆盖替代移动 |
| rejected 逐条更新 | 数千 mutation 串行 50+ 小时 | 逐条 ALTER UPDATE | JOIN 临时表批量 0.3 秒 |

## 可展示成果

- 生产环境稳定运行 15+ 天的同步日志
- 6 个深度 Bug 诊断与修复记录
- 每日同步摘要通知（含总量/被拒分布/Non-organic 占比）
- ClickHouse 97 张日分区表（2026-04-20 ~ 2026-05-06，每日 7-27 万行）

## 在整体架构中的位置

```
数据管道（本项目） → 异常预警系统 → 智能诊断引擎 → 策略测试管理
    ↑                    ↑               ↑               ↑
  每日数据底座      全平台巡检      多Agent并行问诊    A/B测试评估
```

## 下一步计划

- [ ] 数据质量 SLA 指标（同步延迟、丢失率监控）
- [ ] 多渠道告警（目前仅即时通讯）
- [ ] 增量同步模式（当前为全量 DROP+INSERT，可优化为增量 MERGE）
- [ ] Schema 演进自动适配（减少日分区表结构变化的人工干预）
