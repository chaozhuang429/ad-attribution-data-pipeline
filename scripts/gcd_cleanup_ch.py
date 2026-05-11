#!/usr/bin/env python3
"""
AppsFlyer ClickHouse conversion_context gcd_result 清理脚本
- Step 1 纠错: callback=1 且 gcd_result != 'Non-organic' → gcd_result = 'Non-organic'
- Step 2 删除: gcd_result 不在 7 个保留类型内的记录

ClickHouse 特点:
- ALTER UPDATE/DELETE 是异步 mutation，提交后需轮询 system.mutations 等完成
- 支持 CASE WHEN 表达式，可在单条 UPDATE 中完成多个条件判断
- 无事务概念，每个 ALTER 是独立 mutation
"""

import sys, subprocess, time
from urllib.parse import quote

CH_HOST = "http://192.168.0.61:8123"
CH_DB   = "paocai"

# 7 个基础保留类型
KEEP_TYPES = {
    'Non-organic',
    'ai_layer',
    'bayesian_network',
    'fake_install_parameters',
    'device_farms',
    'click_clusters',
    'device_emulators',
}

# 前缀匹配保留（ClickHouse match() 函数正则）
KEEP_PREFIX_PATTERNS = (
    'bots',
    'behavioral_anomalies',
    'validation_',
    'install_hijacking',
    'click_flood',
    'timestamp_anomalies',
    'fake_device',
)


def ch_query(sql, timeout=300):
    url = f"{CH_HOST}/?database={quote(CH_DB, safe='')}"
    data = sql.encode('utf-8')
    r = subprocess.run(
        ['curl', '-s', '-X', 'POST', url, '--data-binary', data],
        capture_output=True, text=True, timeout=timeout
    )
    return r.stdout.strip()


def get_table_list(pattern):
    """获取匹配的表列表"""
    if pattern.endswith('*'):
        prefix = pattern[:-1]
        sql = f"SHOW TABLES FROM {CH_DB} LIKE '{prefix}%'"
    else:
        return [pattern]
    r = ch_query(sql)
    if 'Exception' in str(r):
        return []
    return [l.strip() for l in r.split('\n') if l.strip()]


def analyze(table, label=""):
    """分析 gcd_result 分布"""
    sql = f"""
SELECT gcd_result, count() as cnt
FROM {table}
GROUP BY gcd_result
ORDER BY cnt DESC
FORMAT TabSeparated
"""
    r = ch_query(sql)
    dist = {}
    if r and 'Exception' not in str(r):
        for line in r.split('\n'):
            parts = line.split('\t')
            if len(parts) == 2:
                dist[parts[0]] = int(parts[1])
    return dist


def wait_mutation(table, timeout=300):
    """轮询等待该表的 mutation 全部完成"""
    start = time.time()
    while time.time() - start < timeout:
        r = ch_query(
            f"SELECT count() FROM system.mutations "
            f"WHERE database='{CH_DB}' AND table='{table}' AND is_done=0"
        )
        try:
            pending = int(r.strip())
        except:
            pending = 1
        if pending == 0:
            return True
        time.sleep(2)
    return False


def step1_correct(table):
    """Step 1: 纠错 — callback=1 且 gcd_result != 'Non-organic' → 'Non-organic'"""
    # 先查需纠正的条数
    check_sql = f"""
SELECT count()
FROM {table}
WHERE callback = 1 AND gcd_result != 'Non-organic'
FORMAT TabSeparated
"""
    cnt_r = ch_query(check_sql)
    try:
        rows_to_fix = int(cnt_r.strip())
    except:
        rows_to_fix = 0

    if rows_to_fix == 0:
        print(f"    [SKIP] 无需纠正")
        return 0

    # 用 ALTER UPDATE + CASE WHEN 一次性完成纠正
    update_sql = f"""ALTER TABLE {table} UPDATE
  gcd_result = CASE
    WHEN callback = 1 AND gcd_result != 'Non-organic' THEN 'Non-organic'
    ELSE gcd_result
  END
WHERE callback = 1 AND gcd_result != 'Non-organic'"""
    r = ch_query(update_sql, timeout=300)
    if 'Exception' in str(r):
        print(f"    [ERROR] UPDATE: {r[:120]}")
        return 0

    # 等待 mutation 完成
    wait_mutation(table)
    print(f"    [OK] 纠正了 {rows_to_fix} 条")
    return rows_to_fix


def step2_delete(table):
    """Step 2: 删除 — gcd_result 不在保留类型内的记录"""
    in_clause = ','.join([f"'{t}'" for t in KEEP_TYPES])
    prefix_pattern = '^(' + '|'.join(KEEP_PREFIX_PATTERNS) + ').*'

    # 先查待删除条数（精确 + 前缀两步判断）
    check_sql = f"""
SELECT count()
FROM {table}
WHERE gcd_result NOT IN ({in_clause})
  AND NOT match(gcd_result, '{prefix_pattern}')
FORMAT TabSeparated
"""
    cnt_r = ch_query(check_sql)
    try:
        rows_to_delete = int(cnt_r.strip())
    except:
        rows_to_delete = 0

    if rows_to_delete == 0:
        print(f"    [SKIP] 无需删除")
        return 0

    delete_sql = f"""ALTER TABLE {table} DELETE
WHERE gcd_result NOT IN ({in_clause})
  AND NOT match(gcd_result, '{prefix_pattern}')"""
    r = ch_query(delete_sql, timeout=300)
    if 'Exception' in str(r):
        print(f"    [ERROR] DELETE: {r[:120]}")
        return 0

    # 等待 mutation 完成
    wait_mutation(table)
    print(f"    [OK] 删除了 {rows_to_delete} 条")
    return rows_to_delete


def cleanup_table(table):
    """清理单个表"""
    print(f"\n{'='*60}")
    print(f"处理表: {table}")
    print('='*60)

    print("  [1/4] 清理前分布...")
    before = analyze(table)
    print(f"    {dict(list(before.items())[:10])}")

    print("  [2/4] Step1 纠错 (callback=1 → Non-organic)...")
    step1_correct(table)

    print("  [3/4] Step2 删除 (非保留类型)...")
    deleted = step2_delete(table)

    print("  [4/4] 清理后分布...")
    after = analyze(table)
    print(f"    {dict(list(after.items())[:10])}")

    print(f"\n  ✓ 完成 (删除 {deleted} 条)")
    return {'table': table, 'deleted': deleted, 'before': before, 'after': after}


def main():
    if len(sys.argv) < 2:
        print("用法: python gcd_cleanup_ch.py <table_pattern1> [table_pattern2] ...")
        print("示例:")
        print("  python gcd_cleanup_ch.py conversion_context_20260304")
        print("  python gcd_cleanup_ch.py conversion_context_20260304 conversion_context_20260305")
        print("  python gcd_cleanup_ch.py conversion_context_*")
        sys.exit(1)

    tables = []
    for pattern in sys.argv[1:]:
        tables.extend(get_table_list(pattern))
    tables = list(dict.fromkeys(tables))  # 去重

    if not tables:
        print("未找到匹配的表")
        sys.exit(1)

    print(f"找到 {len(tables)} 个表: {tables}")
    print("保留类型:", KEEP_TYPES)
    print("="*60)

    results = []
    for tbl in tables:
        try:
            res = cleanup_table(tbl)
            results.append(res)
        except Exception as e:
            print(f"  [ERROR] {tbl}: {e}")

    # 汇总
    print("\n" + "="*60)
    print("清理汇总:")
    print("="*60)
    total_deleted = 0
    for r in results:
        print(f"  {r['table']}: 删除 {r['deleted']} 条")
        total_deleted += r['deleted']
    print(f"\n总计删除: {total_deleted} 条 / {len(results)} 表")
    print("="*60)


if __name__ == '__main__':
    main()
