#!/usr/bin/env python3
"""
每日数据同步脚本
从 S3 拉取 conversion + rejected 数据，处理后入库 ClickHouse

执行: python3 daily_sync.py [日期YYYYMMDD]
不传参默认昨天
"""
import sys, os, subprocess, json, datetime, time, shutil
import boto3
from botocore.config import Config
from urllib.parse import quote

# ═══════════════════════════════════════
# 路径配置
# ═══════════════════════════════════════
SQL_DIR   = "/Users/macmini/Documents/sql"
REJ_DIR   = "/Users/macmini/Documents/sql/rejected"
TMP_DIR   = "/tmp/daily_sync_tmp"   # 先下载到本地，确认成功后再移入 SSD
os.makedirs(TMP_DIR, exist_ok=True)
os.makedirs(REJ_DIR, exist_ok=True)

# ═══════════════════════════════════════
# AWS 配置（从环境变量读取）
# ═══════════════════════════════════════
S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "https://s3.ap-southeast-1.amazonaws.com")
S3_BUCKET   = os.environ.get("S3_BUCKET",   "phx-adx-adids")
S3_KEY      = "up-file"
AWS_CREDS   = {
    "aws_access_key_id":     os.environ.get("AWS_ACCESS_KEY_ID", ""),
    "aws_secret_access_key": os.environ.get("AWS_SECRET_ACCESS_KEY", ""),
    "region_name":           os.environ.get("AWS_DEFAULT_REGION", "ap-southeast-1"),
}

# ═══════════════════════════════════════
# ClickHouse 配置
# ═══════════════════════════════════════
CH_HOST = "http://192.168.0.61:8123"
CH_DB   = "paocai"

# ═══════════════════════════════════════
# MySQL 配置
# ═══════════════════════════════════════
MYSQL_CREDS  = os.environ.get("MYSQL_CREDS", "root:changeme")
MYSQL_SOCK   = "/tmp/mysql.sock"
MYSQL_TMP_DB = "paocai"

# ═══════════════════════════════════════
# 通知配置
# ═══════════════════════════════════════
FEISHU_TARGET = "feishu:oc_374ff443ed85eb80d99c2fbe7c320aa1"

# ═══════════════════════════════════════
# 基础工具函数
# ═══════════════════════════════════════
def sh(cmd, timeout=300, check=True):
    # MySQL 9.6+ 在 capture_output 时 stderr warning 导致 RC 异常，忽略 stderr
    r = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=timeout)
    if check and r.returncode != 0:
        print(f"  [FAIL] RC={r.returncode}: {r.stderr.strip()[:100]}")
    return r

def ch_query(sql, timeout=180):
    """ClickHouse 查询（使用文件避免 ARG_MAX 限制）"""
    url = f"{CH_HOST}/?database={quote(CH_DB, safe='')}"
    # 超大 SQL 写临时文件，通过 @file 避免命令行参数超限
    tmp_file = f"/tmp/ch_query_{os.getpid()}.sql"
    try:
        with open(tmp_file, 'wb') as f:
            f.write(sql.encode('utf-8'))
        r = subprocess.run(
            ['curl', '-s', '-X', 'POST', url, '--data-binary', f'@{tmp_file}'],
            capture_output=True, text=True, timeout=timeout
        )
        return r.stdout.strip()
    finally:
        if os.path.exists(tmp_file):
            os.remove(tmp_file)

def mysql_cmd(sql, db=MYSQL_TMP_DB, timeout=300, check=True):
    cmd = f"mysql -u{MYSQL_CREDS.split(':')[0]} -p{MYSQL_CREDS.split(':')[1]} -h127.0.0.1 -A {db}"
    r = subprocess.run(f"{cmd} -e {sql!r}", shell=True, capture_output=True, text=True, timeout=timeout)
    if check and r.returncode != 0:
        print(f"  [ERROR] MySQL: {r.stderr.strip()[:200]}")
    return r

def wait_mutation(table, timeout=300):
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

# ═══════════════════════════════════════
# ClickHouse 工具函数
# ═══════════════════════════════════════
def ch_table_exists(table):
    r = ch_query(f"EXISTS TABLE {CH_DB}.{table} FORMAT TabSeparated")
    return r.strip() == '1'

def ch_count(table):
    r = ch_query(f"SELECT count() FROM {CH_DB}.{table} FORMAT TabSeparated")
    try:
        return int(r.strip())
    except:
        return -1

# ═══════════════════════════════════════
# ClickHouse 保留类型（来自 gcd_cleanup_ch.py）
# ═══════════════════════════════════════
KEEP_TYPES = {
    'Non-organic', 'ai_layer', 'bayesian_network', 'fake_install_parameters',
    'device_farms', 'click_clusters', 'device_emulators',
}
KEEP_PREFIX_PATTERNS = (
    'bots', 'behavioral_anomalies', 'validation_',
    'install_hijacking', 'click_flood', 'timestamp_anomalies', 'fake_device',
)
prefix_re = '^(' + '|'.join(KEEP_PREFIX_PATTERNS) + ').*'

def ch_stats(table):
    """返回指定表的统计 dict"""
    sql = f"""
SELECT
    count()                                                  as total,
    sum(gcd_result = 'Non-organic')                          as non_organic,
    sum(gcd_result = 'ai_layer')                             as ai_layer,
    sum(gcd_result = 'bayesian_network')                     as bayesian_network,
    sum(gcd_result = 'device_emulators')                     as device_emulators,
    sum(gcd_result = 'device_farms')                         as device_farms,
    sum(gcd_result = 'click_clusters')                        as click_clusters,
    sum(gcd_result = 'fake_install_parameters')               as fake_install,
    sum(match(gcd_result, '{prefix_re}'))                     as prefix_keep,
    sum(gcd_result NOT IN {tuple(KEEP_TYPES)} AND NOT match(gcd_result, '{prefix_re}')) as other
FROM {CH_DB}.{table}
FORMAT TabSeparated
"""
    r = ch_query(sql)
    parts = r.split('\t')
    if len(parts) < 10:
        return None
    return {
        'total':       int(parts[0]) if parts[0] != '\\N' else 0,
        'non_organic': int(parts[1]) if parts[1] != '\\N' else 0,
        'ai_layer':    int(parts[2]) if parts[2] != '\\N' else 0,
        'bayesian':    int(parts[3]) if parts[3] != '\\N' else 0,
        'emu':         int(parts[4]) if parts[4] != '\\N' else 0,
        'farm':        int(parts[5]) if parts[5] != '\\N' else 0,
        'cluster':     int(parts[6]) if parts[6] != '\\N' else 0,
        'fake':        int(parts[7]) if parts[7] != '\\N' else 0,
        'prefix':      int(parts[8]) if parts[8] != '\\N' else 0,
        'other':       int(parts[9]) if parts[9] != '\\N' else 0,
    }

def build_notify_text(date, stats, steps_ok, steps_fail):
    t = stats
    total = t['total']
    rejected = total - t['non_organic'] - t['prefix']
    rej_rate = rejected / total * 100 if total > 0 else 0

    lines = [
        f"📊 **{date} 数据同步完成**",
        "",
        f"✅ 成功步骤: {steps_ok}",
    ]
    if steps_fail > 0:
        lines.append(f"❌ 失败步骤: {steps_fail}")
    lines += [
        "",
        f"**conversion\\_context\\_{date}:**",
        f"  总记录: {total:,}",
        f"  Non-organic: {t['non_organic']:,}",
        "",
        f"📛 被拒数: **{rejected:,}** (被拒率 {rej_rate:.1f}%)",
    ]
    # 只显示非零被拒类型
    rej_items = [
        ("ai_layer", t['ai_layer']),
        ("bayesian_network", t['bayesian']),
        ("device_emulators", t['emu']),
        ("device_farms", t['farm']),
        ("click_clusters", t['cluster']),
        ("fake_install_parameters", t['fake']),
    ]
    for name, val in rej_items:
        if val > 0:
            lines.append(f"  {name}: {val:,}")
    if t['prefix'] > 0:
        lines.append(f"  bots/behavioral 等: {t['prefix']:,}")
    lines += [
        "",
        "🧹 数据清洗已执行",
    ]
    return '\n'.join(lines)

# ═══════════════════════════════════════
# Step 1: S3 下载
# ═══════════════════════════════════════
def s3_download(date_str):
    """下载 S3 文件，返回 (conversion_local_path, [rejected_local_paths])"""
    # ===== 调试：launchd 环境变量检查 =====

    client = boto3.client('s3',
                           aws_access_key_id=AWS_CREDS['aws_access_key_id'],
                           aws_secret_access_key=AWS_CREDS['aws_secret_access_key'],
                           region_name=AWS_CREDS['region_name'])

    # S3 rejected 文件日期格式是 YYYY-MM-DD
    date_s3 = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"

    results = {}

    # 1) conversion SQL（先下载到 TMP，确认成功后再移入 SSD）
    conv_key = f"{S3_KEY}/mocker-conversion/conversion_context_{date_str}.sql.gz"
    conv_tmp = os.path.join(TMP_DIR, f"conversion_context_{date_str}.sql.gz")
    conv_final = os.path.join(SQL_DIR, f"conversion_context_{date_str}.sql.gz")
    try:
        client.download_file(S3_BUCKET, conv_key, conv_tmp)
        print(f"  [OK] 下载 conversion → {conv_tmp}")
        # launchd 沙箱下 shutil.move/os.replace 报 EPERM，改用 copyfile + unlink
        import shutil as _shutil
        if os.path.exists(conv_final):
            os.unlink(conv_final)
        _shutil.copyfile(conv_tmp, conv_final)
        os.unlink(conv_tmp)
        results['conversion'] = conv_final
        print(f"  [OK] 移入 SSD: {conv_final}")
    except Exception as e:
        print(f"  [ERROR] 下载/移动 conversion 失败: {e}")
        # 清理残留在 TMP 的文件
        if os.path.exists(conv_tmp):
            os.remove(conv_tmp)
        results['conversion'] = None

    # 2) rejected JSONL（同样先写 TMP 再移动）
    rej_pattern = f"{S3_KEY}/rejected/mocker_rejected_"
    paginator = client.get_paginator('list_objects_v2')
    rej_files = []
    try:
        for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=rej_pattern):
            for obj in page.get('Contents', []):
                key = obj['Key']
                if date_s3 in key and key.endswith('.gz'):
                    basename = os.path.basename(key)
                    rej_tmp = os.path.join(TMP_DIR, basename)
                    rej_final = os.path.join(REJ_DIR, basename)
                    client.download_file(S3_BUCKET, key, rej_tmp)
                    try:
                        import shutil as _shutil
                        # launchd 沙箱下 os.unlink 对带 com.apple.provenance 的文件报 EPERM
                        # 改用覆盖写入方式：直接 copyfile 不删原文件（rej_tmp 是独立的 /tmp 文件）
                        _shutil.copyfile(rej_tmp, rej_final)  # 覆盖写入已有文件
                        os.unlink(rej_tmp)
                        print(f"  [OK] 下载 rejected → {rej_final}")
                        rej_files.append(rej_final)
                    except Exception as e:
                        print(f"  [ERROR] 移动 rejected {basename} 失败: {e}")
                        if os.path.exists(rej_tmp):
                            os.remove(rej_tmp)
    except Exception as e:
        print(f"  [ERROR] 遍历 rejected 文件失败: {e}")

    results['rejected'] = rej_files
    return results

# ═══════════════════════════════════════
# Step 2: 处理 conversion SQL
# ═══════════════════════════════════════
def process_conversion_sql(gz_path, date_str):
    """解压 → 导入 MySQL → 同步 ClickHouse → 重新压缩"""
    if not gz_path or not os.path.exists(gz_path):
        print(f"  [SKIP] conversion 文件不存在: {gz_path}")
        return False

    table = f"conversion_context_{date_str}"
    sql_path = gz_path.replace('.gz', '')

    # 2a. 解压
    print(f"  [1/4] 解压 {gz_path} ...")
    sh(f"gzip -dkf '{gz_path}'", check=False)

    # 2b. 导入 MySQL（文件含 DROP/CREATE/INSERT，直接覆盖目标表）
    print(f"  [2/4] 导入 MySQL ({table})...")
    r = sh(f"mysql -uroot -padmin@mysql -h127.0.0.1 {MYSQL_TMP_DB} < '{sql_path}'", timeout=600, check=False)
    if r.returncode != 0:
        print(f"  [ERROR] MySQL 导入失败: {r.stderr.strip()[:200]}")
        return False
    print(f"  [OK] MySQL 导入完成")

    # 2c. 同步 ClickHouse（源表名 = 目标表名）
    ok = sync_mysql_to_ch(table, table, date_str)
    if not ok:
        return False

    # 2d. 重新压缩源文件
    print(f"  [3/4] 重新压缩源文件...")
    sh(f"gzip -f '{sql_path}'", check=False)

    print(f"  [OK] conversion 处理完成: {table}")
    return True

# ═══════════════════════════════════════
# MySQL → ClickHouse 同步（conversion_context 中转）
# ═══════════════════════════════════════
def sync_mysql_to_ch(mysql_table, ch_target_table, date_str):
    """将 MySQL 临时表同步到 ClickHouse 指定表（先 INSERT 后 ALTER UPDATE）"""
    import socket

    # 检测 MySQL 是否有数据
    r = sh(f"mysql -u{MYSQL_CREDS.split(':')[0]} -p{MYSQL_CREDS.split(':')[1]} -h127.0.0.1 {MYSQL_TMP_DB} "
           f"-N -e \"SELECT count(*) FROM {mysql_table}\"")
    try:
        mysql_cnt = int(r.stdout.strip())
    except:
        mysql_cnt = 0

    if mysql_cnt == 0:
        print(f"  [WARN] MySQL 表为空，跳过")
        return True

    # ═══════════════════════════════════════════════════════
    # MySQL → ClickHouse 字段映射（23 个字段全部保留）
    # MySQL 列顺序: tid, platform, ad_source, offer_id, site_id, country, pkg, mmp,
    #               random, gaid, sensor_size, ua, params, callback, gcd_result,
    #               predict_result, gp_click_ts, gp_install_begin_ts, install_ts,
    #               first_launch_date_ts, last_launch_date, gmt_create, gmt_update
    # MySQL 无 ip 列，ClickHouse 的 ip 填 NULL 占位
    # ═══════════════════════════════════════════════════════

    CH_COLUMNS = [
        'tid', 'platform', 'ad_source', 'offer_id', 'site_id', 'country',
        'pkg', 'mmp', 'random', 'gaid', 'sensor_size', 'ua', 'params',
        'callback', 'ip', 'gcd_result', 'predict_result',
        'gp_click_ts', 'gp_install_begin_ts', 'install_ts',
        'first_launch_date_ts', 'last_launch_date', 'gmt_create', 'gmt_update',
    ]
    CH_COLUMNS_STR = ', '.join(CH_COLUMNS) + ', ver'

    # 每次运行前先 DROP 表（避免 INSERT INTO 累加旧数据）
    if ch_table_exists(ch_target_table):
        ch_query(f"DROP TABLE {CH_DB}.{ch_target_table}")
        print(f"  [OK] 删除旧表: {ch_target_table}")

    # 创建新表（23 列完整结构）
    create_sql = f"""
CREATE TABLE IF NOT EXISTS {CH_DB}.{ch_target_table} (
    tid             String,
    platform        Nullable(String),
    ad_source       Nullable(String),
    offer_id        Nullable(String),
    site_id         Nullable(String),
    country         Nullable(String),
    pkg             Nullable(String),
    mmp             Nullable(String),
    random          Nullable(Int8),
    gaid            Nullable(String),
    sensor_size     Nullable(Int16),
    ua              Nullable(String),
    params          Nullable(String),
    callback        Nullable(Int8),
    ip              Nullable(String),
    gcd_result      Nullable(String),
    predict_result  Nullable(String),
    gp_click_ts     Nullable(Int64),
    gp_install_begin_ts Nullable(Int64),
    install_ts      Nullable(Int64),
    first_launch_date_ts Nullable(Int64),
    last_launch_date Nullable(String),
    gmt_create      Nullable(String),
    gmt_update      Nullable(String),
    ver             UInt32  -- ReplacingMergeTree 版本号（批次号）
) ENGINE = ReplacingMergeTree(ver) ORDER BY tid
"""
    ch_query(create_sql)
    print(f"  [OK] 创建 ClickHouse 表: {ch_target_table} (24 列含ver)")

    # 用 chunked INSERT 同步数据（每批 2000 行）
    # 关键：用 explicit column name 替代 SELECT *，确保列顺序与 CH_COLUMNS 一致
    print(f"  [4/4] 同步 ClickHouse ({mysql_cnt} 条, 24 列)...")
    batch = 2000
    offset = 0
    synced = 0

    def _v(cols, idx, is_int=False, is_str=True):
        """从 cols[idx] 提取值，处理 MySQL NULL (\\N) → ClickHouse NULL"""
        if idx >= len(cols):
            return 'NULL'
        v = cols[idx]
        # MySQL -N 输出 NULL 为字面量字符串 "NULL"
        if v in ('\\N', 'NULL'):
            return 'NULL'
        if is_int:
            return v.strip()
        if is_str:
            # String: 单引号转义
            return v.replace("'", "\\'")
        return v

    # 用显式列名查询 MySQL（避免位置偏移错位）
    MYSQL_COLS = [
        'tid', 'platform', 'ad_source', 'offer_id', 'site_id', 'country',
        'pkg', 'mmp', 'random', 'gaid', 'sensor_size', 'ua', 'params',
        'callback', 'gcd_result', 'predict_result',
        'gp_click_ts', 'gp_install_begin_ts', 'install_ts',
        'first_launch_date_ts', 'last_launch_date', 'gmt_create', 'gmt_update',
    ]
    MYSQL_SELECT = ', '.join(MYSQL_COLS) + ', NULL AS ip'  # MySQL 无 ip 列，补 NULL

    while offset < mysql_cnt:
        batch_ver = offset // batch + 1  # 批次号（从1开始）
        # 用文件传递 SQL（避免命令行参数解析问题）
        sql_file = f"/tmp/mysql_query_{os.getpid()}.sql"
        with open(sql_file, 'w') as f:
            f.write(f"SELECT {MYSQL_SELECT} FROM {mysql_table} LIMIT {batch} OFFSET {offset}")
        r = sh(f"mysql -u{MYSQL_CREDS.split(':')[0]} -p{MYSQL_CREDS.split(':')[1]} -h127.0.0.1 {MYSQL_TMP_DB} -N < {sql_file} 2>/dev/null", timeout=600)
        os.remove(sql_file)
        rows = [l for l in r.stdout.split('\n') if l.strip()]
        if not rows:
            break

        values = []
        for row in rows:
            cols = row.split('\t')
            # cols 顺序: tid, platform, ad_source, offer_id, site_id, country, pkg, mmp,
            #            random, gaid, sensor_size, ua, params, callback, gcd_result,
            #            predict_result, gp_click_ts, gp_install_begin_ts, install_ts,
            #            first_launch_date_ts, last_launch_date, gmt_create, gmt_update, ip
            # 共 24 列（23 MySQL 列 + ip=NULL）
            # MySQL -N 输出的 NULL 是字面量字符串 'NULL'
            if len(cols) < 24:
                continue

            def _v(cols, idx, is_int=False):
                if idx >= len(cols):
                    return 'NULL'
                v = cols[idx]
                if v == 'NULL':
                    return 'NULL'
                if is_int:
                    return v.strip()
                return v.replace("'", "\\'")

            tid             = _v(cols, 0)
            platform        = _v(cols, 1)
            ad_source      = _v(cols, 2)
            offer_id       = _v(cols, 3)
            site_id        = _v(cols, 4)
            country        = _v(cols, 5)
            pkg            = _v(cols, 6)
            mmp            = _v(cols, 7)
            random         = _v(cols, 8,  is_int=True)
            gaid           = _v(cols, 9)
            sensor_size    = _v(cols, 10, is_int=True)
            ua             = _v(cols, 11)
            params         = _v(cols, 12)
            callback       = _v(cols, 13, is_int=True)
            gcd_result    = _v(cols, 14)
            predict_result = _v(cols, 15)
            gp_click_ts   = _v(cols, 16, is_int=True)
            gp_install    = _v(cols, 17, is_int=True)
            install_ts    = _v(cols, 18, is_int=True)
            first_launch  = _v(cols, 19, is_int=True)
            last_launch   = _v(cols, 20)
            gmt_create    = _v(cols, 21)
            gmt_update    = _v(cols, 22)
            ip_val        = _v(cols, 23)   # NULL（MySQL 无此列）

            def _sq(v):
                if v == 'NULL':
                    return v
                return f"'{v}'"

            values.append(
                f"({_sq(tid)},{_sq(platform)},{_sq(ad_source)},{_sq(offer_id)},"
                f"{_sq(site_id)},{_sq(country)},{_sq(pkg)},{_sq(mmp)},"
                f"{random},{_sq(gaid)},{sensor_size},{_sq(ua)},{_sq(params)},"
                f"{callback},{ip_val},{_sq(gcd_result)},{_sq(predict_result)},"
                f"{gp_click_ts},{gp_install},{install_ts},{first_launch},"
                f"{_sq(last_launch)},{_sq(gmt_create)},{_sq(gmt_update)},{batch_ver})"
            )

        if values:
            # 大批量 JSON 字段会导致单条 SQL 超过 curl ARG_MAX，分批 INSERT
            INSERT_BATCH = 200  # 每批 200 行，避免 SQL 文本过长
            for i in range(0, len(values), INSERT_BATCH):
                chunk = values[i:i+INSERT_BATCH]
                insert_sql = f"INSERT INTO {CH_DB}.{ch_target_table} ({CH_COLUMNS_STR}) VALUES {','.join(chunk)}"
                result = ch_query(insert_sql, timeout=300)
                if result:
                    import sys
                    print(f"  [WARN] INSERT err: {result[:200]}", file=sys.stderr)
        synced += len(values)

        offset += batch

    print(f"  [OK] ClickHouse 同步完成: {synced} 条 (24 列含ver)")
    return True

# ═══════════════════════════════════════
# Step 3: 处理 rejected JSONL
# ═══════════════════════════════════════
import base64, struct, json, datetime

def confusion_decode(uuid):
    PREFIX = "px"
    if not uuid.startswith(PREFIX):
        return uuid
    uuid = uuid[len(PREFIX)+1:][::-1]
    result = []
    for c in uuid:
        o = ord(c)
        if 'a' < c <= 'z':   result.append(chr(o - 33))
        elif c == 'a':        result.append('Z')
        elif 'A' < c <= 'Z':  result.append(chr(o + 31))
        elif c == 'A':        result.append('z')
        elif '0' < c <= '9':  result.append(chr(o - 1))
        elif c == '0':        result.append('9')
        else:                 result.append(c)
    return ''.join(result)

def decrypt_clickid(clickid):
    try:
        decoded = confusion_decode(clickid)
        content = base64.urlsafe_b64decode(decoded + '==')
    except:
        return None
    buf = bytearray(content)
    if len(buf) < 27:
        return None
    version = buf[0]
    platform = buf[11]
    ts = struct.unpack('>i', bytes(buf[12:16]))[0]
    ext_bytes = bytes(buf[26:])
    ext = {}
    if version > 1 and ext_bytes:
        try:
            ext = json.loads(ext_bytes.decode('utf-8'))
        except:
            pass
    return {
        'platform': platform,
        'ts': ts,
        'ts_date': datetime.datetime.fromtimestamp(ts).strftime('%Y%m%d') if ts > 0 else None,
        'ext': ext
    }

def build_rejected_reason(rec):
    """从 JSONL 记录重组被拒原因字符串"""
    parts = []
    for field in ['blocked_reason', 'blocked_reason_value', 'blocked_sub_reason',
                  'rejected_reason', 'rejected_reason_value', 'rejected_sub_value']:
        v = rec.get(field)
        if v and str(v).strip():
            parts.append(str(v).strip())
    return '/'.join(parts) if parts else 'unknown'

def e(s):
    """单引号转义"""
    return str(s).replace("'", "\\'")

def process_rejected_jsonl(gz_paths, date_str, table):
    """
    解压 JSONL → 逐行解密 clickid → 批量 UPDATE ClickHouse（JOIN 方式，秒级完成）
    """
    if not gz_paths:
        print(f"  [SKIP] 无 rejected 文件")
        return True

    all_records = {}

    for gz_path in gz_paths:
        jsonl_path = gz_path.replace('.gz', '')
        print(f"  [1/4] 解压 {gz_path} ...")
        sh(f"gzip -dkf '{gz_path}'", check=False)

        if not os.path.exists(jsonl_path):
            continue

        print(f"  [2/4] 解析 {jsonl_path} ...")
        with open(jsonl_path, 'r', encoding='utf-8', errors='ignore') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                except:
                    continue

                clickid = rec.get('clickid') or rec.get('click_id') or ''
                if not clickid:
                    continue

                dec = decrypt_clickid(clickid)
                if not dec or not dec.get('ts_date'):
                    continue

                ts_date = dec['ts_date']
                if ts_date != date_str:
                    continue

                ext = dec.get('ext', {})
                tid = ext.get('aff_sub', '')
                subid = ext.get('subid', '')
                ip = ext.get('ip', '') or rec.get('ip', '') or ''

                if not tid:
                    continue

                # 取 blocked_reason 作为 gcd_result（干净的单级类型）
                # 不再拼接多级，避免出现 'bots/store_validation/bots/...' 这类拼接字符串
                gcd = rec.get('blocked_reason', '') or 'unknown'

                # 保留第一个匹配（去重）
                if tid not in all_records:
                    all_records[tid] = {
                        'subid': subid, 'ip': ip, 'gcd': gcd, 'date': ts_date,
                        'callback': rec.get('callback')
                    }

        sh(f"gzip -f '{jsonl_path}'", check=False)

    if not all_records:
        print(f"  [WARN] 无有效 rejected 记录（date={date_str}）")
        return True

    print(f"  [3/4] 批量应用更新（{len(all_records)} 条唯一 tid）...")

    # 用临时表 + JOIN 方式批量更新（避免逐条 mutation）
    import uuid as _uuid
    tmp_tbl = f"{CH_DB}.rej_tmp_{date_str.replace('-','')}_{_uuid.uuid4().hex[:6]}"
    try:
        ch_query(f"DROP TABLE IF EXISTS {tmp_tbl}")
        # rejected 临时表：只存 rejected 知道的 4 个字段（+ tid 作为 JOIN key）
        ch_query(f"""CREATE TABLE {tmp_tbl} (
            tid String,
            site_id Nullable(String),
            callback Nullable(Int8),
            gcd_result Nullable(String),
            ip Nullable(String)
        ) ENGINE = MergeTree() ORDER BY tid""")

        BATCH = 1000
        tids = list(all_records.keys())
        for i in range(0, len(tids), BATCH):
            batch = tids[i:i+BATCH]
            vals = []
            for t in batch:
                subid = all_records[t]['subid']
                ip_v  = all_records[t]['ip']
                gcd_v = all_records[t]['gcd']
                cb_v  = all_records[t].get('callback', 'NULL')
                s_q = f"'{e(subid)}'" if subid else 'NULL'
                i_q = f"'{e(ip_v)}'"  if ip_v  else 'NULL'
                g_q = f"'{e(gcd_v)}'" if gcd_v else 'NULL'
                vals.append(f"('{e(t)}',{s_q},{cb_v},{g_q},{i_q})")
            ch_query(f"INSERT INTO {tmp_tbl} (tid,site_id,callback,gcd_result,ip) VALUES {','.join(vals)}")

        # 完整 23 列的临时表（JOIN 结果）
        new_tbl = f"{CH_DB}.{table}_rj"
        all_ch_cols = (
            'tid', 'platform', 'ad_source', 'offer_id', 'site_id', 'country',
            'pkg', 'mmp', 'random', 'gaid', 'sensor_size', 'ua', 'params',
            'callback', 'ip', 'gcd_result', 'predict_result',
            'gp_click_ts', 'gp_install_begin_ts', 'install_ts',
            'first_launch_date_ts', 'last_launch_date', 'gmt_create', 'gmt_update'
        )
        ch_query(f"DROP TABLE IF EXISTS {new_tbl}")
        ch_query(f"""CREATE TABLE {new_tbl} (
            tid             String,
            platform        Nullable(String),
            ad_source       Nullable(String),
            offer_id        Nullable(String),
            site_id         Nullable(String),
            country         Nullable(String),
            pkg             Nullable(String),
            mmp             Nullable(String),
            random          Nullable(Int8),
            gaid            Nullable(String),
            sensor_size     Nullable(Int16),
            ua              Nullable(String),
            params          Nullable(String),
            callback        Nullable(Int8),
            ip              Nullable(String),
            gcd_result      Nullable(String),
            predict_result  Nullable(String),
            gp_click_ts     Nullable(Int64),
            gp_install_begin_ts Nullable(Int64),
            install_ts      Nullable(Int64),
            first_launch_date_ts Nullable(Int64),
            last_launch_date Nullable(String),
            gmt_create      Nullable(String),
            gmt_update      Nullable(String)
        ) ENGINE = MergeTree() ORDER BY tid""")

        # JOIN：rejected 只更新 site_id/callback/gcd_result/ip 四个字段，其余 19 列保留 conversion 原值
        # ip 来自 rejected 的 ip 字段（从 clickid ext 解出的 IP）
        r = ch_query(f"""INSERT INTO {new_tbl}
SELECT
    t.tid,
    t.platform,
    t.ad_source,
    t.offer_id,
    ifNull(u.site_id,    t.site_id)    AS site_id,
    t.country,
    t.pkg,
    t.mmp,
    t.random,
    t.gaid,
    t.sensor_size,
    t.ua,
    t.params,
    ifNull(u.callback,    t.callback)   AS callback,
    ifNull(u.ip,         t.ip)         AS ip,
    ifNull(u.gcd_result,  t.gcd_result) AS gcd_result,
    t.predict_result,
    t.gp_click_ts,
    t.gp_install_begin_ts,
    t.install_ts,
    t.first_launch_date_ts,
    t.last_launch_date,
    t.gmt_create,
    t.gmt_update
FROM {CH_DB}.{table} AS t
LEFT JOIN {tmp_tbl} AS u ON t.tid = u.tid""", timeout=300)

        # 原子替换
        ch_query(f"EXCHANGE TABLES {CH_DB}.{table} AND {new_tbl}")
        updated = ch_query(f"SELECT count() FROM {tmp_tbl} FORMAT TabSeparated")
        print(f"  [OK] rejected 处理完成，{updated} 条已应用")
    finally:
        ch_query(f"DROP TABLE IF EXISTS {tmp_tbl}")

    return True

# ═══════════════════════════════════════
# Step 4: 数据清洗
# ═══════════════════════════════════════
def run_cleanup(date_str):
    """执行 conversion_context_clean"""
    import sys as _sys
    skill_dir = os.path.expanduser("~/.hermes/skills/conversion-context-clean")
    _sys.path.insert(0, skill_dir)
    from gcd_cleanup_ch import step1_correct, step2_delete, analyze
    from gcd_cleanup_ch import wait_mutation as _wm

    table = f"conversion_context_{date_str}"
    print(f"\n  [Step4] 数据清洗: {table}")

    before = analyze(table)
    print(f"  清洗前: {dict(list(before.items())[:8])}")

    n1 = step1_correct(table)
    n2 = step2_delete(table)

    after = analyze(table)
    print(f"  清洗后: {dict(list(after.items())[:8])}")
    print(f"  [OK] 清洗完成（纠错 {n1} 条，删除 {n2} 条）")
    return True

# ═══════════════════════════════════════
# 发送飞书通知
# ═══════════════════════════════════════
FEISHU_APP_ID     = os.environ.get("FEISHU_APP_ID", "")
FEISHU_APP_SECRET = os.environ.get("FEISHU_APP_SECRET", "")

def send_feishu(markdown_text):
    """通过 schema 2.0 interactive card 发送 markdown（支持表格）"""
    import urllib.request, urllib.parse, urllib.error

    # 1. 获取 tenant_access_token
    req = urllib.request.Request(
        "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal",
        data=urllib.parse.urlencode({"app_id": FEISHU_APP_ID, "app_secret": FEISHU_APP_SECRET}).encode(),
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST"
    )
    try:
        resp = urllib.request.urlopen(req, timeout=10)
        token_data = json.loads(resp.read())
        token = token_data.get("tenant_access_token", "")
    except Exception as e:
        print(f"    [ERROR] 获取飞书 token 失败: {e}")
        return False

    if not token:
        print(f"    [ERROR] 飞书 token 为空")
        return False

    # 2. 构建 schema 2.0 interactive card
    card = {
        "schema": "2.0",
        "config": {"wide_screen_mode": True},
        "body": {
            "elements": [{"tag": "markdown", "content": markdown_text}]
        }
    }
    msg_body = json.dumps({
        "receive_id": "ou_62828fc1d02e7e0cba506563bf1a90a5",
        "msg_type": "interactive",
        "content": json.dumps(card)
    }).encode('utf-8')

    msg_req = urllib.request.Request(
        "https://open.feishu.cn/open-apis/im/v1/messages?receive_id_type=open_id",
        data=msg_body,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}"
        },
        method="POST"
    )
    try:
        resp2 = urllib.request.urlopen(msg_req, timeout=10)
        result = json.loads(resp2.read())
        if result.get("code") == 0:
            print(f"    [OK] 飞书消息已发送")
            return True
        else:
            print(f"    [ERROR] 飞书发送失败: code={result.get('code')} msg={result.get('msg')}")
            return False
    except urllib.error.HTTPError as e:
        body = e.read().decode('utf-8', errors='ignore')
        print(f"    [ERROR] 飞书 HTTP {e.code}: {body[:300]}")
        return False
    except Exception as e:
        print(f"    [ERROR] 飞书请求失败: {e}")
        return False

# ═══════════════════════════════════════
# 主流程
# ═══════════════════════════════════════
def main():
    # 确定处理日期
    if len(sys.argv) >= 2:
        raw = sys.argv[1]
        if len(raw) == 4 and raw.isdigit():
            # MMDD 格式，补今年
            date_str = datetime.date.today().strftime('%Y') + raw
        elif len(raw) == 8 and raw.isdigit():
            date_str = raw
        else:
            date_str = raw
    else:
        yesterday = datetime.date.today() - datetime.timedelta(days=1)
        date_str = yesterday.strftime('%Y%m%d')

    print("=" * 60)
    print(f"每日数据同步 — {date_str}")
    print("=" * 60)

    steps_ok = 0
    steps_fail = 0

    # Step 1: S3 下载（必须真正拿到文件才算成功）
    print(f"\n[Step1] S3 下载...")
    files = s3_download(date_str)
    conv_file = files.get('conversion')
    rej_files = files.get('rejected', [])
    s3_ok = bool(conv_file or rej_files)  # 至少有一个文件才说明下载成功
    if s3_ok:
        steps_ok += 1
        print(f"  [OK] S3 下载成功（conversion={'有' if conv_file else '无'}, rejected={len(rej_files)}个）")
    else:
        steps_fail += 1
        print(f"  [FAIL] S3 下载失败，无任何文件")

    # Step 2: conversion SQL（依赖 Step1 文件）
    print(f"\n[Step2] 处理 conversion SQL...")
    conv_ok = process_conversion_sql(conv_file, date_str) if s3_ok else False
    if conv_ok:
        steps_ok += 1
    else:
        steps_fail += 1

    # Step 3: rejected JSONL（依赖 Step1 文件）
    table = f"conversion_context_{date_str}"
    print(f"\n[Step3] 处理 rejected JSONL...")
    rej_ok = process_rejected_jsonl(rej_files, date_str, table) if s3_ok else False
    if rej_ok:
        steps_ok += 1
    else:
        steps_fail += 1

    # Step 4: 数据清洗
    print(f"\n[Step4] 数据清洗...")
    try:
        run_cleanup(date_str)
        steps_ok += 1
    except Exception as e:
        print(f"  [ERROR] 清洗失败: {e}")
        steps_fail += 1

    # 统计
    table = f"conversion_context_{date_str}"
    print(f"\n[统计] 读取 {table} ...")
    stats = ch_stats(table) if ch_table_exists(table) else None

    # 发送通知
    print(f"\n[通知] 发送飞书消息...")
    if stats and stats.get('total', 0) > 0:
        msg = build_notify_text(date_str, stats, steps_ok, steps_fail)
    elif not s3_ok:
        msg = (f"**{date_str}** 数据同步失败\n\n"
               f"❌ 失败步骤:\n"
               f"  Step1 S3下载: 文件写入被拒（Operation not permitted）\n"
               f"  Step2 conversion: 跳过（无文件）\n"
               f"  Step3 rejected: 跳过（无文件）\n"
               f"  Step4 数据清洗: 跳过（无数据）\n\n"
               f"✅ 实际成功: 0/4\n"
               f"❌ 实际失败: 4/4\n\n"
               f"请检查: /Users/macmini/Documents/sql 写入权限")
    elif stats is None:
        msg = (f"**{date_str}** 数据同步完成（步骤成功 {steps_ok}，失败 {steps_fail}）\n\n"
               f"⚠️ 表 conversion_context_{date_str} 不存在，无统计数据")
    else:
        msg = (f"**{date_str}** 数据同步完成（步骤成功 {steps_ok}，失败 {steps_fail}）\n\n"
               f"⚠️ 当日无 conversion 数据（可能是非工作日或 S3 文件未就绪）")

    send_feishu(msg)
    print(f"\n{'='*60}")
    print(f"完成！steps_ok={steps_ok} steps_fail={steps_fail}")
    print(f"{'='*60}")

if __name__ == '__main__':
    main()
