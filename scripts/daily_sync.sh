#!/bin/bash
# 每日数据同步 cron wrapper
# UTC 00:30 = 北京时间 08:30
# 处理前一天数据（无参数默认昨天）

# 从环境变量或 ~/.aws/credentials 读取，请勿硬编码密钥
export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-}"
export AWS_DEFAULT_REGION=ap-southeast-1
export PATH="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"

SCRIPT_DIR="/Users/macmini/.hermes/skills/data-pipeline/s3-daily-sync/scripts"
LOGFILE="$SCRIPT_DIR/daily_sync.log"

exec >> "$LOGFILE" 2>&1

echo "=== $(date '+%Y-%m-%d %H:%M:%S UTC') Cron 开始 ==="

/usr/bin/python3 "$SCRIPT_DIR/daily_sync.py"

echo "=== Cron 结束 ==="
