# 息壤 部署指南（三步走）

## 第一步：上传代码

```bash
git clone https://github.com/jihongxing/BreathofEarth.git /opt/xirang
cd /opt/xirang
pip install -r requirements.txt
```

## 第二步：首次运行

```bash
python -m runner.daily_runner
```

看到以下输出说明一切正常：

```
✓ 数据校验通过
✓ 数据库已备份
```

首次运行会自动完成：
- 创建 SQLite 数据库（db/xirang.db）
- 初始化 $100,000 虚拟组合
- 拉取最新 ETF 行情并执行第一次状态机更新
- 备份数据库到 db/backups/
- 写入运行日志到 logs/xirang.log

## 第三步：配置 cron

```bash
crontab -e
```

服务器时区为 UTC（美东收盘 17:00 = UTC 22:00）：

```cron
0 22 * * 1-5 cd /opt/xirang && /usr/bin/python3 -m runner.daily_runner >> /opt/xirang/logs/cron.log 2>&1
```

服务器时区为北京时间 UTC+8（美东收盘 17:00 = 北京次日 06:00）：

```cron
0 6 * * 2-6 cd /opt/xirang && /usr/bin/python3 -m runner.daily_runner >> /opt/xirang/logs/cron.log 2>&1
```

注意：北京时间用周二到周六，因为北京周二 06:00 = 美东周一 17:00。

## 中国大陆服务器注意事项

Yahoo Finance 对中国大陆 IP 有限流。系统内置了双数据源自动切换：
- 优先使用 yfinance（Yahoo Finance）
- 被限流时自动切换到 akshare（国内数据源，通过新浪/东方财富获取美股数据）

如果首次运行遇到 `YFRateLimitError`，直接强制使用 akshare：

```bash
echo "XIRANG_DATA_SOURCE=akshare" >> /opt/xirang/.env
source /opt/xirang/.env && export XIRANG_DATA_SOURCE
python3 -m runner.daily_runner
```

cron 中也要加上环境变量：

```cron
0 6 * * 2-6 cd /opt/xirang && XIRANG_DATA_SOURCE=akshare /usr/bin/python3 -m runner.daily_runner >> /opt/xirang/logs/cron.log 2>&1
```

## 配置通知（可选）

```bash
cat >> /opt/xirang/.env << 'EOF'
TELEGRAM_BOT_TOKEN=你的token
TELEGRAM_CHAT_ID=你的chat_id
EOF
```

支持 Telegram / 企业微信 / 飞书 / 钉钉，详见 engine/notifier.py。

不配置也没关系，系统照常运行，只是不会推送通知。

## 三个月后回来看结果

```bash
cd /opt/xirang
python -m runner.report
```

查看最近 30 天：

```bash
python -m runner.report --days 30
```

## 日常运维命令

```bash
python -m runner.report              # 查看完整报告
python -m runner.report --days 90    # 最近 90 天
python -m runner.daily_runner        # 手动运行一次
python -m runner.daily_runner --force  # 强制重跑今天
tail -f logs/xirang.log              # 查看运行日志
tail -f logs/cron.log                # 查看 cron 日志
```

## 自愈机制

不需要你操心的事情：

- 幂等性：同一天重复运行自动跳过
- 重试：数据拉取失败自动重试 3 次（间隔 60 秒）
- 备份：每次成功运行后自动备份数据库（保留 30 天）
- 失败隔离：某天失败不影响次日运行
- 服务器重启：cron 自动恢复，无需任何手动操作
