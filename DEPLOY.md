# 息壤 部署指南（三步走）

## 第一步：上传代码

```bash
git clone https://github.com/jihongxing/BreathofEarth.git /opt/xirang
cd /opt/xirang
pip install -r requirements.txt
```

## 第二步：首次运行

先设置生产控制面密钥：

```bash
export XIRANG_ENV=production
export XIRANG_JWT_SECRET="替换为至少32位随机密钥"
```

启动 API 后，在服务器本机初始化第一个管理员：

```bash
python -m uvicorn main:app --host 127.0.0.1 --port 8000
curl -X POST http://127.0.0.1:8000/api/admin/init-user \
  -H "Content-Type: application/json" \
  -d '{"username":"admin","password":"替换为强密码","role":"admin"}'
```

如果必须远程初始化，先设置一次性密钥 `XIRANG_INIT_USER_SECRET`，请求时带 `X-Xirang-Init-Secret` 头。初始化完成后应删除该环境变量。

每日运行前需要先完成券商只读同步与对账：

```bash
python -m runner.broker_sync --portfolio us
```

```bash
python -m runner.daily_runner
```

如果处于 Stage 9.5 影子观察期，不要启动真实交易。改为运行：

```bash
python -m live.shadow_sync --aum 2000000 --no-broker
python -m live.margin_monitor --broker ibkr
```

`shadow_sync` 只生成 90/10 生产候选影子账本与滑点观察。`margin_monitor` 只读抓取券商保证金快照。两者都不允许下单；券商不可达或字段缺失时返回 `WARNING / UNAVAILABLE` 是正确行为。

看到以下输出说明一切正常：

```
✓ 数据校验通过
✓ 数据库已备份
```

首次运行会自动完成：
- 创建 SQLite 数据库（db/xirang.db）
- 初始化 $100,000 虚拟组合
- 拉取最新 ETF 行情并执行第一次状态机更新
- 读取最近一次券商同步与对账记录；缺失时 fail closed，不执行 Core 调仓
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

腾讯云等国内服务器无法直接访问 Yahoo Finance / 东方财富等金融 API。
息壤采用"本地拉数据 → GitHub 中转 → 服务器读取"的数据管道：

```
你的电脑 (akshare 优先，默认禁用 yfinance) → CSV → git push → GitHub → 服务器 git pull → daily_runner
```

数据脚本做了三层防限流：
- 只在交易日线理论上已经可用后才联网，避免 23:05 北京时间去请求仍在交易中的美股日线。
- 疑似 429 / too many requests 后写入本地冷却状态，冷却期内只用本地 raw 缓存兜底。
- 只有 `data/live_us.csv` 或 `data/live_cn.csv` 内容真的变化时，才更新 `data/last_update.txt` 并推送 GitHub。

### 本地电脑：每天拉数据并推送

手动运行：

```bash
cd /你的本地项目目录
python data/daily_fetch.py
```

这会拉取中美两个市场的最新行情，保存为 `data/live_us.csv` 和 `data/live_cn.csv`，然后自动 git push。
美股 ETF 默认使用 Yahoo Adj Close；只有排障时才禁用：

```bash
python data/daily_fetch.py --no-yfinance --force
```

#### Windows 定时任务

PowerShell（管理员）运行：

```powershell
$action = New-ScheduledTaskAction -Execute "python" -Argument "D:\codeSpace\BreathofEarth\data\daily_fetch.py" -WorkingDirectory "D:\codeSpace\BreathofEarth"
$trigger = New-ScheduledTaskTrigger -Weekly -DaysOfWeek Tuesday,Wednesday,Thursday,Friday,Saturday -At 11:05AM
$settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -DontStopIfGoingOnBatteries -AllowStartIfOnBatteries
Register-ScheduledTask -TaskName "XiRang-DailyFetch" -Action $action -Trigger $trigger -Settings $settings -Description "息壤每日数据拉取" -RunLevel Highest
```

说明：
- 周二到周六 11:05 跑一次：北京时间周二 11:05 对应美股周一盘后数据已经可用
- 不再建议 23:05 跑任务：此时美股仍在交易中，容易请求到还不存在的日线并触发外部限流
- `-StartWhenAvailable`：如果到点时电脑没开，开机后会立刻补跑
- 如果行情文件无变化，脚本不会更新 `last_update.txt`，也不会 git push

验证：`Get-ScheduledTask -TaskName "XiRang-DailyFetch"`
手动测试：`Start-ScheduledTask -TaskName "XiRang-DailyFetch"`
删除任务：`Unregister-ScheduledTask -TaskName "XiRang-DailyFetch" -Confirm:$false`

#### macOS / Linux 定时任务

```cron
# 北京时间周二到周六 11:05 跑一次
5 11 * * 2-6 cd /你的项目目录 && python3 data/daily_fetch.py >> /tmp/xirang_fetch.log 2>&1
```

### 服务器：先拉数据再运行

```cron
# 服务器 cron：本地数据推送后再 git pull 并运行
30 11 * * 2-6 cd /opt/xirang && git pull --ff-only -q && /usr/bin/python3 -m runner.daily_runner >> /opt/xirang/logs/cron.log 2>&1
```

服务器的 daily_runner 会自动从本地 CSV 读取数据，不需要访问任何外部 API。

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
