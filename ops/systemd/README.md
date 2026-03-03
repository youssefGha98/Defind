# Defind systemd units

## 1) Indexer service

```bash
cp /home/youssef/defind/Defind/ops/systemd/defind-indexer.env.example /home/youssef/defind/Defind/ops/systemd/defind-indexer.env
sudo cp /home/youssef/defind/Defind/ops/systemd/defind-indexer.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now defind-indexer.service
```

## 2) Watchdog timer (heartbeat + lag)

```bash
cp /home/youssef/defind/Defind/ops/systemd/defind-watchdog.env.example /home/youssef/defind/Defind/ops/systemd/defind-watchdog.env
sudo cp /home/youssef/defind/Defind/ops/systemd/defind-watchdog.service /etc/systemd/system/
sudo cp /home/youssef/defind/Defind/ops/systemd/defind-watchdog.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now defind-watchdog.timer
```

## 3) Manual health check command

```bash
PYTHONPATH=src /home/youssef/defind/Defind/.venv/bin/python /home/youssef/defind/Defind/scripts/check_indexer_health.py --protocol uniswap --contract usdc_weth --heartbeat-key _meta/heartbeat.json --max-heartbeat-age-s 180 --max-lag-blocks 300
```
