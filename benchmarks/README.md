# 转发基准

`benchmark_forwarder.py` 使用固定的飞书消息夹具和假的 QQ 发送端，不会连接真实账号，也不会向真实 QQ 群发消息。它提供 `legacy`（优化前主链路的图片行为）和 `optimized` 两种模式，用于同机比较。

默认模拟每次图片下载 50ms、每次 QQ 发送 5ms；可通过 `--download-delay-ms` 和 `--qq-delay-ms` 固定或调整模拟网络耗时。

推荐先保存当前版本的基线：

```bash
.venv/bin/python benchmarks/benchmark_forwarder.py \
  --mode legacy --messages 20 --groups 1 --images-every 5 --runs 10 \
  --output baseline-1group.json

.venv/bin/python benchmarks/benchmark_forwarder.py \
  --mode optimized --messages 20 --groups 2 --images-every 5 --runs 10 \
  --output optimized-2groups.json
```

重点比较：

- `elapsed_ms_p95`：批量处理延迟；
- `throughput_messages_per_second_median`：吞吐；
- `image_download_count_per_run`：图片下载次数，主链路每条图片应只下载一次；
- `sent_count_per_run`：应等于消息数乘以目标群数。

保存两份结果后，可用比较器输出百分比变化及门槛判断：

```bash
.venv/bin/python benchmarks/compare_benchmark.py baseline-2groups.json optimized-2groups.json
```

性能优化必须同时满足可靠性测试：消息不丢失、单来源有序、失败不推进游标、重启可恢复。
