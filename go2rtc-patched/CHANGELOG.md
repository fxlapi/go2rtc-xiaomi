# 更新日志

## 1.9.15-p1（2026-11-18）

基于 go2rtc v1.9.15：

- PR #2575：登录后持续排空 CS2 命令队列，修复「pop buffer is full」卡顿（原版备份 go2rtc.orig-1.9.15）。
- CW501D 双镜头单连接合并：
  - session pool 单连接复用 + StartMediaDual 双画质命令（社区 commit b713d79）；
  - 修复副镜头 channel=2 映射（producer.go：channel 1/2 均映射到内部通道 1）；
  - CW501D 通道解析改按包头 flags 位 0x01400000 判断（client.go），hdr[28] 对该机型无效；
  - HDRDBG 诊断日志默认关闭，设环境变量 GO2RTC_DEBUG_HDR=1 开启。
- 实测：双流同拉仅 1 条 TCP 连接，两路画面独立（抓帧对比确认）。
