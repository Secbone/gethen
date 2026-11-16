# @gethen/dsh

DSH 插件装配层（P1 最小版）。把自研的 `@gethen/master` 挂进 DeepSeek Harness：
一个 SQLite 控制面 + 三个工具，执行面暂时是确定性的 fake worker。

## 用法

在 DSH profile 的 `cordis.patch.yml` 中插入：

```yaml
- id: gethen
  name: 'file:///home/<user>/.local/share/gethen/dsh/index.js'
  config:
    dir: /absolute/state/dir          # 默认 $GETHEN_STATE_DIR 或 <cwd>/.gethen/master
```

## 提供的工具

| 工具 | 作用 |
|---|---|
| `gethen_status` | `{tickets, open, receipts, running}` |
| `gethen_run` | 接收一句话，走 receive → eval → accept → assignment → fake report → receipt |
| `gethen_overview` | 每张 Ticket 的 `stage`（`ticketStage`）、outcome、latestReport、assignment 数 |

## P1 边界

- 策略：单操作者，`source: local`，`allowed: ["local-user"]`，`acceptMode: auto`。
- 执行：`createFakeLaunch()`；P3 换成远端 Worker，master 不感知。
- 面板与 client shell：P6 重做，当前不注册 HTTP 路由。
