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

## 开发热重载

`pnpm build:dsh` 改的是 `~/.local/share/gethen/dsh/` 下的构建产物，DSH 默认不监听
插件源码目录（`hmr.root` 为 `[]`），所以重建后不会自动替换。在 profile 的
`cordis.patch.yml` 里加：

```yaml
- id: hmr
  disabled: false
  config:
    root: ["/home/secbone/.local/share/gethen/dsh"]
```

之后重建 bundle，HMR 会在文件写入稳定（约 2s）后停掉旧入口、导入新入口。
实测：编辑被监听目录里的模块，约 2–4 秒内完成替换。

注意：

- 只改 `cordis.patch.yml`（增删/改条目）本来就会热重组，不需要重启；
- 不配 `root` 时，即使手动删掉再重新加回 entry，Node 的 ESM 模块缓存也可能让旧
  代码继续生效（实测过），所以重建产物不能靠"卸载再加载"；
- 热替换会停掉并重建插件实例（master 的 SQLite 数据保留，但进行中的 Assignment
  会被打断），建议空闲时重建，或先跑测试再重建。

## 本地开发循环（不重启）

推荐组合：

```bash
pnpm dev:dsh ~/.local/share/gethen/dsh   # esbuild watch，保存即重建
```

profile 里保持 `hmr.root` 指向该目录，DSH 会在重建后自动停掉旧入口、加载新入口。
需要模拟远端 Transport 时，给 `gethen` 配置 `devRemote: true`（脚本化 Worker：
升级 → 答复 → 报告，按 session 重放），不再需要单独的服务插件条目：

```yaml
- id: gethen
  config:
    devRemote: true
    remote: { hostId: "dev-local", workspaceId: "ws-dev" }
    escalation: { answer: "rebase onto main" }   # 删掉这行 = parked 等人
```

实测：重建 bundle + 改 profile 均无需重启；配置变更（如增删 `escalation`）也会
热生效。注意热替换会重建 master 服务（SQLite 数据保留，进行中的 Assignment 会被
打断），client/UI 变更仍需刷新浏览器。
