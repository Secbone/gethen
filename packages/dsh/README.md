# @gethen/dsh

DSH 插件装配层：把自研的 `@gethen/master` 挂进 DeepSeek Harness。插件只做装配，
控制面逻辑在 `@gethen/master`（引擎/SQLite/策略/outbox）与 `@gethen/master-runtime`
（Master + Worker 网关 + WS launch 的统一装配）里。

执行面（launch）有三种形态：

| 形态 | 何时使用 | 说明 |
|---|---|---|
| `fake` | 默认 | `createFakeLaunch()`：确定性 worker，本地跑通 receive → eval → accept → report → receipt |
| `remote` | 配了 `remote` / `devRemote` | 通过 DSH Remote client 在远端 DSH session 里执行（P3） |
| `ws` | 配了 `ws` | 插件自己托管 `/gethen/worker`，Worker 出站连接并执行派发（R5，实验） |

## 用法（fake，本地闭环）

在 DSH profile 的 `cordis.patch.yml` 中插入：

```yaml
- id: gethen
  name: 'file:///home/<user>/.local/share/gethen/dsh/index.js'
  config:
    dir: /absolute/state/dir          # 默认 $GETHEN_STATE_DIR 或 <cwd>/.gethen/master
```

## WS 控制面（R5，实验）

配置 `ws` 后，插件用 `@gethen/master-runtime` 装配 `MasterService` +
`WorkerGateway` + `WsLaunchPort`。profile 里有 `webServer` service 时，插件通过
`webServer.registerUpgrade` 注册精确路径 `/gethen/worker`（`gethen.worker.v1`）；
插件卸载/热替换时 dispose 该路由并关闭 runtime。

```yaml
- id: gethen
  name: 'file:///home/<user>/.local/share/gethen/dsh/index.js'
  config:
    dir: /var/lib/gethen
    ws:
      tokensFile: /etc/gethen/workers.json   # 优先；或直接写 tokensJson
      # tokensJson: '[{"workerId":"worker-1","token":"..."}]'
      workspace: /srv/project                # Worker 侧工作目录，默认 process.cwd()
      packDigest: sha256:local-unpinned      # 可选，派发包 digest 的默认值
    # escalation: { answer: "rebase onto main" }  # 可选：自动回答 Worker 升级
```

`tokensFile` 是一个 JSON 数组，每个 Worker 一个独立 Bearer token：

```json
[{ "workerId": "worker-1", "token": "long-random-secret" }]
```

- 空 token 列表（`[]` 或未配置）自动回退 fake launch，不会暴露 worker 端点。
- 没有 `webServer` service 的环境会打印 warning 并跳过路由注册（不会崩溃）。
- 每个 token 与 `workerId` 绑定；WS 网关在 upgrade 阶段校验 Bearer token。
- 升级（escalation）：配置 `escalation.answer` 时自动回答；否则 escalate 记录
  `parked` 等人处理，用 `gethen_escalations` 工具查看。

Worker 侧（出站连接，不监听端口）：

```bash
gethen worker --config /etc/gethen/worker.json
# worker.json:
# { "masterUrl": "wss://master.example.com/gethen/worker",
#   "token": "long-random-secret", "workerId": "worker-1",
#   "provider": "...", "model": "...", "workspaces": ["/srv/project"] }
```

网络：Master 必须对 Worker 可达。生产用 `wss://`（DSH webServer 配 `tls`，或在
前面放反向代理终止 TLS）；本机联调可以 SSH 隧道，不需要开放 Worker 入站端口：

```bash
ssh -N -L 8443:127.0.0.1:8443 master-host
# Worker 侧 masterUrl 改成 ws://127.0.0.1:8443/gethen/worker（仅隧道内）
```

注意：`registerUpgrade` 是**精确路径**路由，重复注册同一路径会抛错；`/gethen/worker`
由本插件独占。Worker 断线后 Master 按 `assignmentId` 对账，不重放进行中的 Assignment。

## 提供的工具

| 工具 | 作用 |
|---|---|
| `gethen_status` | `{tickets, open, receipts, running, escalations, launch, workers}`；`launch` 为 `fake`/`remote`/`ws`，`workers` 是当前连接的 Worker 数 |
| `gethen_run` | 接收一句话，走 receive → eval → accept → assignment → launch → report → receipt |
| `gethen_overview` | 每张 Ticket 的 `stage`（`ticketStage`）、outcome、latestReport、assignment 数 |
| `gethen_escalations` | Worker 升级列表：question、status（waiting/resolved/parked）、resolution |

## Remote 回退（P3）

不配 `ws` 时保留原有行为：`config.remote` 指定 DSH Remote host/workspace，或
`devRemote: true` 使用脚本化 Worker 做本地联调：

```yaml
- id: gethen
  config:
    devRemote: true
    remote: { hostId: "dev-local", workspaceId: "ws-dev" }
    escalation: { answer: "rebase onto main" }   # 删掉这行 = parked 等人
```

## 边界

- 策略：单操作者，`source: local`，`allowed: ["local-user"]`，`acceptMode: auto`。
- 面板与 client shell：P6 重做，插件本身不注册 HTTP 页面路由（WS 端点除外）。
- Worker 身份/配额/工作区选择目前由 `master-runtime` 的 worker snapshots 提供；
  多 Worker 调度与租约在 fleet/router 层演进。

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
  会被打断，WS 连接会断开并由 Worker 自动重连），建议空闲时重建。

## 本地开发循环（不重启）

推荐组合：

```bash
pnpm dev:dsh ~/.local/share/gethen/dsh   # esbuild watch，保存即重建
```

profile 里保持 `hmr.root` 指向该目录，DSH 会在重建后自动停掉旧入口、加载新入口。
实测：重建 bundle + 改 profile 均无需重启；配置变更（如增删 `escalation`）也会
热生效。注意热替换会重建 master 服务（SQLite 数据保留，进行中的 Assignment 会被
打断），client/UI 变更仍需刷新浏览器。
