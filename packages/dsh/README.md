# @gethen/dsh

DSH 插件装配层：把自研的 `@gethen/master` 挂进 DeepSeek Harness。插件只做装配，
控制面逻辑在 `@gethen/master`（引擎/SQLite/策略/outbox）与 `@gethen/master-runtime`
（Master + L1 ws transport + L2 mailbox 的统一装配）里。

执行面（launch）只有两种形态：

| 形态 | 何时使用 | 说明 |
|---|---|---|
| `fake` | 没配 `ws` 或没有 worker token | 确定性 fake worker，本地跑通 receive → eval → accept → report → receipt；不注册任何 HTTP 路由 |
| `ws` | 配了 `ws` 且有 worker token | 宿主持有 `/gethen/worker` transport 端点，经持久化 mailbox 派发；Worker 出站连接 |

旧的 DSH Remote（`remote`/`devRemote`/`clientService`）路径已删除，不再保留兼容层。

## 用法（fake，本地闭环）

在 DSH profile 的 `cordis.patch.yml` 中插入：

```yaml
- id: gethen
  name: 'file:///home/<user>/.local/share/gethen/dsh/index.js'
  config:
    dir: /absolute/state/dir          # 默认 $GETHEN_STATE_DIR 或 <cwd>/.gethen/master
```

没有 `ws` 配置时插件用 fake launch，工具照常可用（`gethen_run` 会走完整个
ticket → assignment → report → receipt 流程），适合开发与本地验证。

## Mailbox 控制面（单 Worker，C3）

配置 `ws` 且提供 worker token 后，插件用 `@gethen/master-runtime` 装配
`MasterService` + `@gethen/bridge-mailbox`（L2 durable mailbox，sqlite）+
`@gethen/transport`（L1 ws transport）。profile 里有 `webServer` service 时，插件
通过 `ctx.inject(["webServer"], …)` 等它就绪后注册精确路径 `/gethen/worker`
（upgrade 路由），卸载/热替换时 dispose 路由并关闭 runtime。

```yaml
- id: gethen
  name: 'file:///home/<user>/.local/share/gethen/dsh/index.js'
  config:
    dir: /var/lib/gethen
    ws:
      tokensFile: /etc/gethen/workers.json   # 优先；或直接写 tokensJson
      # tokensJson: '[{"workerId":"worker-1","token":"..."}]'
      workerId: worker-1                     # 单 token 时可省略
      workspace: /srv/project                # Worker 侧工作目录，默认 process.cwd()
      packDigest: sha256:local-unpinned      # 可选，派发包 digest 默认值
      mailboxDbPath: /var/lib/gethen/mailbox.sqlite   # 默认 <dir>/mailbox.sqlite
      masterMailbox: mailbox:master          # 默认值，必须是 "<prefix>master"
    escalation: { answer: "rebase onto main" }  # 可选：自动回答 Worker 升级
```

`tokensFile` 是 JSON 数组，每个 Worker 一个独立 Bearer token：

```json
[{ "workerId": "worker-1", "token": "long-random-secret" }]
```

- token 与 `workerId` 绑定；L1 transport 在 upgrade 阶段校验 Bearer token，
  L2 mailbox 用 `mailbox:<workerId>` / `mailbox:master` 两个持久队列传递信封。
- 运行时是单 Worker 模式：多个 token 必须显式给 `ws.workerId`，否则启动报错。
- `tokensFile`/`tokensJson` 为空（或没配 `ws`）→ fake launch，不注册路由。
- 没有 `webServer` service → 打印 warning 并跳过路由注册（不会崩溃）。
- 升级（escalation）：配置 `escalation.answer` 时自动回答；否则 escalate 记录为
  `parked` 等人处理，用 `gethen_escalations` 工具查看。

Worker 侧（出站连接，不监听端口；mailbox 模式）：

```bash
gethen worker --config /etc/gethen/worker.json
# worker.json:
# { "masterUrl": "wss://master.example.com/gethen/worker",
#   "token": "long-random-secret", "workerId": "worker-1",
#   "provider": "...", "model": "...", "workspaces": ["/srv/project"],
#   "comms": "mailbox" }
```

通讯栈固定为 L1 transport + L2 mailbox（C7 起 legacy 协议已删除）；
mailbox 名由 workerId 推导，不需要手动配置。

网络：Master 必须对 Worker 可达。生产用 `wss://`（DSH webServer 配 `tls`，或在
前面放反向代理终止 TLS）；本机联调可以 SSH 隧道，不需要开放 Worker 入站端口：

```bash
ssh -N -L 8443:127.0.0.1:8443 master-host
# Worker 侧 masterUrl 改成 ws://127.0.0.1:8443/gethen/worker（仅隧道内）
```

`registerUpgrade` 是**精确路径**路由，`/gethen/worker` 由本插件独占；重复注册同一
路径会抛错。Worker 断线后 mailbox 消息保留（lease/visibility timeout），重连后按
mailbox 继续，不需要重放 Assignment。

## 提供的工具

| 工具 | 作用 |
|---|---|
| `gethen_status` | `{tickets, open, receipts, running, escalations, launch, workers, mailbox?, tasks?}`；`launch` 为 `fake`/`ws`，`workers` 是当前 transport 会话数，`mailbox` 仅 ws 模式返回 `{pending, leased, dead}`，`tasks` 是任务摘要 `{count, recent}`（仅 mailbox 模式） |
| `gethen_run` | 接收一句话，走 receive → eval → accept → assignment → launch → report → receipt |
| `gethen_overview` | 每张 Ticket 的 `stage`（`ticketStage`）、outcome、latestReport、assignment 数 |
| `gethen_escalations` | Worker 升级列表：question、status（waiting/resolved/parked）、resolution |
| `gethen_task` | 查看/控制一个运行中的 Assignment：`status`（先读缓存，再问 Worker）/ `steer`（追加指令）/ `cancel`（请求停止）；仅 mailbox 模式可用 |

## 任务查看与控制（gethen_task）

mailbox 模式下，`gethen_status` 的 `tasks` 字段给出缓存摘要（`count` + 最近 5 个
assignment 的 phase/step/lastEventAt）。需要细节或干预时用 `gethen_task`：

```jsonc
// status：优先返回缓存的 live state；没有缓存才向 Worker 发 gethen.task.status
{ "action": "status", "assignmentId": "assignment-…", "timeoutMs": 2000 }

// steer：给运行中的 Assignment 追加一条指令（text 非空）
{ "action": "steer", "assignmentId": "assignment-…", "text": "focus on the failing test" }

// cancel：请求 Worker 停止（reason 可选）
{ "action": "cancel", "assignmentId": "assignment-…", "reason": "user asked" }
```

返回：`status` → `{assignmentId, action, state: {phase, step, lastTool?, lastMessage?,
lastEventAt, seq} | null, cached, message?}`；`steer` → `{…, text, steered: true}`；
`cancel` → `{…, cancelled: true, reason?}`。fake 模式没有 Worker，调用会返回明确错误
（`task state/control requires the mailbox runtime…`）；未知/已结束/离线的 assignment
在 status 超时后返回可读的 “did not answer a status request” 错误。

## 边界

- 策略：单操作者，`source: local`，`allowed: ["local-user"]`，`acceptMode: auto`。
- 通讯：C3 单 Worker（一个 runtime 服务一个 `workerId`）；多 Worker 调度在后续
  fleet/router 层演进。
- 面板与 client shell：P6 重做，插件本身不注册 HTTP 页面路由（transport 端点除外）。

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
- 热替换会停掉并重建插件实例（master 与 mailbox 的 SQLite 数据保留，但进行中的
  Assignment 会被打断，Worker 会自动重连 mailbox），建议空闲时重建。

## 本地开发循环（不重启）

推荐组合：

```bash
pnpm dev:dsh ~/.local/share/gethen/dsh   # esbuild watch，保存即重建
```

profile 里保持 `hmr.root` 指向该目录，DSH 会在重建后自动停掉旧入口、加载新入口。
实测：重建 bundle + 改 profile 均无需重启；配置变更（如增删 `escalation`）也会
热生效。注意热替换会重建 master 与 mailbox runtime（SQLite 数据保留，进行中的
Assignment 会被打断），client/UI 变更仍需刷新浏览器。
