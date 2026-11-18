# Workstream 工作区隔离与交付契约

日期：2026-11-18。状态：已实现（`feat/ws-collab`）。

## 问题

多 Worker 已经能注册、路由和排队，但派工仍把 workspace 当成一条路径：Master 默认下发自己的
`process.cwd()`，同机 Worker 会直接写 Master 的仓库。没有 per-workstream 工作区、没有交付
分支约定，队列/复用也没有指标，Master 无法判断容量。

## 契约

1. **Master 只给逻辑 workstream id**。`workspace` 是 `ws-collab` 这样的 id，不是
   `/srv/...` 路径。Master 不再推断 Worker 的文件系统；默认值为 `default`。
2. **Worker 拥有工作区**。Worker 配置 `workstreams[]`（`repo` + 可选 `root/base/remote/
   isolation`），在 `workspaceRoot` 下为每个 id 创建独立 git worktree（默认）或 clone。
   同一 id 幂等复用；不同 id 永不共享目录。
3. **绝不写 Master 路径**。一旦配置了 `workstreams`，Worker 拒绝未映射的绝对路径
   （`WORKSTREAM_NOT_OWNED`）；只有显式 `workspaceMap` 才允许跨机路径映射。工作区根也
   不允许位于 source repo 内。
4. **交付**。Worker 在任务提示后追加 `[GETHEN-DELIVERY]`：只改自己的 worktree，在
   `feat/<id>` 上提交并 push 到配置的 remote。`ready` 报告到达时 Worker 再验证分支、
   补提交剩余改动、执行 push；失败则把报告改为 `failed`。报告 `summary` 追加
   `delivery: ... branch=... commit=... remote=... pushed=true`，并要求模型写明测试
   命令/结果与风险。Master 只做验收与合并。
5. **调度可观测性**。路由端口维护 `queued/running/dispatched/reused/queueEntries/
   queueTimeouts/maxQueueDepth/parked`，出现在 `gethen_status`、`gethen_task status`、
   `GET /health`、`GET /gethen/workers`。

## 代码落点

| 位置 | 内容 |
|---|---|
| `packages/worker-bridge/src/workstream.ts` | 纯契约：id 校验、`feat/<id>` 分支、路径识别 |
| `packages/cli/src/workstream.ts` | worktree/clone 准备、归属校验、push 与报告摘要 |
| `packages/cli/src/commands/worker.ts` | 配置解析、manifest 声明、workspace 解析、交付验证 |
| `packages/master-runtime/src/fleet.ts` | 队列/复用/超时计数 |
| `packages/master-runtime/src/runtime.ts` | 计数暴露到 health/control |
| `packages/cli/src/workstream.test.ts` | 双 workstream 并发隔离 + fake launch 路由集成 |

## 边界

- 这是工作区隔离，不是 OS 权限沙箱；Worker 自己的 DSH 审批/文件策略仍生效。
- 一个 workstream 同时只有一个 worktree；同一 id 的并发 assignment 共享该目录，适合顺序
  交付，需要并行修改时请拆成不同 workstream。
- `workspaceMap` 保留给跨机显式映射；它不是默认路径，不会把 Master 仓库暴露给 Worker。
- 队列计数是进程内实时值，Master 重启后归零；Ticket/receipt 仍是持久记录。
