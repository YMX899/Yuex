# Yuex Agent Runtime

[![License: AGPL v3](https://img.shields.io/badge/License-AGPL_v3-blue.svg)](LICENSE)

Yuex 是位于业务 Backend 与 Agent Harness 之间的生产 Runtime。业务系统负责用户、产品和数据；Runtime 负责 Run、并发、状态与恢复；Harness 负责模型推理和工具循环。

当前实现使用 [OpenClaw](https://github.com/openclaw/openclaw) 作为 Agent Core。感谢 OpenClaw 社区提供优秀的开源 Agent 框架、会话与工具基础。OpenClaw Core 不是本项目的原创代码；Yuex 的工作集中在它上方的 Runtime 控制面、并发治理、执行适配和业务接入边界。Harness Driver 是可替换层，也可以接入 Codex 或其他 Agent runtime。

> 当前仓库是从 Huahuo 现有工程复制出的实现快照，用于理解架构和继续独立化。部分 Go import、持久化和回调仍引用原 Backend，因此它还不是开箱即用的独立发行版。

## 一张图看懂

```text
产品与业务
Backend A ─┐
Backend B ─┼── Host Adapter / SDK
Backend C ─┤   身份、Workspace、Session、Agent 制品、结果回写
Backend D ─┘
                │
                ▼
Runtime API v1
submit / status / events / abort / capabilities
                │
                ▼
Yuex Runtime Control Plane
Run Store / Scheduler / Capacity / Lease / Fencing
Recovery / Event Store / Usage / Terminal Convergence
                │
                ▼
Runtime Host / Go Adapter
物化 Workspace / 验证 RunTicket / 执行工具策略 / 归一化事件
                │
                ▼
Harness Driver
├── OpenClaw Driver → 固定版本发行底座 → OpenClaw Agent Core（当前）
├── Codex Driver    → Codex Runtime（可替换）
└── Other Driver    → 其他 Harness（可替换）
```

不同 Backend 不需要复制一套 Agent Core。它们实现自己的 Host Adapter，把业务对象转换成同一个 Runtime 契约；Runtime 以下的调度、恢复和 Harness 执行可以共用。

| 层 | 负责什么 |
| --- | --- |
| Backend A/B/C/D | 登录与租户、业务权限、Thread、Workspace、产品数据、计费规则和最终展示 |
| Host Adapter / SDK | 把 Backend 的身份、文件、Session、Agent Release 和回写方式转换成 Runtime 契约 |
| Runtime API v1 | 提供稳定的提交、查询、事件、取消和能力发现接口 |
| Runtime Control Plane | 保存 Run 事实，排队和分配容量，维护 Lease/Fence，恢复异常执行并收敛终态 |
| Runtime Host / Go Adapter | 验证短期授权，物化本次 Workspace，启动 Harness，转发事件和 Usage |
| Harness Driver | 把通用 Runtime DTO 映射为某个 Harness 的会话、工具和取消协议 |
| Agent Core | 执行模型循环、调用工具、维护原生会话并生成结果 |

## Backend 要准备什么

接入前，Backend 需要提供八类能力。它们可以使用自己的技术栈，不要求照搬 Huahuo 的表结构。

| 能力 | Backend 需要拥有的事实 |
| --- | --- |
| 身份与权限 | 稳定的 `tenantId`、`userId`、`workspaceId`，以及对本次输入和结果的访问授权 |
| Thread 与 Session | 产品 `threadId`，以及由服务端维护的 Harness Session 映射，例如 `openclawSessionKey` |
| Workspace Provider | Workspace 版本、上下文代次、允许读取的逻辑文件和短期对象读取引用 |
| Agent Catalog | 当前生效的 Agent、Skill、Knowledge、Tool Policy 和 Runtime Config Release |
| 模型配置 | 服务端保存的模型、Provider、Auth Pool、超时和输出预算；密钥不进入 Workspace |
| 运行存储 | Run、Reservation、Lease、Event、Usage 和终态记录的持久化能力 |
| 事件与结果回写 | 把 Runtime 事件投影给前端，把最终结果写入业务 Thread、Task 或资产 |
| 服务间安全 | Backend、Control Plane 和 Runtime Host 之间的认证，生产环境建议使用 mTLS |

数据库是部署依赖，不是交给 OpenClaw 的 Run 参数。Backend 数据库保存用户、Thread、Workspace 和产品结果；Runtime Store 保存 Run、调度、Lease、Event 和 Usage；Harness Session Store 保存原生会话。三者可以先共用一个物理数据库，但所有权和写入接口必须分开。

### 每次 Run 的输入

前端只向自己的 Backend 提交产品语义：

```json
{
  "workspaceId": "workspace-id",
  "threadId": "thread-id",
  "agentProfileId": "agent-profile-from-catalog",
  "skillProfileIds": ["skill-profile-from-catalog"],
  "modelProfileId": "optional-catalog-model",
  "input": {
    "content": [
      {"type": "text", "text": "用户请求"},
      {"type": "file", "resourceId": "authorized-resource-id"}
    ]
  }
}
```

Backend 完成鉴权、选择和冻结后，才向 Runtime 提交执行事实：

| 分组 | 主要字段 |
| --- | --- |
| Run 身份 | `runId`、`tenantId`、`userId`、`workspaceId`、`threadId` |
| Session | 服务端生成或读取的 Harness Session Key；不能由前端伪造 |
| Workspace | `workspaceVersion`、`contextGeneration`、逻辑文件清单、附件身份和短期对象引用 |
| 冻结计划 | Agent Release、Skill Releases、Knowledge Refs、Required Tools、Output Contract、Tool Budget |
| Runtime 配置 | `runtimeConfigId` 和不可变版本；模型与 Auth Pool 由服务端解析 |
| 并发授权 | `reservationId`、递增的 `fencingToken`、`capabilityHash` 和短期 `RunTicket` |
| 本次输入 | `inputMessage` 和经过授权、限量、可验证的附件 |
| 回写引用 | `productSessionRef`、结果解析身份和 Backend 自己保存的业务关联 |

`RunTicket` 会绑定 Run、Tenant、Workspace、Host、Reservation、Manifest、Plan、Fence、过期时间和防重放身份。Runtime Host 必须验证它，但前端和模型都不应看到它。

## 一次 Run 怎样完成

```text
1. Backend 鉴权并固定 Workspace / Thread
2. TaskIntent 在生效 Catalog 中选择 Agent Profile
3. SkillRegistry 过滤可用、已授权的候选 Skill
4. Backend 冻结 AgentRunPlan 与 RuntimeInputManifest
5. Scheduler 选择具备所需能力和空闲 Slot 的 Runtime Host
6. Reservation + Lease + FencingToken 建立唯一执行所有者
7. Runtime Host 物化只属于本次 Run 的 Workspace
8. Harness Driver 调用 OpenClaw、Codex 或其他 Agent Core
9. 事件按序写入 Event Store，Backend 通过 cursor 消费
10. 终态收敛后写回 Assistant、业务结果与 Usage，并释放容量
```

前端永远只调用 Backend。它从 Backend 获取公开状态和 SSE/事件流，不轮询 OpenClaw，也不持有 Runtime 凭据、真实 Workspace 路径、RunTicket、Lease 或 FencingToken。

Runtime API 面向 Backend/SDK 的最小操作是：

| 操作 | 用途 |
| --- | --- |
| `capabilities` | Host 启动和调度前确认 Runtime 版本、工具、策略与取消能力 |
| `submit` | 幂等提交一个已经冻结并取得 Reservation 的 Run |
| `status` | 对账当前状态、最后事件序号、结果、错误和 Usage |
| `events` | 按 sequence/cursor 增量读取事件，支持断线续传 |
| `abort` | 请求取消；Runtime 继续负责把取消收敛为唯一终态 |

## Agent、Skill 和制品

Agent Profile 不是一段临时 Prompt，而是一个可发布、可版本化的能力入口。Huahuo 的设计源通常位于：

```text
agent/<agent_name>/workspace/
├── AGENTS.md                 # 长期行为与任务边界
├── SOUL.md                   # 人格和表达姿态
├── MEMORY.md                 # 允许保留的稳定记忆规则
├── TOOLS.md                  # 工具使用规则，不负责注册工具
├── capability-catalog.json   # Agent 声明需要的能力
├── skills/                   # Agent 源内的 Skill 设计
├── knowledge/                # 只属于该 Agent 的领域知识
└── protocols/                # 输入、输出和业务协议
```

发布时不是把这个目录随意复制到服务器，而是生成不可变制品：

```text
设计源
  → 校验 Agent / Skill / Knowledge / Tool 声明
  → 构建 runtime-agents/<agentProfile>/
  → 构建 runtime-skills/<skillProfile>/
  → 更新 agent-routing-manifest.json
  → 更新 meta-manifest.json 与 runtime-config-overrides.json
  → 生成带版本与内容身份的 Release Bundle
  → 提升 activeCatalogRevision
```

一次 Agent Release 至少要固定：Agent 文件版本、候选/必需 Skill、Knowledge 根和精确引用、Tool Policy、允许的执行 Scope、Runtime Config、输入策略及输出协议。Backend 只从生效 Catalog 解析这些事实，不能从目录名猜版本。

### Huahuo 如何选择 Profile 和 Skill

```text
已鉴权 Workspace + TaskIntent + executionScope
        ↓
activeCatalogRevision
        ↓
L1 Agent Router
按 intent category、task type、scope、状态和优先级筛选 Agent Profile
        ↓
SkillRegistry
在该 Agent 的候选集合中按 active、权限、任务能力和上限筛选 Skill
        ↓
AgentRunPlan
冻结 Agent / Skills / Knowledge / Tools / Output / Workspace 版本
```

App 可以提交 Catalog 中公开的 `agentProfileId` 和 `skillProfileIds`，但不能提交内部文件路径、Release Hash、Provider Key 或任意工具名。动态路由也只能从服务端候选集合选择；模型不能创造未注册 Skill。Plan 一旦冻结，重试、恢复和终态解析都复用同一个 Plan，不在结束时重新路由。

`SKILL.md` 应描述任务方法、输入要求、读取顺序、工具边界、输出 Schema 和失败条件。它不是堆放全部知识正文的地方。

### 知识应该放在哪里

| 知识类型 | 推荐位置 | 装载方式 |
| --- | --- | --- |
| Agent 专属知识 | 设计源 `agent/<name>/workspace/knowledge/`；发布后进入 `runtime-agents/<profile>/knowledge/` | 只随对应 Agent Release 可见 |
| Skill 专属参考 | `runtime-skills/<skillProfile>/references/` | 由该 Skill 的 `SKILL.md` 按需读取 |
| 平台通用知识 | `templates/knowledge/<domain>/INDEX.md` 及其子文档 | Skill 声明精确 `knowledgeRefs` 后按 Release 装载 |
| 用户私有长期知识 | 用户 Formal Workspace 的 `profile/`、`materials/`、`resources/` 等 | 先用 `workspace_search` 找逻辑路径，再用 `read` 读取 |
| 单次任务材料 | Runtime Workspace 的 `input/` | 只在本次 Run 有效，结束后不作为长期事实源 |

`knowledgeRoots` 只是授权边界，不表示把整个知识树塞进上下文。每个 Skill 应从一个小的 `INDEX.md` 或 `OVERVIEW.md` 开始，声明精确、受版本约束的 `knowledgeRefs`，再按需读取更深内容。

### 怎样增加一个 Tool

当前企业契约只向 Agent 暴露 `read`、`workspace_search` 和条件授权的 `write`。新增 Tool 必须走完整发布链：

1. 在 Agent Core、Harness Driver 或受控 Plugin 中实现 Tool 与 JSON Schema。
2. 在 Runtime `capabilities` 中发布名称、来源、版本、Schema 身份和 `ready` 状态。
3. 在服务端 Tool Catalog、Agent `capability-catalog.json` 和 Tool Policy Profile 中授权。
4. 让需要它的 Skill 声明 required capability，Planning 才能把它写入 `requiredTools`。
5. Runtime 为单次 Run 生成最小权限、带签名的 `tools.allow`；`deny` 始终优先。
6. Tool 调用必须验证 Tenant、Workspace、Run 和 Fence，并记录 started/finished/rejected 审计事件。
7. 发布新的 Agent/Skill/Runtime Release 后，只有能力握手通过的 Host 才能接收该 Run。

仅修改 Prompt、`TOOLS.md` 或 allow-list 不等于 Tool 已实现。Backend 和前端也不能临时注入未注册 Tool。

## 状态机与生产并发

内部状态机保留足够多的阶段用于恢复和审计：

```text
created
  → resolving_intent
  → planning
  → awaiting_confirmation?
  → admission_pending
  → queued
  → reserving
  → dispatched
  → accepted
  → materializing
  → running
  → finalizing
  → succeeded | failed | cancelled | timeout | orphaned
```

前端只需要较小的公开状态集合：

```text
resolving → planning → awaiting_confirmation?
          → queued → running / aborting
          → succeeded | failed | cancelled | timeout
```

并发可靠性不靠“启动更多进程”保证：

- Scheduler 只把 Run 分配给版本、能力和空闲 Slot 都满足要求的 Host。
- Lease 表示当前执行者仍然活着；心跳超时后 Recovery 才能接管。
- FencingToken 每次重新取得所有权都递增，旧 Worker 即使晚到也不能写事件或终态。
- Submit、事件 sequence、结果投影和终态收敛都必须幂等。
- Host 丢失、回调中断或进程重启后，Recovery 从持久化事实继续，不从日志猜状态。
- 终态只有一个；释放 Slot、Usage 结算和业务回写都有可恢复检查点。

## 长上下文压缩

压缩不是简单截断聊天记录。Runtime 以“即将发送给当前 Provider 的完整 Prompt”作为唯一压力事实，其中包括历史、旧摘要、工具结果、系统与 Workspace 规则、Tool Schema、当前请求、图像和 Provider 包装。

```text
组装完整 Provider Prompt
        ↓
测量当前模型的 Prompt 压力
        ├── 未超预算 → 发送给 Provider
        └── 超预算
              ├── 有可压缩历史
              │     → 旧历史和工具证据生成结构化检查点
              │     → 保留近期原文尾部；必要时进入 summary-only
              │     → 重组完整 Prompt 并再次测量
              │     → 仍超预算且还能推进边界时继续压缩
              └── 不可压缩部分自身超预算
                    → 明确失败，不静默删除当前请求或安全规则
```

结构化检查点至少保留：用户目标、约束、关键决策、已完成工作、工具证据、当前状态、未完成请求和下一步。压缩成功的条件不是“生成过摘要”，而是重组后的完整 Prompt 已进入当前模型预算。

事件存储另有一层保留压缩：Run 终态后可以清理旧 `draft_delta` 前缀，但必须保留终态与最终 Assistant；过旧 cursor 返回明确 gap，不能伪造连续序列。

## 替换 OpenClaw

Control Plane 不应依赖 OpenClaw 私有 DTO。要接入 Codex 或其他 Harness，只需实现同一 Driver 边界：

| Driver 能力 | 必须保证的语义 |
| --- | --- |
| Submit | 接收冻结 Plan、Manifest、Session、Tool Policy 和输入，幂等启动一次执行 |
| Status / Events | 把 Harness 原生事件归一化为有序 Runtime Event |
| Abort | 能取消当前模型/工具执行，并最终回报唯一终态 |
| Capabilities | 报告版本、工具、策略、上下文和取消能力，供 Scheduler 匹配 |
| Session | 维护产品 Thread 与 Harness 原生 Session 的稳定映射 |
| Workspace | 只访问物化后的逻辑 Workspace，不能越过 Run 授权边界 |
| Result / Usage | 返回标准终态、Assistant 结果、错误分类和 Usage |

因此替换 Harness 时，Backend 的用户、Thread、Workspace、Agent Catalog 和结果表不需要跟着重写；变化集中在 Driver 与少量 Runtime Config。

## 让 Codex 接入你的 Backend

把下面这段任务连同你的 Backend 仓库和本仓库交给 Codex 或其他编程 Agent：

```text
目标：把 <YOUR_BACKEND> 作为新的 Host 接入 Yuex Agent Runtime。

先阅读：
- README.md
- extracted/go-runtime-control-plane/internal/runtime/
- extracted/go-runtime-adapter/cmd/openclaw-runtime-adapter/
- extracted/openclaw-driver/overlay/
- cut-boundary-reference/ 中与 API、Worker、Storage 对应的参考实现

请先输出现有 Backend 的映射表，再实施：
1. 找到 Tenant/User/Workspace/Thread/Task/Result 的真实数据所有者。
2. 实现 Host Adapter：IdentityProvider、WorkspaceProvider、SessionProvider、
   AgentCatalogProvider、RuntimeConfigProvider、ObjectProvider、EventSink、ResultSink。
3. 将前端请求解析成服务端 TaskIntent；只使用 active Catalog 中的公开 Profile。
4. 冻结 AgentRunPlan、RuntimeInputManifest、CapabilityHash 和 RunTicket。
5. 接入 submit/status/events/abort/capabilities，不让前端直接访问 Runtime。
6. 将 Runtime Event 投影为 Backend 的 SSE/查询模型，将唯一终态写回业务结果。
7. 保留 Lease、Fencing、幂等、cursor、取消、恢复和 Usage 语义。
8. 不把数据库凭据、Provider Key、真实 Workspace 路径或内部 Session Store 暴露给模型。

交付物：
- Backend 到 Runtime 的字段映射
- Adapter 接口与实现
- 必要的运行存储迁移
- 配置示例和本地启动方式
- 提交、断线续传、取消、Host 丢失恢复、旧 Fence 拒写的 focused tests

遇到 Huahuo 专属 import 或回调时，用清晰的 port/interface 隔离；
不要把另一个 Backend 的业务表复制进 Runtime Core。
```

## 从哪里读代码

| 目录 | 内容 |
| --- | --- |
| `extracted/go-runtime-control-plane/internal/runtime/` | Plan、Scheduler、Host、Capacity、Lease、Fence、Recovery、Event、Usage、Workspace Materialization |
| `extracted/go-runtime-adapter/cmd/openclaw-runtime-adapter/` | Go Runtime Host、HTTP transport、Host 注册/心跳和 Gateway bridge |
| `extracted/openclaw-driver/overlay/` | OpenClaw 企业 Run、策略、能力握手、事件和恢复扩展 |
| `extracted/openclaw-driver/tooling/` | Overlay 安装、契约生成和源检查工具 |
| `cut-boundary-reference/` | Huahuo Backend 的 API、Worker、Storage、Search 和部署接线参考，不属于独立 Runtime Core |

## License

Huahuo 创作的代码按 [GNU Affero General Public License v3.0 only](LICENSE) 发布。OpenClaw 及其他第三方组件继续适用各自许可证，详见 [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md)。

