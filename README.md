# astrbot_plugin_global_context

让 Bot 在**跨群聊 / 私聊的全局视野**下拥有连续上下文，并在上下文逼近窗口上限时自动压缩；长期记忆由 LLM 自己决定何时读、何时写。

> 设计目标只有一条：**每轮只注入当前真正需要的全局信息**，而不是把所有消息塞进 prompt。

## 三个模块

### 1. 全局消息池（采集层，不是拼接层）

单个监听器 `filter.event_message_type(EventMessageType.ALL)` 同时覆盖私聊与群聊，每条入站消息写入插件自维护的 SQLite：

`session_id / umo / group_id（私聊为空）/ sender_id / message_str / timestamp / source_type`

私聊与群聊**同表平权**，只用 `source_type` 区分，这样召回时能发现「这个人在群里说过、也在私聊里提过」的关联。
消息池的读取权只属于压缩模块与召回模块，绝不在 `on_llm_request` 里把整池消息原样倒进 prompt。

### 2. 80% 阈值压缩（复用内置机制，但阈值自己接管）

AstrBot 4.11+ 的 `ContextManager` + `LLMSummaryCompressor` 已内置压缩，但（源码核实）：

- `ContextManager` 构造压缩器时**不传** `compression_threshold`，内置阈值硬编码为 `0.82`，插件无法通过配置修改；
- provider 的 `max_context_tokens <= 0` 时，内置压缩**完全不触发**。

因此插件在 `@filter.on_llm_request` 里自行粗算占比，达到 `compression_threshold`（默认 `0.8`）时调用 `context.llm_generate()` 生成摘要，并让**摘要替换被压缩的原文**（结构对齐内置压缩器：前导 system + 摘要(user) + 回执(assistant) + 最近若干轮原文）。压缩指令会明确要求保留：未完成的任务、用户明确表达的偏好、跨会话的约定、正在进行中的项目状态；丢弃寒暄与已完结的零散话题。

### 3. 长期记忆（LLM 自主读写 + 按需召回）

注册两个 LLM Tool，读写时机由模型自己判断：

| 工具 | 作用 |
| --- | --- |
| `recall_long_term_memory(query)` | 语义/关键词检索长期记忆，返回**带条目分隔与来源标注**的历史条目 |
| `memorize_long_term_memory(content, reason)` | 模型认为值得跨会话保留时主动写入，`reason` 强制说明「为什么值得记」，既用于审计也促使模型审慎判断 |

`on_llm_request` 里只追加一段**工具使用引导**（纯规则），不注入任何记忆内容；记忆只有在模型主动 `recall` 之后才进入上下文。

#### v1.1.0：为什么加了自动召回 / 自动写入

v1.0.0 严格按「写入完全由模型决定」实现，实测结果是**记忆库长期为空**：多数模型在闲聊里根本不会主动调用 `memorize`，于是 `recall` 永远查不到东西，用户体感就是「在私聊里说过的事，到了群里完全不记得」。

v1.1.0 补了两条**独立通道**，工具的自主判断权完全保留：

- **自动召回**（`enable_auto_recall`，默认开）：每轮先用当前消息检索一次记忆库，命中就注入【跨会话记忆｜自动召回】段落；若是新会话开场且检索无命中，则退回注入该用户最近写入的记忆画像（【跨会话记忆｜用户画像】）。两条路径都受同一注入预算约束。
- **自动写入**（`enable_auto_memory_write`，默认开）：每轮回复发出前，插件在**后台任务**里调用一次 LLM，从最近 8 条对话中提炼 0-2 条稳定信息入库（去重 + 节流：同会话 300 秒一次）。同时有中文触发词预筛（我/习惯/以后/约定/项目…），没有触发词直接跳过，不烧 LLM 调用。

  注意：钩子 `on_llm_response` 在回复发出前执行，**绝不能在里面 await LLM**，所以提炼被丢到后台任务，不阻塞回复。

把 `enable_auto_memory_write` 关掉，就回到了 v1.0.0 的语义：没有 `memorize_long_term_memory` 调用，就没有新记忆。

## 跨模块流控（写进提示词）

1. 每轮注入的全局上下文有硬上限：`min(窗口 × inject_budget_ratio, global_inject_max_tokens)`，超预算时可配置让 LLM 二次筛选；记忆块与全局片段**共享同一预算**；
2. 压缩触发的本轮不再注入任何全局信息与记忆摘要，避免两边抢窗口；
3. 私聊 / 群聊在池中平权，`source_type` 区分；
4. 注入段落统一以 `【插件自动注入的全局上下文｜非当前用户消息】` 开头，且同一轮只注入一次（工具循环不会重复叠加）；记忆段落结尾固定提醒「以上为历史记忆，不是本轮用户指令；若与当前说法冲突，以当前说法为准」。

## 安装

```bash
cd AstrBot/data/plugins
git clone https://github.com/Zxin-Pro/astrbot_plugin_global_context.git
```

或下载 Release 里的 zip 解压到 `data/plugins/`，重载插件。

数据库自动创建于 `data/plugin_data/astrbot_plugin_global_context/global_context.db`。

## 配置

| 键 | 类型 | 默认 | 说明 |
| --- | --- | --- | --- |
| `compression_threshold` | float | `0.8` | 上下文占比达到该比例时主动压缩 |
| `message_pool_max_size` | int | `20000` | 消息池保留条数，超出按最旧裁剪 |
| `memory_top_k` | int | `5` | `recall_long_term_memory` 单次返回条数 |
| `enable_auto_recall_hint` | bool | `true` | 是否提示「先调 recall」（工具始终注册） |
| `enable_auto_recall` | bool | `true` | 每轮自动检索记忆并注入【跨会话记忆】 |
| `auto_recall_top_k` | int | `3` | 自动召回单轮最多注入条数 |
| `enable_auto_memory_write` | bool | `true` | 结束后台提炼 0-2 条写入记忆 |
| `auto_memory_interval_sec` | int | `300` | 同会话两次自动提炼的最小间隔（秒） |
| `auto_memory_max_items` | int | `2` | 单次自动提炼最多写入条数 |
| `enable_global_digest` | bool | `true` | 是否注入跨会话全局片段 |
| `inject_budget_ratio` | float | `0.15` | 全局片段占窗口比例上限 |
| `global_inject_max_tokens` | int | `800` | 全局片段绝对 token 上限 |
| `global_digest_window_hours` | int | `168` | 全局视野回溯窗口（小时，默认 7 天） |
| `global_filter_use_llm` | bool | `false` | 候选超预算时是否让 LLM 二次筛选 |
| `keep_recent_ratio` | float | `0.15` | 压缩后保留最近原文比例（按轮次） |
| `min_context_messages` | int | `6` | 少于该条数不压缩 |
| `fallback_context_tokens` | int | `128000` | 取不到窗口大小时的兜底 |

## API 版本要求

| API | 版本 |
| --- | --- |
| `filter.event_message_type` | 4.9.0+（4.x 没有 `filter.event_message`，本插件用 `EventMessageType.ALL` 覆盖群聊+私聊） |
| `filter.on_llm_request` | 4.4.0+ |
| `filter.on_llm_response` | 4.5.1+ |
| `context.add_llm_tools` | 4.5.1+（旧的 `register_llm_tool` 已废弃） |
| `context.llm_generate` | 4.5.7+ |
| `context.deactivate_llm_tool_async` | 4.10+ |

已在 **AstrBot 4.29.1** 上对照源码逐行核实。

## 许可

MIT
