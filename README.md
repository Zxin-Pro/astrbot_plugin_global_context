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

### 3. LLM 自主长期记忆（按需召回）

注册两个 LLM Tool，由模型自己决定何时读写：

| 工具 | 作用 |
| --- | --- |
| `recall_long_term_memory(query)` | 语义/关键词检索长期记忆，返回**带条目分隔与来源标注**的历史条目 |
| `memorize_long_term_memory(content, reason)` | 模型认为值得跨会话保留时主动写入，`reason` 强制说明「为什么值得记」，既用于审计也促使模型审慎判断 |

`on_llm_request` 里只追加一段**工具使用引导**（纯规则），不注入任何记忆内容；记忆只有在模型主动 `recall` 之后才进入上下文。

## 跨模块流控（写进提示词）

1. 每轮注入的全局上下文有硬上限：`min(窗口 × 15%, global_inject_max_tokens)`，超出时可按配置让 LLM 二次筛选；
2. 压缩触发的本轮不再注入任何全局信息与记忆摘要，避免两边抢窗口；
3. 私聊 / 群聊在池中平权，`source_type` 区分；
4. **长期记忆的写入不自动发生**——没有 `memorize_long_term_memory` 调用，就没有新记忆。

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
| `memory_top_k` | int | `5` | recall 单次返回条数 |
| `enable_auto_recall_hint` | bool | `true` | 是否提示「先调 recall」（工具始终注册） |
| `enable_global_digest` | bool | `true` | 是否注入跨会话全局片段 |
| `inject_budget_ratio` | float | `0.15` | 全局片段占窗口比例上限 |
| `global_inject_max_tokens` | int | `800` | 全局片段绝对 token 上限 |
| `global_digest_window_hours` | int | `72` | 全局视野回溯窗口（小时） |
| `keep_recent_ratio` | float | `0.15` | 压缩后保留最近原文比例（按轮次） |
| `min_context_messages` | int | `6` | 少于该条数不压缩 |
| `fallback_context_tokens` | int | `128000` | 取不到窗口大小时的兜底 |
| `global_filter_use_llm` | bool | `false` | 候选超预算时是否让 LLM 二次筛选 |

## API 版本要求

| API | 版本 |
| --- | --- |
| `filter.event_message_type` | 4.9.0+（4.x 没有 `filter.event_message`，本插件用 `EventMessageType.ALL` 覆盖群聊+私聊） |
| `filter.on_llm_request` | 4.4.0+ |
| `context.add_llm_tools` | 4.5.1+（旧的 `register_llm_tool` 已废弃） |
| `context.llm_generate` | 4.5.7+ |

已在 **AstrBot 4.29.1** 上对照源码逐行核实。

## 许可

MIT