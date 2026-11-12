# 认证扩展迁移路径：适配统一 AppContext 的 Quart 快照

## 快照的上下文结构（与旧模型的差异）

此快照中 `RequestContext` / `WebsocketContext` 已合并为单一的
`AppContext`（`src/quart/ctx.py`）：

- 只有一个 contextvar `_cv_app`（`src/quart/globals.py:39`）；`request`、
  `websocket`、`session`、`g` 全部是同一个 `AppContext` 上的 `LocalProxy`。
- `app.request_context()` / `app.websocket_context()` 都返回 `AppContext`，
  区别仅在于构造时传入 `request=` 还是 `websocket=`。
- `AppContext.copy()`（`src/quart/ctx.py:70`）**共享** `_request`、
  `_websocket`、`_session` 对象，但**新建** `g` 与 after-functions 列表。
- `pop()` 只有一条路径：request teardown → websocket teardown →
  appcontext teardown，且 `teardown_appcontext` 对请求与 WebSocket
  上下文都会触发，`exc` 会传入。
- `copy_current_request_context` / `copy_current_websocket_context` /
  `copy_current_app_context` 都是 `original.copy()` + `async with`，
  即异步子任务拿到的是"共享传输对象、全新 `g`"的副本。

## 旧扩展的失效模式

旧扩展分别挂在 `_cv_request` / `_cv_websocket` 上缓存身份，迁移后会出现：

1. 两条缓存路径合一后，`g` 在复制上下文中是全新的 —— 内层缓存 miss 会
   重新加载身份；
2. 若外层已吊销凭据，内层的重新加载会把**已失效资源重新激活**；
3. 请求与 WebSocket 各写一份 teardown 会在统一 pop 路径下重复执行。

## 迁移路径（可执行）

1. **统一上下文访问**：`_cv_request.get()` / `_cv_websocket.get()` →
   `has_request_context()` / `has_websocket_context()` 判定后取
   `request._get_current_object()` / `websocket._get_current_object()`。
2. **拆分两级缓存**（见 `auth_extension.py`）：
   - L1 = `g` 上的 per-context 备忘，随上下文销毁，复制上下文天然隔离；
   - L2 = 共享 request/websocket 对象上的权威身份 + `_auth_revoked`
     失效标记，嵌套、复制、并行任务共享同一份。
3. **查找顺序**：先查 L2 失效标记 → 再 L1 → 再 L2 → 最后才回源。
   失效标记优先于一切缓存，保证"失效资源不得被内层缓存重新激活"。
4. **吊销写共享对象**：`revoke()` 只写 transport 对象与后端吊销集，
   其他上下文的 L1 由第 3 步的标记检查兜底。
5. **teardown 合并为一处**：只在 `teardown_appcontext` 注册清理；
   不要在 teardown 里清除失效标记（吊销必须比上下文活得久）。
6. **验证**：运行 `run_checks.py`，覆盖正常退出、异常退出、嵌套复制
   上下文、并行任务、WebSocket 对称场景。

## 旧扩展最少需要改的接口

| 旧接口 | 新接口 | 说明 |
| --- | --- | --- |
| `_cv_request.get()` / `_cv_websocket.get()` | `has_request_context()` / `has_websocket_context()` + 对应 proxy | 单一 `_cv_app`，按 `ctx.has_request` / `ctx.has_websocket` 区分 |
| `RequestContext` / `WebsocketContext` 类型注解与 isinstance | `AppContext` | 两类上下文已合并 |
| 缓存在 ctx 对象上的身份属性 | L1 放 `g`，L2 放共享 request/websocket 对象 | `copy()` 新建 `g`、共享传输对象 |
| `teardown_request` + `teardown_websocket` 双注册 | 单个 `teardown_appcontext` | 统一 pop 路径，`exc` 照常传入 |
| 上下文内吊销状态 | 共享传输对象上的失效标记 + 后端吊销集 | 防止内层缓存重新激活 |

不需要改：`has_request_context()` / `has_websocket_context()` 名称、
`copy_current_*_context` 三个装饰器名称、`after_this_request` /
`after_this_websocket`、`async with` 进入上下文的方式。

## 运行

```bash
# 从仓库根目录：
.venv/bin/python examples/auth_context_migration/run_checks.py
# 或从本目录：
../../.venv/bin/python run_checks.py
```
