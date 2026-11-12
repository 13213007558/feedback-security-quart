# Quart 流式下载三类失败的覆盖关系诊断

## 1. 要回答的问题

一个流式下载请求可能同时撞上三类失败：

- **A. 权限撤销**：下载令牌在流式传输途中被撤销，生成器抛 `PermissionError`。
- **B. 生成器异常**：响应体生成器自身抛 `RuntimeError`。
- **C. teardown 异常**：`teardown_request` 清理回调抛 `IOError`。

目标是在**纯进程内、无端口、无网络**的受控环境里，追踪这三类失败经过“响应处理”和
“请求上下文退出”时**谁覆盖谁**，给出首块（`http.response.start` + 首个 body 帧）发送
前后的差别，保留因果链与已外发字节证据，并提出能接入现有错误处理机制、且**不会把清理
失败伪装成成功授权**的修正。

## 2. 复现方式

```sh
.venv/bin/python diagnostics/diagnose_streaming_failures.py
```

- 探针直接调用真实 `Quart` ASGI app（`app(scope, receive, send)`），逐帧记录
  `http.response.start` / `http.response.body`，因此“已外发字节”是硬证据。
- 三个失败由独立开关注入；脚本对每个场景各跑一次 **legacy（修正前发送管线）** 和
  **fixed（修正后）**，同场对照。
- 同时采集 app 日志记录与 `got_serving_exception` 信号。

## 3. 关键机制：上下文在“首块之前”就退出了

追踪时间线（任何流式响应都成立，已用 `QuartClient` 与裸 ASGI 两条路径交叉验证）：

```
VIEW 返回 streaming Response(200)
  -> AppContext.pop()            # handle_request 的 async with 在这里退出
     -> teardown_request(exc=None)
  -> 然后 ASGIHTTPConnection._send_response() 才发送 response.start
     -> 才开始迭代生成器、外发 body
```

即 **请求上下文（含 teardown）在第一个字节外发之前就已经执行完毕**；响应体生成器运行在
请求上下文之外（生成器内访问 `g` 会得到 `RuntimeError: Not within an app context`）。
这决定了三类失败分属两个互斥的时间窗口。

## 4. 首块发送前 vs 首块发送后

### 4.1 首块发送前（teardown 失败，场景 S1/S2/S3/S6）

teardown 在 `response.start` 之前抛错，异常经 `_CollectErrors` 聚合成
`BaseExceptionGroup`，从 `AppContext.pop` → `handle_request` 的 `async with` 直接逃出，
`_send_response` **根本没机会运行**：

- `response.start` 未发送，**已外发字节 = 0**。
- 边界异常（修正前后一致）：
  `ExceptionGroup → ExceptionGroup(Errors during context teardown)
   → ExceptionGroup(Errors during request teardown) → TeardownBoomError`。
- 此时还没有任何提交的状态码，理论上**可以**改走错误处理器；但当前实现让它原样冒泡，
  这是安全的——它绝不会被报告成 `200`。

### 4.2 首块发送后（权限撤销 / 生成器异常，场景 S4/S5）

`response.start(200)` 与若干 body 帧已经提交，生成器随后抛错：

| 场景 | response.start | 已外发字节 | 干净结束帧 | 修正前边界 |
|---|---|---|---|---|
| S4 权限撤销 | 是 (200) | `b"chunk-1"` (7B) | 否 | `ExceptionGroup[PermissionRevokedError]` |
| S5 生成器异常 | 是 (200) | `b"chunk-1chunk-2"` (14B) | 否 | `ExceptionGroup[GeneratorBoomError]` |

- 状态码 200 与部分 body 已上线，**无法再改写成 403/500 错误页**（HTTP 头已提交）。
- **修正前的覆盖问题**：`_send_response` 没有任何 `try/except`，异常只沿
  TaskGroup 冒泡给 ASGI 服务器；`handle_request` 里的错误处理 `try` 只包住
  `app.handle_request`，**包不到发送阶段**，且 `app.errorhandler` /
  `got_request_exception` / `app.log_exception` 全部不会触发。结果是这次失败在应用层
  **零日志、零信号**，线上只能看到连接中断，无法与权限撤销或生成器 bug 区分。

### 4.3 三者同时发生（S6）

teardown（C）发生在首块前并先抛出，整个响应在任何字节外发前中止，权限撤销（A）与
生成器异常（B）所在的生成器**永远不会被迭代**。因此 **C 覆盖 A 和 B**——这不是日志混淆，
而是真实的执行顺序：上下文先死，发送未开始。

## 5. 修正前 → 修正后对照（脚本实测）

```
scenario                                       mode   start bytes terminal signals boundary
S0 无失败                                      legacy True  21B   True     0       none
S0 无失败                                      fixed  True  21B   True     0       none
S1 权限撤销 + teardown 失败                    legacy False  0B   False    0       ExceptionGroup[ExceptionGroup]
S1 权限撤销 + teardown 失败                    fixed  False  0B   False    0       ExceptionGroup[ExceptionGroup]
S2 生成器异常 + teardown 失败                  legacy False  0B   False    0       ExceptionGroup[ExceptionGroup]
S2 生成器异常 + teardown 失败                  fixed  False  0B   False    0       ExceptionGroup[ExceptionGroup]
S3 仅 teardown 失败（首块前）                 legacy False  0B   False    0       ExceptionGroup[ExceptionGroup]
S3 仅 teardown 失败（首块前）                 fixed  False  0B   False    0       ExceptionGroup[ExceptionGroup]
S4 仅权限撤销（首块后）                       legacy True   7B   False    0       ExceptionGroup[PermissionRevokedError]
S4 仅权限撤销（首块后）                       fixed  True   7B   False    1       none*
S5 仅生成器异常（首块后）                     legacy True  14B   False    0       ExceptionGroup[GeneratorBoomError]
S5 仅生成器异常（首块后）                     fixed  True  14B   False    1       none*
S6 三类同时                                   legacy False  0B   False    0       ExceptionGroup[ExceptionGroup]
S6 三类同时                                   fixed  False  0B   False    0       ExceptionGroup[ExceptionGroup]
```

`*` 裸探针里连接协程因发送被显式中止而正常返回；在真实 TaskGroup/测试客户端下，原始异常
仍被包在 `ExceptionGroup` 中保留（见 §7），并未被吞掉。

修正后 S4/S5 的可观测变化：

- `got_serving_exception(PermissionRevokedError|GeneratorBoomError)` 信号触发 1 次。
- app logger 输出 `ERROR ... Exception while sending response to GET /download`，
  带完整 traceback。
- 已提交的 200 与已外发字节保持不变，**不发送伪造的干净结束帧**。

## 6. 修正方案（已实现）

只改 `src/quart/asgi.py`，把发送阶段包起来并区分两个窗口：

1. `ASGIHTTPConnection._send_response` 用 `try/except` 包住 `response.start` 与 body
   迭代；`ClientDisconnectedError` 照旧上抛，其余 `Exception` 交给新助手
   `_report_sending_exception` 后**重新抛出原异常**。
2. `_report_sending_exception` 接入**既有**机制，不新增并行错误体系：
   - 先 `app.logger.error(..., exc_info=error)`（保证日志证据不被信号接收器故障阻断）；
   - 再发既有信号 `got_serving_exception`，并用 `try/except Exception` 保护，接收器
     出错只记一条 `"Error handling got_serving_exception signal"`，不掩盖主错误。
3. **首块前的 teardown 失败路径完全不动**：它仍以 `BaseExceptionGroup` 原样冒泡，
   不经过发送期报告，因此不会被改写或美化。

为什么这不会把清理失败伪装成成功授权：

- 报告路径只在“状态码/头已提交”后触发，且**只记录、只重抛**，从不构造新的成功响应、
  从不改状态码、从不补发结束帧。
- 首块前的 teardown 失败在任何字节外发前中止，边界仍是携带 `TeardownBoomError` 的
  异常组，客户端看到的是失败而不是 200。
- 应用若要把这类中止接入告警/审计，订阅 `got_serving_exception` 即可，拿到的是原始
  异常对象，权限撤销与生成器 bug 可按类型区分。

## 7. 因果链保留证据

测试客户端（真实 TaskGroup）下，首块后异常的异常组结构：

```
ExceptionGroup: unhandled errors in a TaskGroup (1 sub-exception)
  └─ RuntimeError: boom after byte        # 原始根因，traceback 完整
```

- 原始异常类型与消息完整保留在组内，没有被替换成通用错误。
- 日志的 `exc_info` 指向生成器抛出点，信号收到同一异常实例。
- 已外发字节可从 ASGI 帧精确回放（S4=`b"chunk-1"`，S5=`b"chunk-1chunk-2"`）。

## 8. 测试

新增两个用例于 `tests/test_asgi.py`：

- `test_streaming_failure_after_first_byte_is_reported`：断言首块后失败时 200 与首块
  仍在、无干净结束帧、原始异常在异常组内、`got_serving_exception` 收到同一异常。
- `test_streaming_failure_before_first_byte_skips_sending_report`：断言首块前 teardown
  失败时无任何帧外发、不触发发送期信号、异常原样冒泡。

```sh
.venv/bin/python -m pytest -q -o addopts= tests/test_asgi.py tests/test_ctx.py tests/test_app.py
# 82 passed
```

全量套件中仅 `tests/test_cli.py` 的 3 个 dotenv 用例失败，已用 `git stash` 证实改动前
同样失败（环境缺 `python-dotenv`），与本修正无关；`tests/wrappers/test_response.py`
因环境缺 `hypothesis` 无法收集，亦为既有环境限制。
