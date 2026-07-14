# Spark 2.0 流式下载机制分析

> 作者：claude-opus-4-6（基于源码直接分析，2026-07-14）
> 涵盖文件：`src/SplatLoader.ts`、`src/worker.ts`、`src/SplatPager.ts`

---

## 概览

Spark 2.0 中存在两套相互独立的下载机制，分别服务于不同场景：

| 机制 | 入口 | 适用格式 | 下载方式 |
|------|------|----------|----------|
| **分块流式加载（LoD Paging）** | `SplatPager` → `PagedSplats.fetchDecodeChunk` | `.rad` / `.radc` 预处理文件 | HTTP Range 请求，按需精确下载 |
| **文件流式解码（Progressive Decode）** | `SplatLoader.loadInternal` → `worker.decodeBytesUrl` | `.ply` / `.spz` / `.splat` / `.ksplat` 等原始格式 | 完整 HTTP 下载 + ReadableStream 边到边解码 |

两者共享 `workerPool`（最多 4 个 WebWorker），但在下载策略、进度报告和解码时机上完全不同。

---

## 机制一：HTTP Range 分块下载（`SplatPager` 路径）

### 适用场景

- 文件为经过 `build-lod` CLI 预处理的 `.rad`（头文件）+ `.radc`（chunk 文件）格式
- `SplatMesh` 以 `paged: true` 方式创建
- 需要按视点动态按需加载，而不是一次性下载全部数据

### 核心函数：`fetchRange`（`src/SplatPager.ts`）

```typescript
async function fetchRange({
  url,
  requestHeader,
  withCredentials,
  offset,   // 字节起始（可选）
  bytes,    // 请求字节数（可选）
}): Promise<Uint8Array> {
  const request = new Request(url, { ... });
  if (offset !== undefined && bytes !== undefined) {
    // 精确 Range 请求：bytes=start-end（闭区间）
    request.headers.set("Range", `bytes=${offset}-${offset + bytes - 1}`);
  }
  const response = await fetch(request);
  return new Uint8Array(await response.arrayBuffer());
}
```

**特点：**
- 不使用 `ReadableStream`，等待完整 Range 响应后一次性返回 `Uint8Array`
- Range 请求失败时直接抛出异常（由 `driveFetchers` 的退避机制处理）
- `worker.ts` 中也有一份相同签名的 `fetchRange` 实现（两份独立副本，逻辑相同）

### 三种 chunk 下载路径（`PagedSplats.fetchDecodeChunk`）

```
chunk 来自 RAD 内嵌数据（filename 字段为空）
  → fetchRange(this.rootUrl, offset + chunksStart, bytes)
      └─ HTTP Range 请求，从 .rad 文件中精确截取该 chunk 的字节范围

chunk 来自外部 .radc 文件（filename 字段有值）
  → fetchRange(new URL(filename, rootUrl).toString())
      └─ 完整 GET 请求获取整个 .radc 文件（每个 .radc 就是一个 chunk）

非 RAD 格式（如 .ksplat 分块文件）
  → fetch(this.chunkUrl(chunk))
      └─ 普通 GET，URL 由 chunkUrl() 生成（-lod-0. 替换为 -lod-N.）
         无 Range 头，不带进度回调
```

### RAD 头部下载流程（`PagedSplats.getRadMeta`）

```
尝试 Range bytes=0-65535 (64KB)
  → 若 decode_rad_header() 成功 → 返回 RadMeta
  → 若失败（头部太大）→ 尝试 256KB
    → 若失败 → 尝试 1MB
      → 若失败 → 抛出异常 "Failed to decode RAD header"
```

所有尝试都是独立 HTTP Range 请求，三档渐进回退，取第一个成功的结果。

### fetch 失败处理（`SplatPager.driveFetchers`）

```typescript
splats.fetchDecodeChunk(chunk)
  .then(async (data) => {
    this.fetched.push({ splats, chunk, data });
    // fetchPause 用于测试/调试时人为降速
    if (this.fetchPause > 0) {
      await new Promise(resolve => setTimeout(resolve, this.fetchPause));
    }
  }, async (error) => {
    console.warn(error);
    // 失败退避：250ms 固定 + 0~500ms 随机，防止并发失败风暴
    const backoff = 250 + 500 * Math.random();
    await new Promise(resolve => setTimeout(resolve, backoff));
  })
  .finally(() => {
    // 从 fetchers 列表移除自己，释放 fetcher 槽位
    this.fetchers = this.fetchers.filter(...);
    this.processFetched();
  });
```

失败后不会标记 chunk 为永久失败，下一帧 `driveFetchers` 仍会重试（只要 chunk 还在 `fetchPriority` 中）。

---

## 机制二：ReadableStream 流式解码（`SplatLoader` 路径）

### 适用场景

- 文件为未经预处理的原始格式：`.ply`、`.spz`、`.splat`、`.ksplat`、`.sog`
- 通过 `SplatLoader.load()` / `loadAsync()` 加载
- 可选传入 `stream: ReadableStream` 实现外部流输入（如来自 Fetch Response 的 body）

### 核心函数：`decodeBytesUrl`（`src/worker.ts`）

Worker 内部的统一解码入口，支持三种数据来源：

#### 模式 A：内存字节数组（`fileBytes` 模式）

```typescript
if (fileBytes) {
  const CHUNK_SIZE = 1048576; // 1MB 分块
  for (let i = 0; i < fileBytes.length; i += CHUNK_SIZE) {
    decoder.push(fileBytes.subarray(i, i + CHUNK_SIZE));
  }
}
```

- 数据已在内存中，同步按 1MB 块依次推送给 WASM 解码器
- 无网络 IO，无进度事件
- 适用于文件已预先下载到内存的场景

#### 模式 B：直接 URL 流式下载（`url` 模式）

```typescript
} else if (url) {
  const response = await fetch(request);
  const readStream = response.body.getReader();
  const contentLength = Number.parseInt(response.headers.get("Content-Length") || "0");
  let loaded = 0;

  while (true) {
    const { done, value } = await readStream.read();
    if (done) { readStream.releaseLock(); break; }

    loaded += value.length;
    sendStatus({ loaded, total });  // 发送进度给主线程
    decoder.push(value);            // 边下载边推送给 WASM 解码
  }
}
```

- **真正的流式**：每到一个网络块立即推送给 WASM 解码器，无需等待全文件
- 通过 `sendStatus` 将 `{loaded, total}` 发送给主线程（触发 `onProgress` 回调）
- `Content-Length` 不可用时 `total=0`（`lengthComputable=false`）
- WASM 解码器（`ChunkDecoder`）支持增量解码

#### 模式 C：外部 ReadableStream 推送（`chunked` 模式）

```typescript
} else if (chunked) {
  let loaded = 0;
  const total = chunkedLength ?? 0;

  while (true) {
    // 向主线程请求下一块数据
    const readNextChunk: Promise<Uint8Array> = new Promise(resolve => {
      nextChunkWaiter = resolve;
    });
    sendStatus({ nextChunk: true });   // 告知主线程"给我下一块"
    const nextChunk = await readNextChunk;

    if (nextChunk.length === 0) break; // 流结束

    decoder.push(nextChunk);
    loaded += nextChunk.length;
    sendStatus({ progress: { loaded, total } });
  }
}
```

- Worker 主动向主线程"拉取"数据块（pull 模型）
- 主线程在 `onStatus` 回调中检测 `nextChunk: true`，从 `ReadableStream` 读取下一块并通过 `worker.call("nextChunk", {chunk})` 推回

**主线程侧（`SplatLoader.loadInternal` 中的 `onStatus`）：**

```typescript
const onStatus = async (data) => {
  // 进度报告
  const { loaded, total } = data as { loaded, total };
  if (loaded !== undefined && onProgress) {
    onProgress(new ProgressEvent("progress", { lengthComputable: total !== 0, loaded, total }));
  }

  // chunk 拉取请求
  if ((data as { nextChunk?: boolean }).nextChunk) {
    let chunk: Uint8Array;
    if (!readStream) {
      chunk = new Uint8Array(0); // 流已结束
    } else {
      const { done, value } = await readStream.read();
      if (done) {
        readStream.releaseLock();
        readStream = undefined;
        chunk = new Uint8Array(0);
      } else {
        chunk = value;
      }
    }
    worker.call("nextChunk", { chunk }); // 将数据块推回 Worker
  }
};
```

### `fetchWithProgress`（`src/SplatLoader.ts`）

```typescript
async function fetchWithProgress(
  request: Request,
  onProgress?: (event: ProgressEvent) => void,
) {
  const response = await fetch(request);
  const reader = response.body.getReader();
  let loaded = 0;
  const chunks: Uint8Array[] = [];

  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    chunks.push(value);
    loaded += value.length;
    if (onProgress) {
      onProgress(new ProgressEvent("progress", {
        lengthComputable: total !== 0,
        loaded,
        total,
      }));
    }
  }

  // 合并所有 chunk 为单一 ArrayBuffer
  const bytes = new Uint8Array(loaded);
  let offset = 0;
  for (const chunk of chunks) { bytes.set(chunk, offset); offset += chunk.length; }
  return bytes.buffer;
}
```

**注意：** 此函数在当前代码中**已定义但未被直接调用**（可能为历史遗留或预留接口）。实际的带进度下载在 `decodeBytesUrl` 的 `url` 模式中直接实现。

---

## 两种机制对比

| 维度 | HTTP Range 分块（LoD Paging） | ReadableStream 流式解码 |
|------|-------------------------------|------------------------|
| **下载单位** | 精确的 chunk 字节范围 | 完整文件（流式读取） |
| **是否需要预处理** | 是（`build-lod` CLI 离线构建） | 否（原始格式直接用） |
| **进度报告** | 无（chunk 粒度，完成即通知） | 有（字节级进度事件） |
| **解码时机** | chunk 完整下载后统一解码 | 边下载边解码（增量） |
| **优先级控制** | 有（`fetchPriority` 按距离排序） | 无 |
| **并发控制** | 3 路并发（`numFetchers`） | 1 文件 1 Worker（池中排队） |
| **错误处理** | 退避重试（250~750ms），无限重试 | 抛出异常，由 `onError` 处理 |
| **内存占用** | 固定 GPU 页池，LRU 淘汰 | 解码后一次性放入内存 |
| **适用文件大小** | 任意大（分块按需加载） | 中等（全部解码后放内存） |

---

## 数据流图

```
                    ┌─────────────────────────────────────────┐
                    │           SplatLoader 路径               │
                    │  load(url) / load(stream)               │
                    │           ↓                             │
                    │  workerPool.withWorker(worker => {      │
                    │    worker.call("loadPackedSplats", {    │
                    │      url, stream, chunked, ...          │
                    │    }, { onStatus })                     │
                    │  })                                     │
                    └────────────────┬────────────────────────┘
                                     │
              ┌──────────────────────▼───────────────────────┐
              │           Worker: decodeBytesUrl              │
              │                                              │
              │  Mode A: fileBytes → push(1MB chunks)        │
              │  Mode B: url → fetch() + getReader()          │
              │               → sendStatus({loaded, total})  │
              │               → decoder.push(chunk)          │
              │  Mode C: chunked → sendStatus({nextChunk})   │
              │               ← worker.call("nextChunk")     │
              │               → decoder.push(chunk)          │
              │                                              │
              │  decoder.finish() → DecodedPackedResult      │
              └─────────────────────┬────────────────────────┘
                                    │
                              PackedSplats / ExtSplats
                              (全量，放入内存)


                    ┌─────────────────────────────────────────┐
                    │           SplatPager 路径                │
                    │  PagedSplats(url: "scene.rad")          │
                    │           ↓                             │
                    │  getRadMeta()                           │
                    │    fetchRange(url, 0, 65536)   ←── HTTP Range
                    │    decode_rad_header(bytes)             │
                    │           ↓                             │
                    │  driveFetchers() [每帧]                 │
                    │    fetchDecodeChunk(chunk)              │
                    │      ├─ fetchRange(url, offset, bytes)  ←── HTTP Range（内嵌）
                    │      ├─ fetchRange(chunkUrl)            ←── GET（.radc 文件）
                    │      └─ fetch(chunkUrl(N))              ←── GET（非 RAD 分块）
                    │           ↓                             │
                    │  workerPool.withWorker → decode         │
                    │           ↓                             │
                    │  processFetched → newUploads            │
                    │  consumeLodTreeUpdates → readyUploads   │
                    │  processUploads → GPU DataArrayTexture  │
                    └─────────────────────────────────────────┘
```

---

## 关键设计细节

### 1. Worker 内 HTTP fetch

`decodeBytesUrl` 在 Worker 线程内直接调用 `fetch()`（WebWorker 支持 Fetch API）。这意味着网络 IO 和解码都在后台线程，不阻塞主线程渲染。`SplatPager` 的 `fetchDecodeChunk` 同样在 `workerPool.withWorker` 内执行，同一架构。

### 2. WASM ChunkDecoder 增量接口

WASM 解码器（`decode_to_packedsplats` / `decode_to_extsplats` / `decode_to_csplatarray` / `decode_to_gsplatarray`）暴露 `push(bytes)` + `finish()` 接口，允许分批推送数据，最终一次性完成解码。这支持了 `decodeBytesUrl` 的流式 push 模式。

### 3. Chunked 模式的 pull-push 协议

Worker（消费者）通过 `sendStatus({nextChunk: true})` 拉取数据，主线程（生产者）通过 `worker.call("nextChunk", {chunk})` 推送数据。这是一个简单的背压（backpressure）机制：Worker 每次只拉取它能处理的一块，避免内存堆积。

### 4. 两套 `fetchRange` 实现

`src/SplatPager.ts` 和 `src/worker.ts` 各有一份完全相同的 `fetchRange` 函数（私有/模块级，未共享）。前者在主线程中被 `PagedSplats` 调用，后者在 Worker 线程中使用。两者逻辑一致，可作为合并优化点。

### 5. 进度事件规范

`SplatLoader` 使用标准的 `ProgressEvent`，兼容 THREE.js `Loader` 接口：
```
{ lengthComputable: boolean, loaded: number, total: number }
```
`SplatPager` 路径无此进度事件，因为分块加载的"进度"由 LoD 树的视觉质量体现，而非字节传输量。
