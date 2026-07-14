# Spark 2.0 LoD 瓦片流式加载模块分析

> 初版：qwen3.7-max；校正：claude-opus-4-6（基于源码逐行核查，2026-07-14）

## 核心文件

| 模块 | 文件 | 职责 |
|------|------|------|
| 流式页面管理器 | `src/SplatPager.ts` | GPU 页面池管理、chunk 下载调度、LRU 淘汰、纹理上传 |
| 渲染器调度 | `src/SparkRenderer.ts` | 每帧驱动 LoD 遍历、构建 fetch 优先级队列 |
| LoD 树遍历 (WASM) | `rust/spark-rs/src/lod_tree.rs` | 基于最大堆的贪心遍历算法，屏幕空间像素尺度量 |
| 树分块 (离线) | `rust/spark-lib/src/chunk_tree.rs` | 将 LoD 树切分为 ~65536 splats 的 chunk |
| RAD 文件格式 | `rust/spark-lib/src/rad.rs` | 支持 HTTP Range 请求的流式容器格式 |
| SplatMesh | `src/SplatMesh.ts` | THREE.js Object3D，持有 `PagedSplats` 实例 |
| Worker 桥接 | `src/worker.ts` | RPC 调用 WASM 函数：traverseLodTrees、updateLodTrees 等 |
| Worker 池 | `src/SplatWorker.ts` | 最多 4 个并发的 SplatWorker 实例池（`NewSplatWorkerPool`） |
| LoD 树构建 (Quick) | `rust/spark-lib/src/tiny_lod.rs` | 快速 LoD 树算法（按需构建用） |
| LoD 树构建 (Quality) | `rust/spark-lib/src/bhatt_lod.rs` | 基于 Bhattacharyya 距离的高质量 LoD 树算法 |
| 离线构建工具 | `rust/build-lod/src/main.rs` | CLI 工具，输入 .ply/.spz，输出 .rad + .radc 分块文件 |

---

## 整体架构（类虚拟内存分页）

```
离线构建:
  输入 .ply/.spz → compute_lod_tree() → chunk_tree(分块~64K) → .RAD头 + .RADC分块文件

运行时:
  SplatMesh(paged:true)
      ↓
  PagedSplats.getRadMeta()  ← HTTP Range 请求 .RAD 头部(64KB~1MB，三档自适应)
      ↓
  SparkRenderer.driveLod()  ← 每帧调用（tryExclusive 保证单次执行）
      ↓
  SplatPager.driveFetchers()  ← 并行最多 numFetchers（默认3）路 HTTP Range 下载 chunk
      ↓
  processFetched() → allocatePage() → newUploads[]
      ↓
  consumeLodTreeUpdates() → newUploads 转 readyUploads
      ↓
  processUploads() → uploadPage()  ← 上传到 GPU DataArrayTexture 页池
      ↓
  WASM traverse_lod_trees()  ← 最大堆贪心遍历，预算受限
      ↓
  GPU 渲染 (从页池纹理读取)
```

---

## 端到端流水线

### 1. 离线管道

```
输入 .ply/.spz 文件
        |
        v
[build-lod CLI (Rust)]
  tiny_lod.rs / bhatt_lod.rs --> compute_lod_tree()
  chunk_tree.rs              --> chunk_tree() (切分为 ~64K splats/chunk)
  rad.rs                     --> RadEncoder (写入 .RAD 头 + .RADC 分块文件)
        |
        v
输出: scene-lod.rad (头文件) + scene-lod-0.radc, -1.radc, ... (分块文件)
```

### 2. 运行时管道

```
1. SplatMesh 以 { url: "scene-lod.rad", paged: true } 创建
        |
        v
2. PagedSplats.getRadMeta()
   - 尝试 HTTP Range 请求 65KB，不足则重试 256KB、1MB（三档渐进回退）
   - 在 WASM 中解码 RAD 头 (decode_rad_header)
   - 返回 RadMeta，包含 chunk 偏移/大小/可选外部 .radc 文件名
        |
        v
3. SparkRenderer.driveLod() (每帧调用，tryExclusive 防止重入)
   a. 首次创建 SplatPager（GPU 页池，平台差异：桌面 256 页 / mobile 128 页 / iOS 96 页）
   b. 通过 new_shared_lod_tree() 创建 WASM LodTree（shared 模式，共享 pagerId）
   c. 对每个 paged SplatMesh 按摄像机距离排序 → 设置 pager.fetchPriority
      - 优先放入每个 mesh 的 chunk 0（根节点，距离最近者优先）
      - 再追加 traverse 结果中触及的非 0 chunk
   d. 消费 consumeLodTreeUpdates()，发送 updateLodTrees 到 Worker
   e. 调用 pager.driveFetchers()
        |
        v
4. SplatPager.driveFetchers()
   - 遍历 fetchPriority：
     - 已加载 → 更新 LRU，标记 needed
     - 已在 fetchers/fetched 队列 → 跳过（避免重复 fetch）
     - 空闲 fetcher 槽位且未超 maxPages → 启动 fetch:
         RAD 格式内嵌 chunk：fetchRange(url, offset, bytes)（HTTP Range）
         RAD 格式外部 .radc：fetchRange(chunkUrl)（完整文件下载）
         非 RAD 格式（如 .ksplat 分块）：fetch(chunkUrl(N))（普通 GET）
         → WebWorker 解码（loadPackedSplats / loadExtSplats）
         → 成功后 push 到 fetched[]；失败退避 250~750ms 后释放槽位
   - autoDrive=true：fetch 完成后自动再次触发 driveFetchers()
        |
        v
5. SplatPager.processFetched()（fetch.finally 时调用）
   - allocatePage() 从 pageFreelist 取空闲页
     若无空闲 → allocateFreeable() 从 freeablePages 淘汰 LRU 页（同时向 lodTreeUpdates 推送淘汰通知）
   - insertSplatsChunkPage() 更新双向映射（splats/chunk ↔ page）
   - 将 packed + sh 数据 push 到 newUploads[]
   - 将 lodTree 数据 push 到 lodTreeUpdates[]
        |
        v
6. consumeLodTreeUpdates()（SparkRenderer 帧内调用）
   - 将 newUploads 整批移入 readyUploads（两阶段隔离，防止 upload 和 traverse 竞争）
   - 返回 lodTreeUpdates 供 updateLodTrees WASM 调用
        |
        v
7. SplatPager.processUploads()（SparkRenderer 帧内调用）
   - uploadPage()：将 packed + SH 数据写入 DataArrayTexture 对应 layer
   - 非 extSplats：packedTexture + shTextures[0..2]（sh1/sh2/sh3，各 uint32x2/x4/x4）
   - extSplats：packedTexture + extTexture + shTextures[0..3]（sh1/sh2/sh3a/sh3b）
        |
        v
8. SparkRenderer 消费 lodTreeUpdates
   - 调用 WASM update_lod_trees()，传入 page/chunk 映射 + LoD 树数据
   - 更新 chunk_to_page 使遍历知道哪些 chunk 已驻留
        |
        v
9. WASM traverse_lod_trees() / dynamic_traverse_lod_trees()
   - 最大堆前沿队列，种子为根 splat（rootPage 已驻留时才加入）
   - 在 maxSplats 预算内贪心展开像素尺度最大的 splat
   - 若子 chunk 未驻留 → 保留父节点作为叶子（粗粒度渲染）
   - 返回: 每个实例的索引数组 + 触及的 chunk 列表 + lastPixelLimit
        |
        v
10. SparkRenderer 处理遍历结果
    - updateLodIndices()：paged mesh 调用 paged.update()；非 paged 更新 lodInstances 纹理
    - 触及但未加载的 chunk → 追加进 pager.fetchPriority
    - 下一帧从步骤 3 重复
        |
        v
11. GPU 从页池纹理渲染 splat
    - readSplat 路径：pagedSplatTexCoord(index) 读取 DataArrayTexture
    - readSplatExt 路径：ivec3(index&255, (index>>8)&255, index>>16) 直接计算坐标
    - 索引纹理将逻辑 splat 索引映射到物理页池索引
    - 支持 SH 求值（evaluatePackedSH / evaluateExtSH）、lodOpacity 调整
```

---

## 关键数据结构

### `SplatPager` (`src/SplatPager.ts`)

- **GPU 页面池管理器**。预分配固定大小的 `DataArrayTexture` 池。
  - 桌面默认：256 页 × 65536 splats = 16M splats
  - 其他 mobile：128 页 × 65536 = 8M splats
  - iOS：96 页 × 65536 = 6M splats
- `pageFreelist` — 空闲页编号栈；`pageLru` — 已使用页的 LRU 有序集合；`freeablePages` — 当前帧可淘汰页（`driveFetchers` 计算）
- `fetchers[]` — 进行中的并行 chunk 下载（默认 3 路）
- `fetched[]` — 已完成解码待分配页的结果队列
- `fetchPriority[]` — 每帧由 `SparkRenderer.driveLod` 重建的待 fetch 优先级队列
- `newUploads[]` / `readyUploads[]` — 两阶段上传缓冲（processFetched → consume → processUploads）
- `lodTreeUpdates[]` — 待推送给 WASM 的 page/chunk 映射更新

**关键纹理（非 extSplats 模式）：**
- `packedTexture`：每 splat 4 × uint32（位置/缩放/旋转/颜色压缩编码）
- `shTextures[0]`：SH1，每 splat 2 × uint32（RG32UI）
- `shTextures[1]`：SH2，每 splat 4 × uint32（RGBA32UI）
- `shTextures[2]`：SH3，每 splat 4 × uint32（RGBA32UI）

**关键纹理（extSplats 模式，额外）：**
- `extTexture`：ext 编码第二部分，4 × uint32
- `shTextures[3]`：SH3B，第二路 SH3 纹理（sh3a + sh3b 分拆）

### `PagedSplats` (`src/SplatPager.ts`)

- 表示共享 `SplatPager` 中的单个可流式 LoD 资产。
- `getRadMeta()` — 获取并解码 RAD 头部（65KB→256KB→1MB 三档渐进请求，使用第一个成功的）
- `chunkUrl(chunk)` — 将 `-lod-0.` 替换为 `-lod-{N}.` 生成外部 .radc chunk URL
- `fetchDecodeChunk(chunk)` — 根据格式分两路：
  - **RAD 内嵌 chunk**：`fetchRange(url, offset, bytes)`（HTTP Range）
  - **RAD 外部 .radc**：`fetchRange(chunkUrl)`（整文件 GET）
  - **非 RAD 分块文件**：`fetch(chunkUrl(N))`（普通 GET，不带 Range 头）
  - 之后统一交给 `workerPool.withWorker` 中的 `loadPackedSplats` / `loadExtSplats` 解码
- `update(numSplats, indices)` — 更新每个资产的索引纹理（`RGBA32UI`，行高动态扩容）
- `fetchSplat({index, viewOrigin})` — GPU shader Dyno block，根据是否有 viewOrigin 决定是否评估球谐

### `LodTree` / `LodSplat` (Rust: `lod_tree.rs`)

```rust
struct LodTree {
    splats: Rc<RefCell<Vec<LodSplat>>>,   // 共享 splat 数组
    page_to_chunk: Vec<u32>,              // 页 → chunk 映射
    chunk_to_page: Vec<u32>,              // chunk → 页映射 (0xFFFFFFFF = 未加载)
}

struct LodSplat {
    center: [f16; 3],       // 位置坐标（f16 压缩）
    size: f16,              // 特征尺寸
    child_start: u32,       // 子节点在树中的起始索引
    child_count: u16,       // 子节点数量
}
```

### `RadMeta` (TypeScript: `defines.ts`；Rust: `rad.rs`)

```typescript
type RadMeta = {
  version: number;
  type: string;
  count: number;
  maxSh?: number;
  lodTree?: boolean;
  chunkSize?: number;
  chunks: {
    offset: number;      // chunk 在 .rad 文件中的字节偏移
    bytes: number;       // chunk 字节数
    base?: number;       // splat 基础偏移
    count?: number;      // splat 数量
    filename?: string;   // 外部 .radc 文件路径（有则独立文件，无则内嵌）
  }[];
  splatEncoding?: SplatEncoding;
};
```

---

## WASM 遍历算法核心 (`lod_tree.rs`)

```
输入: maxSplats 预算, pixelScaleLimit（每像素世界尺寸阈值）,
      每个实例的 viewToObjectCols（4x4矩阵）+ 注视点参数

算法:
  1. 最大堆种子 = 所有根 splat（rootPage 已驻留的实例）
  2. while 预算未耗尽 && 堆非空:
       弹出屏幕像素尺度最大的 splat
       if 子节点所在 chunk 已驻留 (chunk_to_page[chunk] != 0xFFFFFFFF):
           将子节点入堆
       else:
           保留当前 splat 作为叶子（粗粒度表示）
           记录该 chunk 到 touched_chunks（驱动后续 fetch）
  3. 输出: 每个实例的 splat 索引数组 + touched_chunks 列表 + lastPixelLimit

traverse 有两个变体：
  traverse_lod_trees()         — 标准模式（traverseMode: "standard"）
  dynamic_traverse_lod_trees() — 动态模式（traverseMode: "dynamic"，默认）
```

`compute_pixel_scale()` 计算每个 splat 的屏幕空间像素大小，支持注视点优化（foveation）：
- `coneFov0` — 全分辨率内锥视场角（默认 90°）
- `coneFov` — 降分辨率外锥视场角（默认 120°）
- `coneFoveate` — 外锥边缘的分辨率因子（默认 0.4）
- `behindFoveate` — 背后方向的分辨率衰减（默认 0.2）

---

## 关键设计原则

### 1. 虚拟内存分页类比

`SplatPager` 类似操作系统的虚拟内存系统。每页 65536 splats（256×256）。页表将逻辑 chunk 地址映射到物理 GPU DataArrayTexture 层。当池满时通过 LRU 策略淘汰最久未使用的页（`allocateFreeable`）。

### 2. 渐进式细化

LoD 树从粗到细遍历。根 chunk（chunk 0）优先加载，立即呈现低分辨率视图。随着更多 chunk 流入，遍历自动展开到更精细的层次。**rootPage 未驻留时该 mesh 不参与遍历**，避免空指针。

### 3. 并行下载 + 自驱动

最多 `numFetchers`（默认 3）路并发 HTTP fetch，在 Worker 池（最大 4 个 Worker 实例）中解码。`autoDrive=true` 时 fetch 完成后立即再次触发 `driveFetchers()`，无需等待下一帧。Fetch 失败有 250~750ms 随机退避后自动重试。

### 4. 注视点选择 (Foveated Selection)

遍历通过 `compute_pixel_scale()` 结合内外锥参数进行注视点优化。优先展开观察者正在注视（锥内）的 splat，对周边/背后的 splat 使用更粗粒度的表示，从而在有限预算内集中精度于视线中心。

### 5. 多物体联合遍历

多个 `SplatMesh` 对象在单次 `traverse_lod_trees()` 调用中联合遍历，共享全局 `maxSplats` 预算，在所有可见物体间最优分配 splat 精度。每个 mesh 有独立的 `lodScale` 可调整其权重。

### 6. 未驻留 chunk 的优雅降级

未驻留 chunk 中的 splat 保留父节点作为"占位叶节点"渲染——保证场景始终可视（粗粒度），同时将该 chunk 加入下帧的 fetchPriority，触发后台下载。

### 7. 两阶段上传隔离

`processFetched()` 写入 `newUploads`；帧内 `consumeLodTreeUpdates()` 原子地将 `newUploads` 移入 `readyUploads`；同帧稍后 `processUploads()` 执行实际纹理上传。这保证了 WASM 遍历和纹理上传的数据一致性，避免中间状态暴露给 GPU。

---

## 已知遗漏/待验证项

- `LodSplat` 字段类型（`f16` 等）来自文档推断，未直接读取 `lod_tree.rs` 源码，需核查
- `chunk_tree.rs`、`rad.rs` 的具体实现逻辑未展开分析
- 离线 `build-lod` CLI 的命令行参数和输出格式未分析
