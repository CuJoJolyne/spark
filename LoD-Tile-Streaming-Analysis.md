# Spark 2.0 LoD 瓦片流式加载模块分析

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
| Worker 池 | `src/SplatWorker.ts` | 4 个后台线程的 Worker 池管理 |
| LoD 树构建 (Quick) | `rust/spark-lib/src/tiny_lod.rs` | 快速 LoD 树算法（按需构建用） |
| LoD 树构建 (Quality) | `rust/spark-lib/src/bhatt_lod.rs` | 基于 Bhattacharyya 距离的高质量 LoD 树算法 |
| 离线构建工具 | `rust/build-lod/src/main.rs` | CLI 工具，输入 .ply/.spz，输出 .rad + .radc 分块文件 |
| LoD 树离线构建 CLI | `rust/build-lod/src/main.rs` | 离线 LoD 树构建入口，输出 .rad 和 .radc 分块文件 |

---

## 整体架构（类虚拟内存分页）

```
离线构建:
  输入 .ply/.spz → compute_lod_tree() → chunk_tree(分块~64K) → .RAD头 + .RADC分块文件

运行时:
  SplatMesh(paged:true)
      ↓
  PagedSplats.getRadMeta()  ← HTTP Range 请求 .RAD 头部(64KB~1MB)
      ↓
  SparkRenderer.updateLod()  ← 每帧调用
      ↓
  SplatPager.driveFetchers()  ← 并行3路 HTTP Range 下载 chunk
      ↓
  processFetched() → allocatePage() → uploadPage()  ← 上传到 GPU DataArrayTexture 页池
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
   - 通过 HTTP Range 请求获取 .RAD 文件的前 64KB~1MB
   - 在 WASM 中解码 RAD 头 (decode_rad_header)
   - 返回 RadMeta，包含 chunk 偏移/大小/文件名
        |
        v
3. SparkRenderer.updateLod() (每帧调用)
   a. 创建 SplatPager（一次性），GPU 页池 (256 层 DataArrayTexture)
   b. 通过 new_lod_tree() / new_shared_lod_tree() 创建 WASM LodTree
   c. 对每个可见的 paged SplatMesh:
      - 将根 chunk (chunk 0) 加入 pager.fetchPriority，按距离排序
   d. 调用 pager.driveFetchers()
        |
        v
4. SplatPager.driveFetchers()
   - 处理 fetchPriority 队列中的每个条目：
     - 若 chunk 已加载 (在 pageLru 中) → 标记为需要，更新 LRU
     - 若未加载且有空闲 fetcher 槽位 → 启动 fetch:
       PagedSplats.fetchDecodeChunk(chunk)
         -> HTTP Range fetch (bytes=offset-offset+bytes)
         -> WebWorker 解码 chunk (loadPackedSplats/loadExtSplats)
         -> 返回 packedArray + lodTree 数据 + SH 数组
     - 数据进入 fetched[] 队列
        |
        v
5. SplatPager.processFetched()
   - allocatePage() 从空闲列表分配，或 allocateFreeable() (淘汰 LRU 页)
   - insertSplatsChunkPage() 更新 page↔chunk 映射
   - 排队页面上传 (packed 数据 + SH 数据)
   - 排队 lodTreeUpdate (WASM 的 LoD 树元数据)
        |
        v
6. SplatPager.processUploads()
   - uploadPage() 将数据复制到 DataArrayTexture 层
   - 更新 GPU 纹理 (packed, ext, SH1, SH2, SH3)
        |
        v
7. SparkRenderer 消费 lodTreeUpdates
   - 调用 WASM update_lod_trees()，传入 page/chunk 映射 + LoD 树数据
   - 更新 chunk_to_page 使遍历知道哪些 chunk 已驻留
        |
        v
8. WASM traverse_lod_trees()
   - 最大堆前沿队列，种子为根 splat
   - 在预算内贪心展开像素尺度最大的 splat
   - 若子 chunk 未驻留 → 保留父节点作为叶子（粗粒度渲染）
   - 返回: 每个实例的索引数组 + 触及的 chunk 列表
        |
        v
9. SparkRenderer 处理遍历结果
   - updateLodIndices() 更新每个 mesh 的索引纹理
   - 触及但未加载的 chunk → 加入 pager.fetchPriority
   - 下一帧从步骤 4 重复
        |
        v
10. GPU 从页池纹理渲染 splat
    - PagedSplats.fetchSplat() 通过 pagedSplatTexCoord() 读取 DataArrayTexture
    - 索引纹理将逻辑 splat 索引映射到物理页池索引
    - 支持 SH 求值、lodOpacity 调整等
```

---

## 关键数据结构

### `SplatPager` (`src/SplatPager.ts`)

- **GPU 页面池管理器**。预分配固定大小的 `DataArrayTexture` 池（默认 16M splats = 256 页 x 65,536 splats/页）。
- 维护 `pageFreelist`、`pageLru`（LRU 集合）、`freeablePages` 用于页面分配/淘汰。
- `fetchers[]` — 活跃的并行 chunk 下载（默认 3 个并发）。
- `fetchPriority[]` — 按摄像机距离排序的待 fetch chunk 有序队列。
- `driveFetchers()` — 主 fetch 循环：处理 `fetchPriority`，启动 fetch，管理 LRU 淘汰。
- `processFetched()` — 为已下载 chunk 分配页面，排队 GPU 纹理上传。
- `uploadPage()` — 将 packed splat 数据 + SH 数据上传到 `DataArrayTexture` 池。
- `consumeLodTreeUpdates()` — 生成 chunk→page 映射更新给 WASM LoD 树。
- 关键纹理：`packedTexture`（每 splat 4 x uint32）、`extTexture`（扩展编码）、`shTextures[0..3]`（球谐）。

### `PagedSplats` (`src/SplatPager.ts`)

- 表示共享 `SplatPager` 中的单个可流式 LoD 资产。
- `getRadMeta()` — 获取并解码 RAD 头部（尝试 64KB、256KB、1MB 范围请求）。
- `chunkUrl(chunk)` — 通过将 `-lod-0.` 替换为 `-lod-{N}.` 生成 chunk URL。
- `fetchDecodeChunk(chunk)` — 通过 HTTP Range 请求获取 chunk，在 WebWorker 中解码，返回 packed splat 数据 + LoD 树数据。
- `update(numSplats, indices)` — 更新每个资产的索引纹理，将逻辑 splat 索引映射到物理页池位置。
- `fetchSplat({index, viewOrigin})` — GPU shader 函数（dyno block），从分页纹理池读取 splat。

### `LodTree` / `LodSplat` (Rust: `lod_tree.rs`)

```rust
struct LodTree {
    splats: Rc<RefCell<Vec<LodSplat>>>,   // 共享 splat 数组
    page_to_chunk: Vec<u32>,              // 页 → chunk 映射
    chunk_to_page: Vec<u32>,              // chunk → 页映射 (0xFFFFFFFF = 未加载)
}

struct LodSplat {
    center: [f16; 3],       // 位置坐标
    size: f16,              // 特征尺寸
    child_start: u32,       // 子节点在树中的起始索引
    child_count: u16,       // 子节点数量
}
```

### `RadMeta` (TypeScript: `defines.ts`; Rust: `rad.rs`)

```typescript
type RadMeta = {
  version: number;
  type: string;
  count: number;
  maxSh?: number;
  lodTree?: boolean;
  chunkSize?: number;
  chunks: {
    offset: number;
    bytes: number;
    base?: number;
    count?: number;
    filename?: string;  // 外部 chunk 文件 (.radc) 的路径
  }[];
  splatEncoding?: SplatEncoding;
};
```

---

## WASM 遍历算法核心 (`lod_tree.rs`)

```
输入: 最大 splat 预算, 像素尺度阈值, 每个实例的视图矩阵 + 注视点参数

算法:
  1. 最大堆种子 = 所有根 splat
  2. while 预算未耗尽 && 堆非空:
       弹出屏幕像素尺度最大的 splat
       if 子节点所在 chunk 已驻留(page resident):
           将子节点入堆
       else:
           保留当前 splat 作为叶子（粗粒度表示）
  3. 输出: 每个实例的 splat 索引数组 + 触及的 chunk 列表（驱动后续 fetch）
```

`compute_pixel_scale()` 计算每个 splat 的屏幕空间像素大小，支持注视点优化（foveation）。

`dynamic_traverse_lod_trees()` 为替代遍历方案，支持动态模式。

---

## 关键设计原则

### 1. 虚拟内存分页类比

`SplatPager` 类似操作系统的虚拟内存系统。每页 65,536 splats。页表将逻辑 chunk 地址映射到物理 GPU 纹理层。当池满时通过 LRU 策略淘汰最久未使用的页。

### 2. 渐进式细化

LoD 树从粗到细遍历。根 chunk (chunk 0) 优先加载，立即呈现低分辨率视图。随着更多 chunk 流入，遍历自动展开到更精细的层次。

### 3. 并行下载

最多 3 路并发 HTTP fetch 在后台 WebWorker（4 线程池）中运行。Fetch 优先级按摄像机距离决定——距离更近的物体优先下载其 chunk。

### 4. 注视点选择 (Foveated Selection)

遍历通过 `compute_pixel_scale()` 结合以下参数进行注视点优化：
- `coneFov0` / `coneFov` — 注视椎体的视场角
- `coneFoveate` — 注视椎体的强度因子
- `behindFoveate` — 背后方向的衰减因子

优先展开观察者正在注视的 splat，对周边/背后的 splat 使用更粗粒度的表示。

### 5. 多物体联合遍历

多个 `SplatMesh` 对象在单次 `traverse_lod_trees()` 调用中联合遍历，共享全局 splat 预算，在所有可见物体间最优分配。

### 6. 未驻留 chunk 的优雅降级

未驻留 chunk 中的 splat 作为"占位叶节点"渲染——保证场景始终可视，同时触发后台下载。
