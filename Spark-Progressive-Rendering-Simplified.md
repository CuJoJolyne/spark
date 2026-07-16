# Spark 渐进式渲染生命周期（精简版）

```mermaid
sequenceDiagram
    participant App as 应用层
    participant SR as SparkRenderer
    participant Pager as SplatPager
    participant Net as HTTP/CDN
    participant WASM as WASM
    participant GPU as GPU

    Note over App: 场景创建多个 SplatMesh(paged:true)
    App->>SR: 首帧渲染触发 driveLod()

    Note over SR: 初始化阶段:<br/>1. 每个 mesh 发送 HTTP Range 请求获取 .RAD 头部<br/>2. WASM 创建共享 LoD 树<br/>3. 创建 SplatPager 页池

    rect rgb(230,245,255)
        Note over SR,GPU: 第 1 帧 — 根 chunk 加载（低分辨率）

        SR->>Page: 为每个 mesh（按距离排序）请求 chunk 0
        Pager->>Net: 并行下载根 chunk（最多 3 路）
        Net-->>Pager: 编码数据到达
        Pager->>GPU: 解码 → 分配页 → 上传到 GPU 纹理池
        SR->>WASM: 更新页映射 → 遍历 LoD 树
        Note over WASM: 根 chunk 已驻留，尝试展开子节点<br/>子 chunk 未加载 → 保留父节点为占位叶
        WASM-->>SR: 输出粗粒度 splat 索引
        SR->>GPU: 渲染低分辨率场景 ✓
    end

    rect rgb(230,255,230)
        Note over SR,GPU: 第 2~N 帧 — 渐进细化

        SR->>WASM: 每帧遍历，发现更多未驻留 chunk
        SR->>Pager: 将触及的 chunk 加入 fetchPriority
        Pager->>Net: 后台并行下载新 chunk
        Net-->>Pager: 数据到达 → 分配页 → 上传 GPU
        SR->>WASM: 更新映射 → 重新遍历（展开更深层级）
        Note over WASM: 已驻留 chunk 增多<br/>遍历深入更细粒度
        SR->>GPU: 渲染精度逐步提升 ✓✓
    end

    rect rgb(255,245,230)
        Note over SR,GPU: 最终状态 — 预算饱和

        Note over WASM: 所有可见 chunk 已驻留<br/>或达到 maxSplats 预算上限
        Note over GPU: 呈现当前视角下的最优渲染质量

        Note over SR: 用户移动视角时:<br/>1. 新可见区域触发未加载 chunk 的请求<br/>2. 已不可见区域被 LRU 淘汰释放页<br/>3. 循环回到"渐进细化"阶段
    end
```

---

# 由粗到精渲染 & 触发精细 chunk 下载的代码定位

这两个机制体现在两处代码：

## 1. 由粗到精渲染 — `traverse_lod_trees()` in `rust/spark-rs/src/lod_tree.rs:481-540`

关键逻辑：遍历时用最大堆按屏幕像素尺度排序 splat，尝试展开子节点。**若子节点所在 chunk 未驻留（`chunk_to_page[chunk] == 0xFFFFFFFF`），则保留父节点为占位叶节点**：

```rust
// lod_tree.rs:522-525
if first_page == 0xFFFFFFFF || last_page == 0xFFFFFFFF {
    output.push((inst_index, paged_index));  // ← 保留父节点作粗粒度表示
    continue;
}
```

父 splat 尺寸更大、精度更粗，但保证场景始终可视。等子 chunk 下载完毕后，下次遍历 `chunk_to_page` 不再是 `0xFFFFFFFF`，便会展开到更精细的子节点 —— 这就是"由粗到精"。

## 2. 触发更精细 chunk 下载 — `SparkRenderer.ts:1500-1507`

遍历返回的 `chunks` 数组（即 WASM 中 `touched` 列表，记录遍历过程"触碰到但未驻留"的 chunk）会被追加到 `fetchPriority`：

```typescript
// SparkRenderer.ts:1500-1507
for (const [lodId, chunk] of chunks) {  // ← traverse 返回的 touched chunks
  const splats = this.lodIdToSplats.get(lodId);
  if (splats instanceof PagedSplats) {
    if (chunk !== 0) {
      this.pager.fetchPriority.push({ splats, chunk });  // ← 触发下载
    }
  }
}
```

而 WASM 中记录 touched 的代码在 `lod_tree.rs:505-513`：

```rust
// lod_tree.rs:505-513
let first_chunk = child_start >> 16;
if touched_set.insert((*lod_id, first_chunk)) {
    touched.push((*lod_id, first_chunk));  // ← 记录需要下载的 chunk
}

let last_chunk = (child_start + child_count as u32 - 1) >> 16;
if last_chunk != first_chunk && touched_set.insert((*lod_id, last_chunk)) {
    touched.push((*lod_id, last_chunk));
}
```

## 闭环流程

| 步骤 | 位置 | 作用 |
|------|------|------|
| ① 遍历尝试展开子节点 | `lod_tree.rs:527` | 检查 `chunk_to_page` |
| ② chunk 未驻留 → 保留父节点 | `lod_tree.rs:522-525` | 粗粒度渲染 |
| ③ 记录 touched chunk | `lod_tree.rs:505-513` | 标记需下载 |
| ④ 返回 touched 给 JS | `lod_tree.rs:583-588` | `out_chunks` |
| ⑤ 追加到 fetchPriority | `SparkRenderer.ts:1500-1504` | 触发下载 |
| ⑥ 下帧 chunk 驻留 → 展开子节点 | 回到 ① | 精细化 |

每帧循环 ①→⑤，chunk 不断流入，traverse 展开深度递增，渲染由粗到精。
