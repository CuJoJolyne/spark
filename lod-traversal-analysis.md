# Spark 3DGS 大场景 LoD 层级切换方案分析

## 核心设计思想：无固定距离阈值，基于屏幕空间像素尺度的贪心遍历

Spark **不使用传统的离散固定距离阈值**来做 LoD 切换，而是采用一个**连续的屏幕空间度量 + 优先级队列贪心算法**来决定每个节点何时细分为子节点。

---

## 1. 核心度量函数 `compute_pixel_scale`

**文件**: `rust/spark-rs/src/lod_tree.rs:601-629`

```rust
fn compute_pixel_scale<'a>(
    splat: &LodSplat,
    instance: &(u32, Ref<'a, Vec<LodSplat>>, &Vec<u32>, &Vec<u32>, Vec3A, Vec3A, f32, f32, f32, f32, f32),
) -> f32 {
    let &(_, _, _, _, origin, forward, lod_scale, behind_foveate, cone_foveate, cone_dot0, cone_dot) = instance;
    let center = splat.center();
    let delta = center - origin;
    let distance = delta.length().max(1.0e-6);
    let inv_distance = 1.0 / distance;
    let pixel_scale = splat.size() * inv_distance;
    let pixel_scale = pixel_scale * lod_scale;

    let forward_dot = delta.dot(forward);
    let foveate = if forward_dot <= 0.0 {
        behind_foveate
    } else {
        let dot = forward_dot * inv_distance;
        if dot >= cone_dot0 {
            1.0
        } else if dot >= cone_dot {
            let t = (dot - cone_dot) / (cone_dot0 - cone_dot);
            cone_foveate + (1.0 - cone_foveate) * t
        } else {
            let t = dot / cone_dot;
            behind_foveate + (cone_foveate - behind_foveate) * t
        }
    };
    foveate * pixel_scale
}
```

公式：

```
pixel_scale = (splat.feature_size × lod_scale / distance_to_camera) × foveation
```

- `splat.feature_size`：高斯体的特征尺寸（合并后的包围尺度）
- `distance_to_camera`：到相机的欧氏距离
- `lod_scale`：每个 mesh 的精细度缩放因子（`SplatMesh.lodScale`，默认 1.0）
- `foveation`：注视区域衰减系数（0~1），基于视线锥角进行线性插值

### 注视区衰减 (Foveation) 三段模型

| 区域 | 条件 | foveate 值 |
|------|------|-----------|
| 内锥（清晰区） | `dot >= cos(coneFov0/2)` | 1.0 |
| 外锥过渡 | `cos(coneFov/2) <= dot < cos(coneFov0/2)` | 从 `coneFoveate` 线性到 1.0 |
| 锥外/背后 | `dot < cos(coneFov/2)` | 从 `behindFoveate` 线性到 `coneFoveate` |
| 正背后 | `forward_dot <= 0` | `behindFoveate`（默认 0.2） |

---

## 2. 贪心遍历算法 `traverse_lod_trees`

**文件**: `rust/spark-rs/src/lod_tree.rs:481-540`

用一个 **max-heap（最大堆）** 按 `pixel_scale` 排序所有候选节点，贪心地展开最大像素尺度的节点：

```
WHILE frontier 非空:
    取堆顶 pixel_scale 最大的节点 N
    IF pixel_scale(N) <= pixelScaleLimit → 停止（所有剩余都已小于 1 像素）
    IF N 无子节点 → 输出 N 作为叶片
    IF 展开后总 splat 数 > maxSplats → 停止（预算耗尽）
    IF 子节点的 chunk 未加载 → 保持 N 为占位叶片
    ELSE → 展开 N，将子节点压入堆中
```

三个停止条件：

1. **像素尺度停止**：所有 frontier 节点的 `pixel_scale <= pixelScaleLimit`，说明剩余节点在屏幕上小于 1 像素
2. **叶片停止**：当前节点无子节点（已是叶子），直接输出
3. **预算停止**：展开当前节点会导致总 splat 数超过 `maxSplats`

**隐含切换距离**：一个特征尺寸为 `S` 的节点在距离 `D = S × lod_scale × foveation / pixelScaleLimit` 时会被展开为子节点。

---

## 3. `pixelScaleLimit` 的计算

**文件**: `src/SparkRenderer.ts:1147-1161`

```typescript
let pixelScaleLimit = 0.0;
if (camera instanceof THREE.PerspectiveCamera) {
    const tanYfov = Math.tan((0.5 * camera.fov * Math.PI) / 180);
    pixelScaleLimit = (2.0 * tanYfov) / this.renderSize.y;
} else if (camera instanceof THREE.OrthographicCamera) {
    const viewHeight = (camera.top - camera.bottom) / camera.zoom;
    const viewWidth = (camera.right - camera.left) / camera.zoom;
    const pxY = viewHeight / Math.max(1, this.renderSize.y);
    const pxX = viewWidth / Math.max(1, this.renderSize.x);
    pixelScaleLimit = Math.min(pxX, pxY);
}
pixelScaleLimit *= this.lodRenderScale;
```

对于透视相机：`pixelScaleLimit = (2 × tan(fov/2)) / screen_height × lodRenderScale`

这代表**屏幕上 1 个像素对应的世界空间尺寸**。`lodRenderScale` 是可调节的乘数（默认 1.0，增大则提前停止细化）。

---

## 4. 预算控制（隐式控制切换距离）

**文件**: `src/SparkRenderer.ts:1143-1145`

```typescript
const splatCount = this.lodSplatCount ?? defaultSplatCount;
const maxSplats = splatCount * this.lodSplatScale;
```

平台默认 `lodSplatCount`（`defaultSplatTarget()` at line 1122）：

| 平台 | 默认值 |
|------|--------|
| Oculus | 500,000 |
| Vision Pro | 750,000 |
| Android | 1,000,000 |
| iOS | 1,500,000 |
| Desktop | 2,500,000 |

预算越大 → 贪心遍历可展开更多层级 → 隐式的切换距离更近。

---

## 5. 重遍历触发条件（Dirty Check）

**文件**: `src/SparkRenderer.ts:1174-1189`

```typescript
if (this.lastLod) {
    if (
        this.lastLod.pixelScaleLimit !== pixelScaleLimit ||
        this.lastLod.maxSplats !== maxSplats
    ) {
        this.lodDirty = true;
    }

    const distance = viewPos.distanceTo(this.lastLod.pos);
    const distanceRamp = Math.max(0.0, 1.0 - distance / 1.0);
    const dot = viewQuat.dot(this.lastLod.quat);
    const quatRamp = Math.max(0.0, 1.0 - (1.0 - dot) / 0.01);
    const similarity = distanceRamp * quatRamp;
    if (similarity < 0.999) {
        this.lodDirty = true;
    }
}
```

触发条件：

- `pixelScaleLimit` 或 `maxSplats` 变化 → 立即触发
- 相机移动超过约 **1 世界单位** → `distanceRamp` 归零
- 旋转约 **~8°**（quaternion dot ≈ 0.99）→ `quatRamp` 归零
- `similarity < 0.999` → 触发重遍历

---

## 6. 流式 LoD（Streaming LOD）的距离感知

**文件**: `src/SplatPager.ts`, `rust/spark-lib/src/chunk_tree.rs`

对于大规模场景的流式加载，使用 GPU 页面池（`SplatPager`）：

- 树被切分为 **64K splats/chunk**（`BATCH_SIZE = 65536`）
- 按距离优先级下载 chunk，近距离的高优先级 chunk 先加载
- **未加载的 chunk 导致父节点保持为占位叶片** — 即远处场景先以粗糙 LoD 渲染，chunk 加载完后逐步细化
- 默认 GPU 页面池：Desktop 16M splats、iOS 6.3M、移动端 8.4M
- 并行下载数：`numLodFetchers = 3`

流式场景下的遍历停止条件增加了第四种：

4. **Chunk 未驻留停止**：子节点所在的 chunk 尚未从服务器下载完毕（`chunk_to_page[child] == 0xFFFFFFFF`），父节点作为占位保留

---

## 7. 全部可调参数汇总

### 全局参数（`SparkRenderer`）

| 参数 | 类型 | 默认值 | 作用 |
|------|------|--------|------|
| `enableLod` | `boolean` | `true` | 主开关 |
| `enableDriveLod` | `boolean` | `= enableLod` | 是否每帧驱动 LoD 更新 |
| `lodSplatCount` | `number?` | 平台自适应 | Splat 预算上限 |
| `lodSplatScale` | `number` | `1.0` | 预算乘数 |
| `lodRenderScale` | `number` | `1.0` | 最小像素阈值乘数（越大→越早停止细化） |
| `lodInflate` | `boolean` | `false` | 对 alpha < 1.0 的 splat 进行膨胀 |
| `lodTraverseMode` | `"standard" \| "dynamic"` | `"standard"` | 遍历算法变体 |
| `behindFoveate` | `number` | `0.2` | 背后区域分辨率衰减（0.2 = 仅 20%） |
| `coneFov0` | `number` (度) | `90` | 全分辨率内锥角 |
| `coneFov` | `number` (度) | `120` | 衰减外锥角 |
| `coneFoveate` | `number` | `0.4` | 外锥边缘分辨率衰减 |
| `maxPagedSplats` | `number` | 16M (桌面) / 6.3M (iOS) / 8.4M (移动) | 流式 GPU 页面池大小 |
| `numLodFetchers` | `number` | `3` | 并行 chunk 下载数 |
| `pagedExtSplats` | `boolean` | `false` | 使用扩展编码流式 splat |
| `lodPosOverride` | `Vector3?` | `undefined` | 覆盖 LoD 计算使用的相机位置 |
| `lodQuatOverride` | `Quaternion?` | `undefined` | 覆盖 LoD 计算使用的相机朝向 |

### 每 Mesh 参数（`SplatMesh`）

| 参数 | 类型 | 默认值 | 作用 |
|------|------|--------|------|
| `lod` | `boolean \| "quality"` | `false` | 启用按需 LoD 构建；数值覆盖树底数（1.1-2.0） |
| `lodAbove` | `number` | `0` | 仅当 splat 数量超过此值才构建 LoD |
| `nonLod` | `boolean` | `false` | 同时保留原始非 LoD 数据 |
| `enableLod` | `boolean?` | `undefined` | 强制启用/禁用此 mesh 的 LoD 渲染 |
| `lodScale` | `number` | `1.0` | 精细度缩放（2.0 = 同等距离要求 2x 更高的细节） |
| `behindFoveate` | `number?` | `undefined` | 覆盖全局背后衰减 |
| `coneFov0` | `number?` | `undefined` | 覆盖全局内锥角 |
| `coneFov` | `number?` | `undefined` | 覆盖全局外锥角 |
| `coneFoveate` | `number?` | `undefined` | 覆盖全局外锥衰减 |
| `paged` | `boolean \| PagedSplats` | `false` | 启用流式 LoD |

### 构建时参数（`worker.ts` / `quick_lod.rs` / `chunk_tree.rs`）

| 参数 | 默认值 | 所在文件 | 作用 |
|------|--------|----------|------|
| `lodBase` | `1.5`（quick）/ `1.25`/`1.75`（quality） | `worker.ts` | 指数底数，控制层级间特征尺寸衰减倍率 |
| `BATCH_SIZE` | `65536` (64K) | `chunk_tree.rs:11` | 流式 chunk 内目标 splat 数 |
| `MIN_BATCH_SIZE` | `8192` (8K) | `chunk_tree.rs:12` | 最小 chunk 大小 |
| `SLICE_FACTOR` | `3.0` | `chunk_tree.rs:14` | 切片间特征尺寸衰减因子 |
| `CHUNK_LEVELS` | `2` | `quick_lod.rs:9` | 每个 chunk 包含的树层级数 |
| `STD_DEVS` | `1.5` | `chunk_tree.rs:13` | chunk 包围盒标准差 |
| `MAX_SPLAT_CHUNK` | `65536` | `lod_tree.rs:11` | 每个 page/chunk 最大 splat 数 |

---

## 8. 数据流向总览

```
┌─────────────────────────────────────────────────────────┐
│                    SparkRenderer.driveLod()              │
│                                                         │
│  1. 计算 pixelScaleLimit = 2tan(fov/2)/height × scale   │
│  2. 计算 maxSplats = lodSplatCount × lodSplatScale      │
│  3. Dirty check: 位移>1u or 旋转>8° or 参数变化         │
│  4. 收集可见 LoD meshes + 各自参数                      │
│  5. 初始化未注册的 LoD tree (initLodTree)                │
│  6. 处理流式 chunk 更新                                  │
│  7. IF dirty → updateLodInstances() ──┐                 │
└───────────────────────────────────────┼─────────────────┘
                                        ▼
┌─────────────────────────────────────────────────────────┐
│              WASM Worker (lod_tree.rs)                   │
│                                                         │
│  traverse_lod_trees():                                   │
│    对每个 instance: 计算 viewToObject 矩阵              │
│    构建 max-heap，root 节点的 pixel_scale 入堆          │
│    贪心展开:                                            │
│      compute_pixel_scale() 考虑距离+lod_scale+foveation │
│      > pixelScaleLimit ? 展开子节点 : 输出为叶片        │
│      子 chunk 未驻留 (0xFFFFFFFF) ? 保留父节点          │
│    输出每个 instance 的渲染 indices + touched chunks     │
└─────────────────────────────────────────────────────────┘
                                        │
                                        ▼
┌─────────────────────────────────────────────────────────┐
│              GPU 渲染                                    │
│                                                         │
│  根据 indices 从 paged/packed GPU buffer 中提取 splats   │
│  进行排序 + 光栅化渲染                                   │
└─────────────────────────────────────────────────────────┘
```

---

## 9. 总结

Spark 的 LoD 切换方案是一个 **"预算约束下的屏幕空间贪心展开"** 模型：

1. **没有硬编码距离阈值**：切换距离由 `pixel_scale = size × lod_scale × foveation / distance` 与 `pixelScaleLimit` 的比较动态决定
2. **`pixelScaleLimit` 由屏幕分辨率和 FOV 自动计算**：适应不同分辨率和相机配置
3. **注视区 foveation**：视野外围和背后自动降分辨率，减少不关注区域的 splat 预算占用
4. **预算约束**：`maxSplats` 限制了总 splat 数，在有限 GPU 资源下做最优分配
5. **流式渐进渲染**：chunk 加载状态实现"有数据就细化，没数据就保持粗糙"的距离无关渐进渲染
6. **非整数 LoD 底数**：使用 `lodBase = 1.5` 而非 2.0，在层级间创建更多中间 LoD 级别，实现更平滑的过渡
7. **脏检查优化**：仅在相机位移 >1 世界单位或旋转 >8° 时才触发重遍历，避免不必要的计算开销
