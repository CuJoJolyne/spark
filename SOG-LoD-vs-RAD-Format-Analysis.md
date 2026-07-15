# SOG-LoD 格式 vs RAD 格式：流式加载对比分析

> 作者：claude-opus-4-6（基于 Spark 2.0 + GSBox 源码直接分析，2026-07-15）
> 涵盖项目：`C:\Source\3DGS\spark`（Spark 2.0，World Labs/李飞飞）、`C:\Source\3DGS\gsbox`（GSBox 开源工具）

---

## 一、格式概览

### 1.1 单文件 SOG（基础格式）

单个 `.sog` 文件本质是一个 **ZIP 压缩包**，内含多张 WebP 编码的属性图像：

```
scene.sog (ZIP 文件)
├── meta.json                ← 描述文件（几KB）
├── means_hi.webp            ← 位置高8位（WebP 图像）
├── means_lo.webp            ← 位置低8位（WebP 图像）
├── scales.webp              ← 缩放（WebP 图像）
├── quats.webp               ← 旋转四元数（WebP 图像）
├── sh0.webp                 ← SH0 颜色+透明度（WebP 图像）
├── shn_centroids.webp       ← SH 高阶聚类中心（可选）
└── shn_labels.webp          ← SH 高阶 label 图（可选）
```

**`meta.json` 结构（V2 版本）**：

```json
{
  "version": 2,
  "count": 6000000,
  "means": {
    "files": ["means_hi.webp", "means_lo.webp"],
    "mins": [-2.5, -1.8, -3.2],
    "maxs": [2.5, 1.8, 3.2]
  },
  "scales": {
    "files": ["scales.webp"],
    "codebook": [-8.2, -7.9, ..., 2.1]
  },
  "quats": { "files": ["quats.webp"] },
  "sh0": {
    "files": ["sh0.webp"],
    "codebook": [-3.5, ..., 4.8]
  },
  "shN": {
    "files": ["shn_centroids.webp", "shn_labels.webp"],
    "codebook": [-1.2, ..., 1.2],
    "bands": 1
  }
}
```

**解码流水线**（`sogs.rs` 源码确认）：

```
下载完整 .sog 文件 (ZIP)
    ↓
SogsDecoder.push(bytes)     ← 仅仅是 buffer.extend_from_slice(bytes)，不做任何解码
    ↓
SogsDecoder.finish()        ← 所有工作在这一步完成（必须完整文件）
    ├─ ZipArchive::new()    ← 读取 ZIP Central Directory（在文件末尾）
    ├─ 找 meta.json → 解析 JSON
    ├─ 解压所有 .webp 文件
    ├─ decode_means()       → center = exp(lerp(mins, maxs, pixel))
    ├─ decode_scales()      → scale = exp(codebook[pixel])
    ├─ decode_quats()       → quaternion_packed 解码
    ├─ decode_sh0()         → rgb = SH_C0 * codebook[pixel] + 0.5
    ├─ decode_shn()         → SH1/2/3 聚类反查
    └─ emit_to_receiver()   → 分 65536 批次输出 PackedSplats
```

**根本限制**：ZIP Central Directory 在文件末尾 → 必须完整下载才能解压任何一张图片 → 无法增量解码。

---

### 1.2 GSBox SOG-LoD 格式（多文件 LoD 方案）

GSBox 通过外部索引 + 多文件拆分，绕过了单文件 SOG 的根本限制：

```
scene/
├── lod-meta.json            ← 空间树索引（几十KB）
├── 0_0.sog                  ← LOD 0, 分片 0（粗粒度，最多 409600 splats）
├── 0_1.sog                  ← LOD 0, 分片 1
├── 1_0.sog                  ← LOD 1（中等精度）
├── 1_1.sog                  ← LOD 1, 分片 1
├── 2_0.sog                  ← LOD 2（高精度）
├── 2_1.sog                  ← LOD 2, 分片 1
└── environment.sog          ← 可选环境/天空盒
```

**`lod-meta.json` 结构**（Go 类型定义来自 `gsplat/data-sog.go`）：

```json
{
  "lodLevels": 3,
  "filenames": ["0_0.sog", "0_1.sog", "1_0.sog", "2_0.sog"],
  "environment": "environment.sog",
  "tree": {
    "bound": {"min": [-50, -30, -20], "max": [50, 30, 20]},
    "children": [
      {
        "bound": {"min": [-50, -30, -20], "max": [0, 30, 20]},
        "lods": {
          "0": {"file": 0, "offset": 0,    "count": 1500},
          "1": {"file": 2, "offset": 500,  "count": 800},
          "2": {"file": 3, "offset": 1200, "count": 400}
        }
      },
      {
        "bound": {"min": [0, -30, -20], "max": [50, 30, 20]},
        "lods": {
          "0": {"file": 1, "offset": 0,    "count": 1600},
          "1": {"file": 2, "offset": 1300, "count": 900}
        }
      }
    ]
  }
}
```

**Go 类型定义**：

```go
type LodMeta struct {
    LodLevels   int      `json:"lodLevels"`
    Filenames   []string `json:"filenames"`
    Environment string   `json:"environment"`
    Tree        *LodNode `json:"tree"`
}

type LodNode struct {
    Bound    *Bound                  `json:"bound"`
    Children *[]*LodNode             `json:"children,omitempty"` // 内部节点
    Lods     *map[string]*LodMapping `json:"lods,omitempty"`     // 叶节点
}

type LodMapping struct {
    File   int `json:"file"`    // filenames 数组索引
    Offset int `json:"offset"`  // .sog 文件内 splat 偏移
    Count  int `json:"count"`   // splat 数量
}
```

**关键常量**（`lod-meta-cut.go`）：
```go
const FileSplatCountThreshold = 409600  // 单文件最大 splat 数
```

**B-Tree 构建算法**：
1. 递归沿最长轴（X/Y/Z）在中位数处二分
2. 叶节点 splat 数 ≤ cut_size（默认 30000）时停止
3. 叶节点按 LOD 层级分配 splat → 生成 LodMapping

---

### 1.3 Spark RAD 格式

RAD 是 Spark 专为流式加载设计的自研二进制容器：

```
scene-lod.rad              ← 索引头（64KB~1MB）+ 可选内嵌 chunk 数据
scene-lod-0.radc           ← chunk 0（根节点，65536 splats）
scene-lod-1.radc           ← chunk 1
...
scene-lod-611.radc         ← chunk N（叶节点）
```

**二进制布局**：

```
.rad 文件:
┌─────────────────────────────────────────────────────┐
│ [4B] Magic: 0x30444152 ("RAD0")                     │
│ [4B] meta_length                                    │
│ [变长] RadMeta JSON（含所有 chunk 的 offset/bytes）  │
│ [0-7B] 8字节对齐 padding                           │
├─────────────────────────────────────────────────────┤
│ Chunk 0（可选内嵌）                                 │
│ Chunk 1 ...                                         │
└─────────────────────────────────────────────────────┘

.radc 文件（外部分块模式）:
┌────────────────────────────────────────────────────────────┐
│ [4B] Magic: 0x43444152 ("RADC")                            │
│ [4B] chunk_meta_length                                     │
│ [变长] ChunkMeta JSON                                      │
│ [8B] payload_bytes                                         │
│ [gz(center)] [pad] [gz(alpha)] [pad] [gz(rgb)] [pad]      │
│ [gz(scales)] [pad] [gz(orientation)] [pad] [gz(sh...)]    │
│ [gz(child_count)] [gz(child_start)]                        │
└────────────────────────────────────────────────────────────┘
```

**`RadMeta` JSON**：

```json
{
  "version": 1,
  "type": "gsplat",
  "count": 40000000,
  "maxSh": 1,
  "lodTree": true,
  "chunkSize": 65536,
  "allChunkBytes": 823459840,
  "chunks": [
    {"offset": 0,       "bytes": 1351680},
    {"offset": 1351680, "bytes": 1294336},
    ...
  ],
  "splatEncoding": {
    "rgbMin": -0.12, "rgbMax": 1.15,
    "lnScaleMin": -12.0, "lnScaleMax": 9.0,
    "sh1Max": 2.3, "lodOpacity": true
  }
}
```

**运行时加载流程**（`SplatPager.ts`）：

```
Range: bytes=0-65535   ← 64KB 拿到完整索引
    ↓ WASM decode_rad_header()
每帧: WASM traverse_lod_trees()
    → 按屏幕像素尺度贪心展开 → fetchPriority 队列
        ↓
3路并发:
    RAD 内嵌: fetchRange(url, offset, bytes)  ← HTTP Range
    外部 .radc: fetchRange(chunkUrl)          ← 完整 GET
        ↓ 每个 chunk ~1-2MB，到达即解码
Worker: gz 解压 → 属性还原 → PackedSplats
        ↓ 立即
allocatePage → uploadPage → GPU DataArrayTexture
        ↓
WASM: 下一帧继续展开 → 场景持续细化
```

---

## 二、编码原理对比

| 维度 | SOG | RAD |
|------|-----|-----|
| **位置 (center)** | 双 WebP 图（hi+lo）→ 16bit 归一化 → 反 log 还原 | 直接存 f32/f16，gz 压缩 |
| **缩放 (scale)** | WebP 像素 → 256 级 codebook → exp() | 均匀量化到 uint8，gz 压缩 |
| **旋转 (quat)** | WebP 图像 → quaternion_packed (3+1 编码) | 直接存压缩值，gz 压缩 |
| **颜色 (rgb+α)** | WebP 图像 → codebook → SH_C0 转换 | packed uint32，gz 压缩 |
| **SH 高阶** | K-means 聚类（codebook + label 图）| 独立 gz 压缩数组 |
| **量化方法** | codebook 向量量化（256 级）| 均匀量化到 uint8/uint16 |
| **图像编码** | WebP 有损/无损（利用 2D 空间相关性）| 不用，纯字节流压缩 |
| **LoD 树存储** | 外部 JSON（lod-meta.json）| 内嵌在文件头 + chunk 内 child 信息 |

---

## 三、文件结构与索引对比

| 维度 | GSBox SOG-LoD | Spark RAD |
|------|--------------|-----------|
| **容器** | ZIP（每个 .sog）+ JSON（lod-meta）| 自研二进制容器 |
| **索引格式** | 独立 JSON 文件（lod-meta.json） | 内嵌在 .rad 文件头部 |
| **索引大小** | 几十KB~几百KB | 通常 <64KB |
| **树类型** | 二叉空间树（沿最长轴中位数切分）| N叉 LoD 树（Bhattacharyya 距离合并）|
| **节点引用** | file_index + splat_offset + count | chunk_index → byte_offset + bytes |
| **空间信息** | 每个节点有 AABB bounds | 每个 splat 有 center+size（内嵌在树中）|
| **LoD 分级** | 离散层级（"0"/"1"/"2"）| 连续像素尺度（greedy 展开）|
| **索引可达** | GET lod-meta.json（独立请求）| Range 0-65535 （从主文件取）|

---

## 四、核心设计哲学差异

**GSBox SOG-LoD：离散分层 + 空间分块**

```
LOD 0 (粗) ──→ 全场景低密度采样（~10% splats）
LOD 1 (中) ──→ 全场景中密度（~30% splats）
LOD 2 (细) ──→ 全场景高密度（~100% splats）

空间树负责：给定视锥，只加载可见区域的相应 LOD 层
切换粒度：整个 .sog 文件（40万 splats）
```

**Spark RAD：连续 LoD 树 + 父子继承**

```
Chunk 0 (根) ──→ 最重要的 65K splats（覆盖全场景粗粒度）
  ├─ Chunk 1 → 展开后显示更细的 splats
  ├─ Chunk 3 → ...

每个 splat 有明确父子关系：
  parent（粗）→ children（细）
  未加载子节点时，父节点自动作为叶节点渲染
切换粒度：单个 chunk（6.5万 splats）
```

| 对比 | GSBox SOG-LoD | Spark RAD |
|------|--------------|-----------|
| **LoD 模型** | 离散层级切换（LOD 0/1/2）| 连续像素尺度渐进 |
| **过渡方式** | 整层切换（可能有 pop/闪烁）| 逐 splat 展开（无 pop）|
| **最小加载单元** | 一个 .sog 文件（~40万 splats）| 一个 .radc chunk（~6.5万 splats）|
| **空间选择** | AABB 视锥裁剪 | 像素尺度优先级 + foveation |
| **粗粒度占位** | 高 LOD 层不显示时切到低 LOD | 父 splat 自动作为叶节点渲染 |

---

## 五、流式加载能力对比

| 能力 | 单文件 SOG | GSBox SOG-LoD | Spark RAD |
|------|-----------|--------------|-----------|
| **单独获取索引** | ❌（无索引）| ✅ GET lod-meta.json | ✅ Range 64KB |
| **网络层流式下载** | ✅ ReadableStream 可用 | ✅ 每个 .sog 可流式下载 | ✅ 同样支持 |
| **增量解码（边到边）** | ❌ finish() 才开始 | ❌ 每个 .sog 内部仍需完整 | ✅ push() → poll() 立即尝试 |
| **按需部分下载** | ❌ 必须整文件 | ✅ 只下载需要的 .sog | ✅ HTTP Range 精确下载单 chunk |
| **首帧数据量** | 整个文件（50MB+）| 最少 1 个 .sog（20-50MB）| 头部 64KB + chunk 0（1-2MB）|
| **并发下载** | 1 个文件 | ✅ 多 .sog 并行 GET | ✅ 3路并发 Range 请求 |
| **动态优先级** | ❌ | ⚠️ 格式支持，现有客户端未实现 | ✅ 每帧重建优先级队列 |
| **内存回收** | ❌ 需自行实现 | ⚠️ 需自行实现 | ✅ 内置 LRU 页池淘汰 |
| **视角自适应** | ❌ | ⚠️ 部分（AABB 裁剪）| ✅ foveated traversal 全内置 |

---

## 六、粒度对比（关键差异）

```
单文件 SOG:
  单位 = 整个 .sog 文件 = 全部 splats（可能 100M+）
  必须下载 100% 才能渲染

GSBox SOG-LoD:
  单位 = 1 个 .sog 文件 = 最多 409,600 splats ≈ 20-50MB 压缩后
  一次网络请求的最小有意义数据量：20-50MB

Spark RAD:
  单位 = 1 个 chunk = 65,536 splats ≈ 1-2MB 压缩后
  一次网络请求的最小有意义数据量：1-2MB
```

---

## 七、加载时间线模拟（40M splats 场景，100Mbps 带宽）

**GSBox SOG-LoD**：

```
T=0        GET lod-meta.json（50KB）→ 瞬间
T=0.01s    解析完毕，决定加载 LOD 0
T=0.01s    GET 0_0.sog（~30MB，约 4M splats/LOD0）
T=2.4s     0_0.sog 下载完毕
T=2.5s     ZIP 解压完毕
T=2.8s     WebP 解码完毕 → 上传 GPU
T=2.8s     首帧渲染（低精度全景）       ← 首帧：2.8s
T=2.8s     开始加载 LOD 1 相关文件
T=5-8s     LOD 1 文件陆续到齐解码
T=8-15s    LOD 2 文件陆续到齐
T=15s+     全精度渲染
           用户无论去哪里，都需要下载对应区域所有层级
```

**Spark RAD**：

```
T=0        Range: bytes=0-65535（64KB）
T=0.05s    头部解码完毕，611 个 chunk 位置全部已知
T=0.05s    开始并发下载 chunk 0 + chunk 1 + chunk 2
T=0.2s     chunk 0 到达（1.5MB）→ 解码 → 渲染
T=0.2s     首帧渲染（粗粒度全景）       ← 首帧：0.2s
T=0.5s     chunk 1,2,3 陆续到达 → 近处细化
T=2s       ~20 个 chunk → 明显细化
T=10-30s   按需持续加载 → 视线方向逐渐满精度
           用户只浏览 30% 区域 → 只下载 ~30% 数据
```

---

## 八、各自优势汇总

**单文件 SOG 的优势**：
- 压缩率最高（WebP + codebook + ZIP 三级压缩）
- 零预处理，训练框架直接输出
- 部署极简，1个文件即完整场景
- 调试方便（ZIP 可直接查看内容）

**GSBox SOG-LoD 的优势**：
- 继承 SOG 高压缩率
- 通过多文件拆分实现"按需下载部分场景"
- 离散 LOD 概念清晰，易于理解和实现
- 服务端只需静态文件 GET，无需 Range 支持
- 空间树 AABB 视锥裁剪减少无效加载
- 已有完整工具链（Go + Python 双实现）

**Spark RAD 的优势**：
- 首帧极快（64KB 索引 + 1 chunk 即可渲染）
- 无 pop 渐进细化（连续 LoD 树，父→子自然过渡）
- 最细粒度（65K splats/chunk vs 409K splats/file）
- 内存可控（固定页池 + LRU，不会 OOM）
- 完整运行时调度内置（foveation、优先级、自动驱动）
- 支持 100M+ splats 超大场景
- 可视区域优先，带宽利用率更高

---

## 九、选型决策树

```
需要在浏览器实时渲染 3DGS 场景
         ↓
场景 > 20M splats？
    ├─ 是 → RAD（SOG-LoD 文件过大，首帧等待不可接受）
    └─ 否
         ↓
    需要交互式自由浏览（用户可随意移动视角）？
        ├─ 是 → RAD（动态优先级、foveation 全内置）
        └─ 否（固定/少量视角）
              ↓
         需要无 pop 渐进加载？
             ├─ 是 → RAD
             └─ 否
                  ↓
              服务端支持 HTTP Range 请求？
                  ├─ 否 → SOG-LoD（纯静态 GET 即可）
                  └─ 是
                        ↓
                    带宽费用敏感/压缩率优先？
                        ├─ 是 → SOG-LoD（更高压缩率）
                        └─ 否 → RAD（更好体验）
```

---

## 十、技术实现参考

| 组件 | GSBox SOG-LoD | Spark RAD |
|------|--------------|-----------|
| **格式规范** | `gsplat/data-sog.go` | `rust/spark-lib/src/rad.rs` |
| **树构建** | `gsplat/lod-meta-cut.go`（`buildBTree`）| `rust/spark-lib/src/bhatt_lod.rs` |
| **文件读取** | `gsplat/lod-meta-read.go`（`ReadLodMeta`）| `src/SplatPager.ts`（`getRadMeta`）|
| **解码器** | `rust/spark-lib/src/sogs.rs`（`SogsDecoder`）| `rust/spark-lib/src/rad.rs`（`RadDecoder`）|
| **运行时调度** | 无（客户端自行实现）| `src/SplatPager.ts`（`driveFetchers`）|
| **LoD 遍历** | 无（客户端自行实现）| `rust/spark-rs/src/lod_tree.rs`（WASM）|
| **离线构建工具** | `gsbox`（Go CLI + Python）| `rust/build-lod/src/main.rs` |
| **真实示例** | — | `examples/streaming-lod/index.html` |

---

## 十一、一句话总结

| 格式 | 定位 |
|------|------|
| **单文件 SOG** | "下完再渲"——适合小场景、离线展示、带宽充裕场景 |
| **GSBox SOG-LoD** | "分层分块的离线方案"——格式支持按需加载，粒度粗（40万 splats/文件），解码仍需每文件完整，适合中等场景、固定视角 |
| **Spark RAD** | "连续流式的在线方案"——粒度细（6.5万/chunk），解码即时，运行时调度完整，适合大场景、自由浏览 |

**根本差异不在压缩算法，而在：从发出请求到首次可渲染的最小数据量和等待时间。**

- SOG-LoD：等待 1 个 .sog（20-50MB）→ 首帧
- RAD：等待 64KB 索引 + 1 个 chunk（1-2MB）→ 首帧（约快 10-15×）

---

## 十二、核心概念深解

### 12.1 HTTP Range 请求

HTTP Range 是 HTTP 协议的一个功能，允许客户端只请求文件的**一段字节**，不用下整个文件：

```http
GET /scene.rad HTTP/1.1
Range: bytes=1351680-2646015    ← 只要这 1.3MB 范围
```

服务器返回 `206 Partial Content` + 对应字节段。这个机制本身是通用的，关键在于**下载到的那段字节是否有实际意义**。

**三种格式对 HTTP Range 的利用情况：**

**单文件 SOG：❌ 协议能发，内容没意义**
```
浏览器: GET scene.sog  Range: bytes=0-65535
服务器: 返回 ZIP 文件的前 65KB

你拿到了什么？→ ZIP 的 Local File Header 片段
能解压吗？  → 不能。ZIP 必须先读末尾的 Central Directory
                才知道每个文件（WebP 图片）在哪里、多长
结论: Range 拿到的内容无法使用，必须完整下载
```

**GSBox SOG-LoD：❌ 文件级可选，字节级无意义**
```
浏览器: GET lod-meta.json      → 拿到索引，决定需要 0_0.sog
浏览器: GET 0_0.sog            → 必须完整下载（~30MB）

能对 0_0.sog 发 Range 请求吗？→ 可以发，服务器会响应
但拿到半个 .sog 能用吗？    → 不能。仍是 ZIP，中途截断同样定位不了内容
协议层支持，应用层无法利用
```

**RAD：✅ 每一段都是完整可解码的 chunk**
```
浏览器: GET scene.rad  Range: bytes=0-65535
WASM 解码 RadMeta，得知所有 chunk 的精确位置：
    chunk 0: offset=0,        bytes=1351680
    chunk 1: offset=1351680,  bytes=1294336
    chunk 2: offset=2646016,  bytes=1308672

浏览器: GET scene.rad  Range: bytes=0-1351679       → chunk 0 的全部 1.3MB
浏览器: GET scene.rad  Range: bytes=1351680-2646015 → chunk 1 的全部 1.3MB
浏览器: GET scene.rad  Range: bytes=2646016-3954687 → chunk 2 的全部 1.3MB

每次 Range 请求精确对应一个完整 chunk，到达即可独立解码渲染。
```

**关键差异的根源：**

| 格式 | 文件内结构 | 最小可用字节单元 |
|------|-----------|----------------|
| SOG | ZIP：Central Directory 在末尾，文件位置未知 | 整个文件 |
| RAD | 索引在开头，每个 chunk 自描述（含长度）| 单个 chunk（1-2MB）|

---

### 12.2 单文件内增量解码

这个概念问的是：**当同一个文件的字节还在源源不断到来时，已经到达的字节能否被解码并渲染出来？**

注意这里说的不是"多个文件分批加载"，而是**同一个文件的字节流**是否可以边收边用。

**SOG：❌ 必须等整个文件**

```
ReadableStream 字节逐渐到达：
  到达  1MB → push 进 SogsDecoder → buffer.extend(bytes)，什么都没发生
  到达  5MB → push 进 SogsDecoder → buffer.extend(bytes)，什么都没发生
  到达 10MB → push 进 SogsDecoder → buffer.extend(bytes)，什么都没发生
  到达 30MB → 文件完整！→ SogsDecoder.finish() 被调用
               → ZipArchive::new(cursor)   ← 现在才开始读 Central Directory
               → 解压 WebP 图片
               → 解码所有属性
               → 上传 GPU → 渲染
```

Rust 源码（`sogs.rs`）直接证明这一点：

```rust
impl<T: SplatReceiver> ChunkReceiver for SogsDecoder<T> {
    fn push(&mut self, bytes: &[u8]) -> anyhow::Result<()> {
        self.buffer.extend_from_slice(bytes);  // ← 只存不解
        Ok(())
    }
    fn finish(&mut self) -> anyhow::Result<()> {
        decode_sogs(&self.buffer, ...)?;  // ← 所有工作在这里
        Ok(())
    }
}
```

**为什么 ZIP 结构决定了这个命运？**

```
ZIP 文件布局（简化）:
┌──────────────────────────────────────┐
│ [文件数据 1 - WebP]                  │ ← 字节最先到达
│ [文件数据 2 - WebP]                  │
│ ...                                  │
│ [文件数据 N - WebP]                  │
├──────────────────────────────────────┤
│ [Central Directory Entry 1]          │ ← 记录每个文件的位置和名字
│ [Central Directory Entry 2]          │    必须先读这里才知道上面哪段是什么
│ ...                                  │
│ [End of Central Directory Record]    │ ← 在文件最末尾
└──────────────────────────────────────┘

在收到 End of Central Directory 之前：
  → 不知道 means_hi.webp 从哪里开始
  → 不知道 scales.webp 在哪里
  → 无法解压任何一张 WebP
  → 无法解码任何一个属性
  → 无法渲染任何一个 splat
```

**RAD：✅ 状态机驱动，到一块解一块**

```
ReadableStream 字节逐渐到达：
  到达  1KB → push → RadDecoder.poll()：header 够了吗？不够，等
  到达  8KB → push → poll()：header 完整！解析 RadMeta，
                               知道 chunk 0 在 offset=0, bytes=1.3MB
  到达  1MB → push → poll()：chunk 0 完整了吗？还差 0.3MB...
  到达 1.3MB→ push → poll()：chunk 0 字节到齐！立即解码：
                               gz 解压 center/scale/quat/rgb...
                               上传 GPU → 渲染这 65K splats！
                               ← 此时文件剩余 600+ 个 chunk 还没下载
  到达 2.6MB→ push → poll()：chunk 1 也到齐了！继续解码渲染...
  ...
```

Rust 源码（`rad.rs`）的 `poll()` 是状态机：

```rust
impl<T: SplatReceiver> ChunkReceiver for RadDecoder<T> {
    fn push(&mut self, bytes: &[u8]) -> anyhow::Result<()> {
        self.buffer.extend_from_slice(bytes);
        self.poll()?;  // ← 每次 push 后立即尝试解码
        Ok(())
    }
    // poll() 检查 buffer 里是否有完整的 header 或 chunk，
    // 有就解，没有就返回等待更多字节
}
```

**为什么 RAD 结构可以增量？**

```
RAD 文件布局：
┌──────────────────────────────────────┐
│ [Magic: "RAD0"]                      │ ← 4 字节，文件最开始
│ [meta_length]                        │ ← 4 字节，告诉你 JSON 多长
│ [RadMeta JSON]                       │ ← 包含所有 chunk 的 offset+bytes
│ [padding]                            │
├──────────────────────────────────────┤
│ [chunk 0: RADC header + gz 数据]     │ ← 自描述，含自身长度
│ [chunk 1: RADC header + gz 数据]     │
│ ...                                  │
└──────────────────────────────────────┘

在收到前 8KB（通常足够）之后：
  → 知道 RadMeta JSON 的完整内容
  → 知道每个 chunk 的精确位置和字节数
  → 每当某 chunk 的全部字节到达，立即可以独立解码
  → 解码不依赖其他任何 chunk
```

---

### 12.3 两个概念联合作用

这两个特性一起，解释了 RAD "64KB + 1.3MB → 首帧"的完整路径：

```
┌─────────────────────────────────────────────────────────────────┐
│  HTTP Range 的贡献：只请求，只传输，只等待  需要的那 1.3MB      │
│  增量解码的贡献：  那 1.3MB 到齐后立刻解码，无需等其他数据     │
└─────────────────────────────────────────────────────────────────┘

如果只有 Range 而没有增量解码：
  → 我确实只下载了 1.3MB 的 chunk 0
  → 但解码器说："我需要整个文件才能开始工作"
  → 还是无法渲染                                ← 毫无意义

如果只有增量解码而没有 Range：
  → 解码器确实能边收边解
  → 但浏览器在下载整个 1GB 文件，数据按顺序到达
  → chunk 0 到齐了渲染，但后台还在传输 999MB...
  → 带宽没有节省，其他 chunk 也一直在占用下载通道

两者结合：
  → Range：精确选择需要的 chunk，不多不少
  → 增量：chunk 到齐即渲染，不等其他
  → 结果：带宽精准 + 渲染及时 → 真正的按需流式
```
