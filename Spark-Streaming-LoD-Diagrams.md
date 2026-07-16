# Spark 流式下载 + 渐进式加载 流程图与时序图

## 1. 端到端系统流程图

```mermaid
flowchart TD
    subgraph OFFLINE["🔨 离线构建"]
        A[输入 .ply/.spz] --> B[compute_lod_tree<br/>tiny_lod.rs / bhatt_lod.rs]
        B --> C[chunk_tree<br/>切分 ~64K splats/chunk]
        C --> D[RadEncoder<br/>写入 .RAD 头 + .RADC 分块]
        D --> E["scene-lod.rad (头)<br/>scene-lod-0.radc<br/>scene-lod-1.radc ..."]
    end

    subgraph RUNTIME["🖥 运行时流式加载"]
        F["SplatMesh({paged:true})"] --> G["PagedSplats 创建"]
        G --> H["getRadMeta()<br/>HTTP Range 65KB→256KB→1MB 三档"]
        H --> I["RadMeta<br/>{chunks[], count, maxSh}"]
        I --> J["SparkRenderer.driveLod()"]
        J --> K["创建 SplatPager<br/>GPU 页池 256×64K"]
        K --> L["WASM new_lod_tree /<br/>new_shared_lod_tree"]
        L --> M["consumeLodTreeUpdates()<br/>+ updateLodTrees()"]
        M --> N["traverse_lod_trees()<br/>最大堆贪心遍历"]
        N --> O["updateLodIndices()<br/>更新索引纹理"]
        O --> P["构建 fetchPriority<br/>根chunk优先 + 触及chunk"]
        P --> Q["SplatPager.driveFetchers()"]
        Q --> R["PagedSplats.fetchDecodeChunk()<br/>HTTP Range / 整文件"]
        R --> S["WebWorker 解码"]
        S --> T["processFetched()<br/>allocatePage + insertChunk"]
        T --> U["uploadPage()<br/>写入 DataArrayTexture"]
        U --> V["GPU 渲染<br/>pagedSplatTexCoord → texelFetch"]
    end

    OFFLINE -->|"产出 .rad + .radc"| RUNTIME
```

## 2. 每帧驱动 LoD 主循环时序图

```mermaid
sequenceDiagram
    participant RF as 渲染帧<br/>render()
    participant SR as SparkRenderer
    participant SP as SplatPager
    participant SM as SplatMesh
    participant WK as WebWorker池<br/>(4线程)
    participant WASM as WASM<br/>lod_tree.rs

    RF->>SR: driveLod({visibleGenerators, camera})
    
    Note over SR: 1. 计算 pixelScaleLimit<br/>和 maxSplats 预算
    
    SR->>SR: 收集 lodMeshes（带 LoD 的 mesh）
    SR->>SR: ensureLodWorker().tryExclusive()

    alt 首次且有 paged mesh
        SR->>SP: new SplatPager({maxSplats})
        SR->>WK: call("newLodTree", {capacity})
        WK-->>SR: {lodId: pagerId}
    end

    alt 新 mesh 出现
        loop 每个新 mesh
            SR->>WK: call("newSharedLodTree", {lodId: pagerId})
            WK-->>SR: {lodId: instanceLodId}
        end
    end

    SR->>SP: consumeLodTreeUpdates()
    SP-->>SR: {updates[], 转移 newUploads→readyUploads}

    alt updates 非空
        SR->>WK: call("updateLodTrees", {ranges})
        Note right of WK: WASM update_lod_trees()<br/>更新 page_to_chunk / chunk_to_page
        WK-->>SR: done
    end

    alt lodDirty == true
        SR->>SR: 构建 instances[]（视图矩阵 + foveation 参数）
        SR->>WK: call("traverseLodTrees", {maxSplats, pixelScaleLimit, instances, traverseMode})
        Note right of WK: WASM traverse_lod_trees()
        WK-->>SR: {keyIndices, chunks, pixelLimit}

        SR->>SR: updateLodIndices(uuidToMesh, keyIndices)
        SR->>SM: paged.update(numSplats, indices)<br/>更新索引纹理

        SR->>SP: processUploads()<br/>readyUploads → uploadPage()
        
        Note over SR: 构建 fetchPriority:
        SR->>SR: ① 每 paged mesh 的 chunk 0（按距离排序）
        SR->>SR: ② traverse 返回的 touched chunks（跳过 chunk 0）
        SR->>SP: fetchPriority = [...]

        alt enableLodFetching
            SR->>SP: driveFetchers()
        end
    end

    SR->>WK: call("disposeLodTree"...)<br/>cleanupLodTrees（超时3s未触及的树）
```

## 3. Chunk 并行下载 + 页面分配时序图

```mermaid
sequenceDiagram
    participant SR as SparkRenderer
    participant SP as SplatPager
    participant PG as PagedSplats
    participant HTTP as HTTP/CDN
    participant WP as WorkerPool
    participant WK as WebWorker

    SR->>SP: driveFetchers()

    loop 遍历 fetchPriority
        alt chunk 已在 pageLru
            Note over SP: 更新 LRU ordering，标记 needed
        else chunk 已在 fetchers[] 或 fetched[]
            Note over SP: 跳过（避免重复下载）
        else fetchers.length < numFetchers && numPages < maxPages
            SP->>SP: fetchers.push({splats, chunk, promise})
            
            par 并行 fetcher 1
                SP->>PG: fetchDecodeChunk(chunk=0)
                PG->>PG: await getRadMeta()
                PG->>HTTP: GET scene.rad (Range: bytes=offset—offset+bytes)
                HTTP-->>PG: Uint8Array (chunk 数据)
                PG->>WP: withWorker(worker => worker.call("loadPackedSplats"))
                WP->>WK: loadPackedSplats({fileBytes, pathName})
                WK-->>WP: PackedResult {packedArray, extra{lodTree, sh1, sh2, sh3}}
                WP-->>PG: result
                PG-->>SP: PackedResult
                Note over SP: this.fetched.push({splats, chunk, data})

            and 并行 fetcher 2
                SP->>PG: fetchDecodeChunk(chunk=1)
                PG->>HTTP: GET scene-lod-1.radc (整文件)
                HTTP-->>PG: Uint8Array
                PG->>WP: withWorker(worker => loadPackedSplats)
                WK-->>WP: PackedResult
                WP-->>PG: result
                PG-->>SP: PackedResult
                Note over SP: this.fetched.push(...)

            and 并行 fetcher 3
                SP->>PG: fetchDecodeChunk(chunk=2)
                PG->>HTTP: GET scene.rad (Range: bytes=offset—offset+bytes)
                HTTP-->>PG: Uint8Array
                PG->>WP: loadPackedSplats
                WK-->>WP: PackedResult
                WP-->>PG: result
                PG-->>SP: PackedResult
            end

            SP->>SP: fetchers.pop(completed)
            SP->>SP: processFetched()
            
            Note over SP: autoDrive=true → fetch完成后<br/>立即再次 driveFetchers()
        end
    end
```

## 4. processFetched → GPU 上传 详细时序图

```mermaid
sequenceDiagram
    participant SP as SplatPager
    participant TEX as DataArrayTexture<br/>(页池)

    Note over SP: fetch.finally() 触发 processFetched()

    loop while fetched[] 非空
        SP->>SP: fetched.shift()
        Note over SP: const {splats, chunk, data} = fetched

        alt pageFreelist 非空
            SP->>SP: page = pageFreelist.shift()
        else freeablePages 非空（LRU 淘汰）
            SP->>SP: page = allocateFreeable()
            SP->>SP: removeSplatsChunkPage(旧splats, 旧chunk, page)
            SP->>SP: lodTreeUpdates.push({page, chunk, numSplats}) ← 淘汰通知
        else 无可用页
            Note over SP: return（停止处理，等待下帧）
        end

        SP->>SP: insertSplatsChunkPage(splats, chunk, page, now)
        SP->>SP: splatsChunkToPage.set(chunk → {page, lru})
        SP->>SP: pageToSplatsChunk[page] = {splats, chunk}
        SP->>SP: pageLru.add({page, lru: now})

        SP->>SP: lodTreeUpdates.push({splats, page, chunk, numSplats, lodTree})

        Note over SP: 组装 PageUpload
        SP->>SP: newUploads.push({page, packedArray, shArrays, extArray?})
    end

    Note over SP: ── 帧内稍后：consumeLodTreeUpdates() ──
    SP->>SP: lodTreeUpdates → 返回给 SparkRenderer
    SP->>SP: newUploads 整体移入 readyUploads

    Note over SP: ── 帧内稍后：processUploads() ──
    loop while readyUploads 非空
        SP->>SP: readyUploads.shift()
        SP->>SP: uploadPage(page, packedArray, shArrays, extArray)
        SP->>TEX: packedTexture: array.subarray(dstOffset...).set(packedArray)
        SP->>TEX: packedTexture.addLayerUpdate(layer)
        alt extSplats
            SP->>TEX: extTexture: 同上
        end
        loop i = 0..shArrays.length
            SP->>TEX: shTextures[i]: 写入 SH 数据
        end
    end
```

## 5. WASM traverse_lod_trees 算法流程图

```mermaid
flowchart TD
    START([traverse_lod_trees 入口]) --> INIT["初始化:<br/>frontier = BinaryHeap (最大堆)<br/>output = []<br/>touched = []<br/>num_splats = 0"]

    INIT --> SEED["种子阶段: 遍历所有 instance<br/>取 rootPage 对应的根 splat"]
    SEED --> COMPUTE_SEED["compute_pixel_scale(root_splat)<br/>= size/distance × lod_scale × foveate"]
    COMPUTE_SEED --> PUSH_SEED["frontier.push((pixel_scale, inst, root_index))<br/>num_splats += 1<br/>touched.push((lodId, chunk=0))"]

    PUSH_SEED --> LOOP_HEAD{{"frontier 非空<br/>且 num_splats ≤ max_splats?"}}

    LOOP_HEAD -->|No| DRAIN["drain frontier 中剩余元素<br/>→ output"]
    LOOP_HEAD -->|Yes| PEEK["peek 堆顶:<br/>(pixel_scale, inst, paged_index)"]

    PEEK --> CHECK_SCALE{"pixel_scale ≤<br/>pixel_scale_limit?"}
    CHECK_SCALE -->|Yes| DRAIN

    CHECK_SCALE -->|No| CHECK_LEAF{"child_count == 0?"}
    CHECK_LEAF -->|Yes| POP_LEAF["pop + output.push(inst, paged_index)<br/>leaf_count += 1"]
    POP_LEAF --> LOOP_HEAD

    CHECK_LEAF -->|No| CHECK_BUDGET{"num_splans - 1 + child_count<br/>> max_splats?"}
    CHECK_BUDGET -->|Yes| BREAK["break（预算耗尽）"]

    CHECK_BUDGET -->|No| POP_EXPAND["pop 当前 splat"]
    POP_EXPAND --> RECORD_TOUCH["touched.push(first_chunk, last_chunk)"]

    RECORD_TOUCH --> CHECK_RESIDENT{"chunk_to_page[first_chunk]<br/>== 0xFFFFFFFF<br/>|| chunk_to_page[last_chunk]<br/>== 0xFFFFFFFF?"}

    CHECK_RESIDENT -->|Yes 未驻留| KEEP_LEAF["output.push(inst, paged_index)<br/>（保留父节点作占位叶）"]
    KEEP_LEAF --> LOOP_HEAD

    CHECK_RESIDENT -->|No 已驻留| EXPAND_CHILDREN["遍历 child_start..child_start+child_count<br/>对每个 child:<br/>  pagedIdx = (page<<16) | (child & 0xFFFF)<br/>  ps = compute_pixel_scale(child)<br/>  if ps ≤ limit → output<br/>  else → frontier.push"]

    EXPAND_CHILDREN --> UPDATE_COUNT["num_splats = num_splans - 1 + child_count"]
    UPDATE_COUNT --> LOOP_HEAD

    BREAK --> DRAIN
    DRAIN --> PER_INST["按 instance 分桶<br/>生成 keyIndices[uuid] = {lodId, numSplats, indices}"]
    PER_INST --> OUTPUT_T["返回 {keyIndices, chunks, pixelLimit}"]

    style CHECK_RESIDENT fill:#e74c3c,color:#fff
    style EXPAND_CHILDREN fill:#2ecc71,color:#fff
    style BREAK fill:#f39c12,color:#fff
```

## 6. 渐进式渲染完整生命周期时序图

```mermaid
sequenceDiagram
    participant User as 用户
    participant Scene as THREE.Scene
    participant SM1 as SplatMesh #1<br/>(近)
    participant SM2 as SplatMesh #2<br/>(远)
    participant Pager as SplatPager
    participant Net as HTTP/CDN
    participant Worker as WebWorker
    participant WASM as WASM
    participant GPU as GPU

    Note over Scene: t=0: 场景初始化

    User->>Scene: 创建 SplatMesh#1(sceneA.rad, paged)<br/>创建 SplatMesh#2(sceneB.rad, paged)

    Scene->>SM1: paged.getRadMeta()
    SM1->>Net: GET sceneA.rad (Range: 0-65535)
    Net-->>SM1: RadMeta {chunks: [...] }

    Scene->>SM2: paged.getRadMeta()
    SM2->>Net: GET sceneB.rad (Range: 0-65535)
    Net-->>SM2: RadMeta {chunks: [...]}

    Note over Scene: t=1: 首帧 driveLod

    Scene->>Pager: new SplatPager (maxPages=256)
    Scene->>WASM: newLodTree → pagerId
    Scene->>WASM: newSharedLodTree → lodId#1
    Scene->>WASM: newSharedLodTree → lodId#2

    Note over Pager: fetchPriority:<br/>[SM1.chunk0 (d=2m), SM2.chunk0 (d=10m)]

    Pager->>Net: fetch SM1.chunk0 (Range请求)
    Pager->>Net: fetch SM2.chunk0 (Range请求)
    Net-->>Worker: SM1.chunk0 bytes
    Net-->>Worker: SM2.chunk0 bytes

    Worker->>Worker: loadPackedSplats (SM1.chunk0)
    Worker->>Worker: loadPackedSplats (SM2.chunk0)

    Pager->>Pager: processFetched()<br/>分配 page 0,1
    Pager->>GPU: uploadPage(page=0, SM1.chunk0)
    Pager->>GPU: uploadPage(page=1, SM2.chunk0)

    Scene->>WASM: updateLodTrees<br/>(SM1 chunk0→page0, SM2 chunk0→page1)
    Scene->>WASM: traverseLodTrees(maxSplats=2M)

    Note over WASM: SM1 root chunk 驻留 → 展开子节点<br/>SM2 root chunk 驻留 → 展开子节点<br/>触碰到 chunk 3,7 未驻留

    WASM-->>Scene: {keyIndices, chunks: [(SM1,3),(SM1,7),...]}
    Scene->>SM1: paged.update(numSplats, indices)
    Scene->>SM2: paged.update(numSplats, indices)

    Scene->>GPU: 渲染粗粒度 splats ✓

    Note over Scene: t=2: 后台继续下载
    
    Pager->>Net: fetch SM1.chunk3
    Pager->>Net: fetch SM1.chunk7
    Pager->>Net: fetch SM2.chunk2
    Net-->>Pager: chunks 到达
    Pager->>Pager: processFetched → page 2,3,4
    Pager->>GPU: uploadPage(pages 2-4)

    Note over Scene: t=3: 下一帧 driveLod

    Scene->>WASM: updateLodTrees (新页映射)
    Scene->>WASM: traverseLodTrees (更大预算)

    Note over WASM: chunk3,7 现在驻留<br/>展开更深层子节点

    WASM-->>Scene: 更精细的 keyIndices
    Scene->>SM1: paged.update(更精细indices)
    Scene->>GPU: 渲染精细化 splats ✓✓

    Note over Scene: t=N: 持续细化直到所有 chunk 驻留<br/>或达到 maxSplats 预算上限
```

## 7. 页面淘汰（LRU Eviction）流程图

```mermaid
flowchart TD
    A[driveFetchers 遍历 fetchPriority] --> B{chunk 已有 page?}
    B -->|Yes| C{numPages ≥ maxPages?}
    C -->|Yes| D["overflow.push(pageLru)"]
    C -->|No| E["needed.push(pageLru)"]
    B -->|No| F{"`已在 fetchers[]/fetched[]?`"}
    F -->|Yes| G["numPages++ (跳过)"]
    F -->|No| H{numPages<maxPages<br/>且 fetchers<numFetchers?}
    H -->|Yes| I["启动 fetch"]
    H -->|No| J["跳过（无槽位）"]

    D --> K["更新 LRU: overflow 逆序刷新 lru 时间戳"]
    E --> K

    K --> L["extraPages = Set(pageLru)"]
    L --> M["needed 逆序从 extraPages 移除"]
    M --> N["freeablePages = extraPages 中的页"]

    N --> O["processFetched()"]
    O --> P{"allocatePage 非空?"}
    P -->|非空| Q["使用 freelist 页"]
    P -->|空| R{"allocateFreeable 非空?"}
    R -->|非空| S["freeablePages.shift() + removeSplatsChunkPage(旧)<br/>lodTreeUpdates.push(淘汰通知)"]
    R -->|空| T["return (停止, 等下帧)"]
    Q --> U["insertSplatsChunkPage<br/>+ lodTreeUpdates.push(新增)"]
    S --> U
```

## 8. fetchSplat GPU Shader 读取路径

```mermaid
flowchart LR
    subgraph VERTEX["Vertex Shader / Splat Generator"]
        IDX[index: logical splat index] --> READ_IDX["readIndex()<br/>indices 纹理查找"]
        READ_IDX --> SPLAT_IDX["splatIndex = texelFetch(indices, ...)[comp]"]
    end

    SPLAT_IDX --> HAS_EXT{extSplats?}

    HAS_EXT -->|No packed| PAGED_COORD["pagedSplatTexCoord(index)<br/>= ivec3(idx & 255,<br/>(idx>>8)&255, idx>>16)"]
    PAGED_COORD --> PACKED_FETCH["texelFetch(packedTexture, coord, 0)"]
    PACKED_FETCH --> UNPACK["unpackSplatEncoding()<br/>→ center, scales, quat, rgba"]

    HAS_EXT -->|Yes| EXT_FETCH["texelFetch(packedTexture + extTexture)"]
    EXT_FETCH --> EXT_UNPACK["unpackSplatExt()<br/>→ center, scales, quat, rgba"]

    UNPACK --> HAS_SH{hasRgbDir?<br/>viewOrigin?}
    EXT_UNPACK --> HAS_SH

    HAS_SH -->|No| OUT[gsplat]
    HAS_SH -->|Yes| SH_EVAL["evaluatePackedSH / evaluateExtSH<br/>viewDir × shTextures[0..3]"]
    SH_EVAL --> ADD["rgb += shRgb"]
    ADD --> OUT
```

## 9. 三档 RAD Header 渐进请求流程

```mermaid
flowchart TD
    A["PagedSplats 构造"] -->|"RAD 格式"| B["getRadMeta() 启动"]
    B --> C["尝试 #1: fetchRange(bytes=0..65535)"]
    C --> D{"decode_rad_header(bytes)<br/>成功?"}
    D -->|Yes| E["✓ 返回 {meta, chunksStart}"]
    D -->|No| F["尝试 #2: fetchRange(bytes=0..256KB)"]
    F --> G{"decode_rad_header(bytes)<br/>成功?"}
    G -->|Yes| E
    G -->|No| H["尝试 #3: fetchRange(bytes=0..1MB)"]
    H --> I{"decode_rad_header(bytes)<br/>成功?"}
    I -->|Yes| E
    I -->|No| J["throw Error:<br/>'Failed to decode RAD header'"]

    E --> K["RadMeta.chunks[]:<br/>{offset, bytes, filename?, base?, count?}"]
```

## 10. chunk 下载地址生成策略

```mermaid
flowchart TD
    A["fetchDecodeChunk(chunk)"] --> B{"fileType == RAD?"}

    B -->|Yes| C["await getRadMeta()"]
    C --> D{"chunks[chunk].filename<br/>存在?"}
    D -->|Yes| E["外部 .radc 文件<br/>chunkUrl = resolve(filename, rootUrl)<br/>fetchRange(chunkUrl) 整文件"]
    D -->|No| F["内嵌 chunk<br/>offset += chunksStart<br/>fetchRange(rootUrl, offset, bytes)"]

    B -->|No 非RAD格式| G{"有 fileBytes?"}
    G -->|Yes| H["从 fileBytes 切片"]
    G -->|No| I["chunkUrl(N) = rootUrl.replace(<br/>'-lod-0.', '-lod-N.')<br/>普通 fetch(url)"]

    E --> J["Uint8Array"]
    F --> J
    H --> J
    I --> J

    J --> K["workerPool.withWorker()"]
    K --> L{"extSplats?"}
    L -->|No| M["worker.call('loadPackedSplats')"]
    L -->|Yes| N["worker.call('loadExtSplats')"]
    M --> O["PackedResult<br/>{packedArray, extra}"]
    N --> O
```
