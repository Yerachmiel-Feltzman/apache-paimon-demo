---
marp: true
theme: default
paginate: true
size: 16:9
---

<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
:root {
  --color-canvas-default: #f8fafc;
}
section {
  font-size: 24px;
  font-family: 'Inter', sans-serif;
}
h1 {
  font-size: 1.8em;
  font-weight: 700;
}
h2 {
  font-size: 1.4em;
  font-weight: 600;
}
table {
  font-size: 19px;
}
.center-table {
  display: flex;
  justify-content: center;
  width: 100%;
}
.center-table table {
  width: 90%;
}
blockquote {
  font-size: 24px;
}
pre {
  font-size: 17px;
  font-family: 'JetBrains Mono', monospace;
}
code {
  font-family: 'JetBrains Mono', monospace;
}
header {
  font-size: 14px;
  color: #999;
}
header strong {
  color: #2563eb;
}
section.title-slide {
  background: linear-gradient(135deg, #0f172a 0%, #1e293b 50%, #0f172a 100%);
}
section.part-why,
section.part-how,
section.part-comparison,
section.part-adoption,
section.title-slide {
  --h1-color: #fff;
  --heading-strong-color: #fff;
  --fgColor-default: rgba(255, 255, 255, 0.95);
  --fgColor-muted: rgba(255, 255, 255, 0.7);
  color: white;
}
section.part-why {
  background: linear-gradient(135deg, #1e3a5f 0%, #2d5a8e 100%);
}
section.part-how {
  background: linear-gradient(135deg, #064e3b 0%, #047857 100%);
}
section.part-comparison {
  background: linear-gradient(135deg, #7a4a1a 0%, #a66b2e 100%);
}
section.part-adoption {
  background: linear-gradient(135deg, #3d1e5c 0%, #5a2d8e 100%);
}
img[alt~="center"] {
  display: block;
  margin: 0 auto;
}
</style>

<!-- _class: lead title-slide -->

# Apache Paimon
## A streaming-native lakehouse format

**Focus**: When and why to add it to your toolbox
**Date**: May 2026

---

![bg right:50%](https://images.unsplash.com/photo-1540575467063-178a50c2df87?w=800)

# The streaming challenge

"Recently, I attended a data conference where the speaker demonstrated that Iceberg tables can work for streaming..."

<!-- 
SPEAKER NOTES:
Recently, I attended a data conference where one particular presentation caught my eye: the speaker set out to demonstrate that Iceberg tables can effectively be used in high-scale streaming pipelines. He walked through various strategies, including tuning configurations, managing compaction, and deciding between Copy-On-Write and Merge-On-Read approaches. Great talk, I must say.

I only have to agree with him. Yes, it is possible. I've worked in high-volume streaming architectures with Delta Lake, Iceberg's most direct competitor. It works, and it works great. However, it requires dedicated attention. Both will suffer from small file issues, and both will need specific tuning. I did it once, I would do it again. No regrets.

Apache Paimon is a streaming-first Open Table Format that promises to alleviate streaming-first architecture pains. It's a good time to pick it up and see what this new kid in the neighborhood is about.
-->

---

<!-- _header: "" -->
<!-- _class: lead part-why -->

# Part 1: Why Paimon exists

**The streaming-first approach to lakehouse storage**

---

<!-- header: "**Why Paimon exists** > How Paimon works > Comparison > Iceberg integration" -->

# The streaming challenge in lakehouses

<div style="display: flex; gap: 2em;">
<div style="flex: 1;">

### Batch-First Reality
| Challenge | Impact |
|:---------:|:------:|
| 🗃️ Small files | ⚡ Tuning required |
| 🔄 Compaction | 🎯 Constant attention |
| ✏️ COW vs MOR | 🔧 Configuration |

</div>
<div style="flex: 1;">

### Streaming Needs
| Requirement | Batch Gap |
|:-----------:|:---------:|
| ⚡ Low-latency writes | Minutes → Seconds |
| 🔑 Primary key updates | Upserts bolted on |
| 🔄 High-churn tables | Rewrite overhead |

</div>
</div>

> Batch formats work for streaming — but require dedicated attention

<!--
SPEAKER NOTES:
You can use Iceberg or Delta for streaming pipelines, but they were designed for batch first.

Batch-first reality:
- Small file issues require ongoing tuning
- Compaction strategies need constant attention
- Copy-On-Write vs Merge-On-Read decisions affect performance

Streaming needs:
- Low-latency writes (seconds, not minutes)
- Primary key updates as first-class citizens
- High-churn tables without rewrite overhead
- Near real-time reads

I've worked with Delta Lake in high-volume streaming architectures. It works, but requires dedicated attention to these issues.
-->

---

# Enter Apache Paimon

<div style="display: flex; gap: 2em;">
<div style="flex: 1; text-align: center;">

### 📅 Born
Early 2025
**v1.0.1 stable**

</div>
<div style="flex: 1; text-align: center;">

### 🏠 Origin
Apache Flink
community

</div>
<div style="flex: 1; text-align: center;">

### 🌳 Innovation
LSM-tree + Primary keys

</div>
</div>

> **Streaming-first** Open Table Format — designed from day one for streaming pipelines

<!--
SPEAKER NOTES:
Apache Paimon reached its first stable release (1.0.1) in early 2025.

Key characteristics:
- Born in the Apache Flink community — designed from day one for streaming pipelines
- LSM-tree architecture enables fast writes and efficient merges
- Primary key tables are a native concept, not bolted on

This is fundamentally different from formats that started batch-first and added streaming capabilities later.
-->

---

# The positioning

<div class="center-table">

| Paimon 🎯 | Iceberg/Delta 📊 |
|:---------:|:----------------:|
| High-throughput streaming | Batch-heavy workloads |
| High-churn tables | Large-scale analytics |
| Near real-time reads | Existing ecosystem |

</div>

> One day, we might need a niche OTF for a pipeline with stricter latency requirements

<!--
SPEAKER NOTES:
When to choose Paimon:
- High-throughput streaming with incremental updates
- High-churn tables with lots of upserts/deletes
- Near real-time reads with low latency requirements

When to stay with Iceberg/Delta:
- Batch-heavy workloads dominate your architecture
- Large-scale analytics is the primary use case
- You need mature ecosystem tooling and community support
-->

---

<!-- _header: "" -->
<!-- _class: lead part-how -->

# Part 2: How Paimon works

**LSM-tree architecture, hot/cold layers, primary keys**

---

<!-- header: "Why Paimon exists > **How Paimon works** > Comparison > Iceberg integration" -->

# LSM-tree architecture

Log-Structured Merge-tree — the key innovation

```
Hot Layer / write-optimized
-----------------------------------------------
    Level 0: small sorted runs (many small files)
      ↓ compaction
    Level 1: larger sorted runs (fewer, bigger files)
      ↓ compaction
-----------------------------------------------
Cold Layer / query-optimized
    Level N: large, stable columnar files
```

---

# Why LSM-tree matters

<div style="display: flex; gap: 2em;">
<div style="flex: 1;">

### ✍️ Writers
- **Append-only** writes → Fast
- Sorted runs in memory → Flush to disk
- **No random I/O**

</div>
<div style="flex: 1;">

### 📖 Readers
- Merge sorted runs in memory
- Efficient for **recent/hot** data
- Compaction keeps queries fast

</div>
</div>

> Every OTF needs compaction eventually — Paimon just handles small files better during writes

<!--
SPEAKER NOTES:
LSM-tree provides benefits for both writers and readers:

For writers:
- Append-only writes are extremely fast
- Sorted runs are built in memory, then flushed to disk
- No random I/O means consistent write performance

For readers:
- Sorted runs can be efficiently merged in memory
- Most queries target recent/hot data which is in upper levels
- Background compaction keeps query performance stable

The key insight: every Open Table Format needs compaction eventually. Paimon's LSM architecture just handles small files better during the write phase.
-->

---

# Primary key tables

Paimon's killer feature — built-in UPSERT

```sql
CREATE TABLE users (
    id BIGINT,
    name STRING,
    email STRING,
    PRIMARY KEY (id) NOT ENFORCED
);
```

- **UPSERT semantics** by default
- **Deduplication** happens automatically
- **Three modes**: MOR (default), COW, or MOW (deletion vectors)

---

# The fundamental problem

Parquet/ORC files are **immutable** — when you update or delete:

<div style="display: flex; gap: 2em;">
<div style="flex: 1; text-align: center; padding: 1em; background: rgba(255,100,100,0.1); border-radius: 8px;">

### ✏️ Copy-on-Write
**Rewrite entire file**

📝 Change 1 row
↓
🗑️ Delete old file
↓
📄 Write new file

❌ Slow writes

</div>
<div style="flex: 1; text-align: center; padding: 1em; background: rgba(100,255,100,0.1); border-radius: 8px;">

### 🔄 Merge-on-Read
**Track changes separately**

📝 Change 1 row
↓
📄 Append delta
↓
⏩ Merge at query time

✅ Fast writes

</div>
</div>

> This trade-off defines everything about lakehouse performance

---

# COW vs MOR: The trade-offs

<div class="center-table">

| Trade-off | COW | MOR |
|-----------|-----|-----|
| Write latency | Higher | Lower |
| Query latency | Lower | Higher |
| Update cost | High (full rewrite) | Low (append only) |
| Write amplification | High | Low |
| Read amplification | Minimal | Higher |

</div>

> RUM Conjecture: Optimize two of Read/Update/Memory, trade off the third

---

# How formats implement MOR

<div class="center-table">

| Format | MOR mechanism | Notes |
|--------|---------------|-------|
| **Hudi** | Delta logs (Avro) + file groups | Async compaction, event-time ordering |
| **Iceberg** | Delete files (position/equality) | V2 tables, delete + insert for updates |
| **Delta** | Deletion vectors (bitmaps) | Inline or separate files |
| **Paimon** | LSM sorted runs + deletion vectors | Native LSM architecture |

</div>

---

# Paimon's three modes

<div class="center-table">

| Mode | Write | Read | Use Case |
|:----:|:-----:|:----:|:---------|
| **MOR** | ⚡⚡⚡ | ⚡ | Default, write-heavy |
| **COW** | ⚡ | ⚡⚡⚡ | Read-heavy workloads |
| **MOW** ⭐ | ⚡⚡ | ⚡⚡ | Recommended balance |

</div>

> MOW = Merge On Write — deletion vectors enable fast reads without write penalty

<!--
SPEAKER NOTES:
Paimon offers three table modes:

1. **MOR (Merge On Read)** - default
   - Very good write performance
   - Not so good read performance (merges happen at query time)
   - Best for write-heavy workloads

2. **COW (Copy On Write)**
   - Very bad write performance (entire files rewritten on update)
   - Very good read performance
   - Rarely used in practice for streaming

3. **MOW (Merge On Write)** ⭐ recommended
   - Good write performance
   - Good read performance
   - Uses deletion vectors to get the best of both worlds
   - Set with: ALTER TABLE orders SET ('deletion-vectors.enabled' = 'true');
-->

---

# MOW: The best of both worlds

```sql
ALTER TABLE orders SET ('deletion-vectors.enabled' = 'true');
```

<div style="display: flex; gap: 1.5em; align-items: center; justify-content: center; margin-top: 1em;">
<div style="text-align: center; padding: 0.8em; background: rgba(100,200,255,0.15); border-radius: 8px; min-width: 120px;">

### 1️⃣ Write
📝 Append to
LSM tree

</div>
<div style="font-size: 2em;">→</div>
<div style="text-align: center; padding: 0.8em; background: rgba(255,200,100,0.15); border-radius: 8px; min-width: 120px;">

### 2️⃣ Mark
❌ Generate
deletion vector

</div>
<div style="font-size: 2em;">→</div>
<div style="text-align: center; padding: 0.8em; background: rgba(100,255,150,0.15); border-radius: 8px; min-width: 120px;">

### 3️⃣ Query
⚡ Filter
deleted rows

</div>
</div>

> LSM enables primary-key lookups during writes → deletion vectors become cheap

---

<!-- _header: "" -->
<!-- _class: lead part-comparison -->

# Part 3: Paimon vs the alternatives

**When to use which format**

---

<!-- header: "Why Paimon exists > How Paimon works > **Comparison** > Iceberg integration" -->

# Quick comparison

<div class="center-table">

| Feature | Delta | Iceberg | Hudi | Paimon |
|---------|-------|---------|------|--------|
| MOR support | Deletion vectors (DVs) | Delete files | Delta logs + file groups | LSM + DVs |
| Streaming writes | Good | Limited | Good | Excellent |
| Primary keys | Yes (merge) | No | Yes | Yes (native LSM) |
| Event-time ordering | No | No | Yes | Yes |
| Partial updates | No | No | Yes | Yes |
| Multi-engine | Spark-Databricks | Excellent | Good | Flink-focused |
| Maturity | High | High | High | Growing |

</div>

---

# When to use Paimon

<div class="center-table">

| ✅ Good Fit | ❌ Not Ideal |
|:-----------:|:------------:|
| 🌊 Kafka/Kinesis ingestion | 📊 Pure batch analytics |
| 🔄 CDC pipelines | 🔧 Spark-only ecosystem |
| ⚡ Low-latency dimensions | 🛠️ Need mature tooling |
| ✏️ Streaming ETL with upserts | |

</div>

<!--
SPEAKER NOTES:
Paimon is a good fit when:
- Real-time ingestion from Kafka/Kinesis is required
- CDC pipelines from databases need low-latency writes
- Low-latency dimension tables are needed for joins
- Streaming ETL with upserts is the primary workload
- High-churn, high-update tables dominate

Paimon is not ideal when:
- Pure batch analytics is the only workload
- Your ecosystem is Spark-only
- You need mature ecosystem tooling immediately
-->

---

# The ecosystem challenge

<div class="center-table">

| New Format Reality |
|:------------------:|
| 🧰 Fewer ecosystem tools |
| 👥 Smaller community |
| 🏭 Less production testing |
| 🔌 Not every engine supports it |

</div>

> This is where **Iceberg integration** becomes the adoption path

<!--
SPEAKER NOTES:
Paimon is newer than Delta, Iceberg, and Hudi, which creates challenges:
- Fewer tools in the ecosystem (monitoring, governance, etc.)
- Smaller community means less community support and fewer examples
- Less production battle-testing in large-scale deployments
- Not every query engine supports it natively yet

The Iceberg compatibility feature addresses many of these concerns by allowing you to query Paimon tables through the Iceberg ecosystem.
-->

---

<!-- _header: "" -->
<!-- _class: lead part-adoption -->

# Part 4: The Iceberg advantage

**Practical adoption through cross-catalog integration**

---

<!-- header: "Why Paimon exists > How Paimon works > Comparison > **Iceberg integration**" -->

# The adoption problem

<div style="display: flex; gap: 2em; align-items: center; justify-content: center;">
<div style="flex: 1; text-align: center;">

### 🔄 Chicken-and-Egg
<div style="font-size: 0.85em;">
🧰 Tools → 👥 Users → 🏭 Production<br>
↓ &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; ↓ &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; ↓
<br>Need users &nbsp; Need tools &nbsp; Need maturity
</div>

</div>
<div style="font-size: 2em;">→</div>
<div style="flex: 1; text-align: center; padding: 1em; background: rgba(100,255,150,0.15); border-radius: 12px;">

### ✅ Paimon's Solution
<div style="font-size: 0.9em;">
📝 Write **Iceberg-compatible** metadata<br><br>
✓ Use existing tools<br>
✓ Any engine support<br>
✓ Skip adoption cycle
</div>

</div>
</div>

<!--
SPEAKER NOTES:
New formats face a classic chicken-and-egg adoption problem:
- Tools won't support it without a user base
- Users won't adopt it without tool support
- Production systems require mature tooling and battle-testing

Paimon solves this by generating Iceberg-compatible metadata alongside its own, allowing immediate access through the mature Iceberg ecosystem.
-->

---

# Iceberg compatibility

Paimon can generate **dual metadata**

```
warehouse/
  paimon/
    default.db/
      cities/
        bucket-0/
          data-xxx.parquet      # Physical data files
        manifest/               # Paimon metadata
        snapshot/
    iceberg/                    # Iceberg metadata
      default/
        cities/
          metadata/
            v1.metadata.json
```

---

# Dual catalog architecture

![width:550 center](../../articles/resources/paimon-and-iceberg.png)

---

# What this enables

<div style="display: flex; gap: 2em;">
<div style="flex: 1;">

### Through Paimon 🎯
- Native streaming writes
- Primary key semantics
- Incremental reads

</div>
<div style="flex: 1;">

### Through Iceberg 📊
- Existing Iceberg tools
- Any engine support
- Current stack compatible

</div>
</div>

> Same physical data, two access paths

<!--
SPEAKER NOTES:
The dual-catalog architecture provides two access patterns:

Through the Paimon catalog:
- Native streaming writes with low latency
- Primary key semantics (UPSERT, deduplication)
- Incremental reads for change data capture

Through the Iceberg catalog:
- Use existing Iceberg ecosystem tools
- Query with engines that don't have native Paimon support
- Maintain compatibility with your current data stack

This means you can write with Paimon for streaming performance, and read with Iceberg for ecosystem compatibility.
-->

---

# The working assumption

<div class="center-table">

| Real-World Scenario |
|:-------------------:|
| 📊 Most platforms: Batch/micro-batch with Iceberg |
| ⚡ A few pipelines: Stricter streaming (Paimon + Flink) |
| 🔧 Still need Spark readers |
| 🔗 Mix Paimon + federated Iceberg tables |

</div>

> If we pick Paimon, it might happen in a cross-platform world with Iceberg and Spark

<!--
SPEAKER NOTES:
In a realistic production environment:
- Most of your platform will still use batch/micro-batch with Iceberg
- A few specific pipelines have stricter streaming requirements → use Paimon + Flink
- You still need to support Spark readers for batch analytics
- You'll mix Paimon tables with federated Iceberg tables in the same warehouse

The Iceberg compatibility feature makes this hybrid approach feasible without sacrificing either streaming performance or ecosystem access.
-->

---

# REST Catalog integration

```sql
CREATE TABLE events (...)
TBLPROPERTIES (
    'metadata.iceberg.storage' = 'rest-catalog',
    'metadata.iceberg.rest.uri' = 'http://catalog:8181'
);
```

| Production Benefits |
|:-------------------:|
| 🏛️ Centralized metadata management |
| 🔀 Multi-engine access coordination |
| 🔗 Works with Iceberg compatibility |

<!--
SPEAKER NOTES:
For production deployments, use Iceberg REST Catalog:

Key benefits:
- Centralized metadata management across your organization
- Multi-engine access coordination prevents conflicts
- Works seamlessly with Paimon's Iceberg compatibility feature

Configuration:
- Set metadata.iceberg.storage = 'rest-catalog'
- Point metadata.iceberg.rest.uri to your REST catalog endpoint
- Iceberg metadata will be managed through the REST catalog
-->

---

<!-- _header: "" -->
<!-- _class: lead title-slide -->

# Demo: Apache Paimon in practice

**ACID operations, cross-catalog queries, REST catalog**

---

<!-- header: "**Demo** > Paimon basics > Cross-catalog > REST catalog" -->

# Demo setup

<div class="center-table">

| Prerequisites |
|:-------------:|
| ☕ Java 17+ |
| 🐍 Python 3.11 + PySpark |
| 🐳 Docker (REST Catalog) |

</div>

```bash
make setup  # JARs downloaded, data generated
```

---
<!-- header: "**Demo** > Paimon basics > Cross-catalog > REST catalog" -->

# Demo 1: Paimon basics

<div style="display: flex; gap: 2em;">
<div style="flex: 1;">

### 🎯 Goal
Primary key tables + UPSERT

</div>
<div style="flex: 1;">

```bash
make run_paimon_only_demo
```

</div>
</div>

<div class="center-table">

| What to Watch |
|:-------------:|
| 🔑 Table creation with primary key |
| ✏️ UPSERT: Bob Smith → Bob Smith Jr. |
| ➕ New record: Frank Miller |
| 🔒 ACID transaction semantics |

</div>

---
<!-- header: "**Demo** > Paimon basics > Cross-catalog > REST catalog" -->

# Demo 2: Cross-catalog query

<div style="display: flex; gap: 2em;">
<div style="flex: 1;">

### 🎯 Goal
Query same table via Paimon AND Iceberg

</div>
<div style="flex: 1;">

```bash
make run_paimon_and_iceberg_cross_platform_demo
```

</div>
</div>

<div class="center-table">

| Key Detail |
|:----------:|
| 📦 `'metadata.iceberg.storage' = 'hadoop-catalog'` |
| 👁️ Same data visible through both catalogs |
| 📁 Iceberg warehouse = `<paimon-warehouse>/iceberg` |

</div>

---
<!-- header: "**Demo** > Paimon basics > Cross-catalog > REST catalog" -->

# Demo 3: REST Catalog

<div class="center-table">

| 🎯 Goal | 📋 What to Watch |
|:-------:|:----------------:|
| Register table in Iceberg REST Catalog | Table registered in REST catalog |
| | Queryable by any Iceberg client |
| | Production-ready architecture |

</div>

> This is how you'd deploy in a real environment

---

<!-- header: "**Demo** > Paimon basics > Cross-catalog > REST catalog" -->

# Key findings from the demos

<div class="center-table">

| Test | Result |
|------|--------|
| Cross-catalog query | ✅ Works — same data via both catalogs |
| Drop table | ⚠️ Iceberg metadata not cleaned up |
| ALTER TABLE (local) | ✅ Works — metadata generated on next write |
| ALTER TABLE (REST) | ❌ Doesn't work — must set at CREATE time |

</div>

---

# Governance considerations

<div class="center-table">

| ⚠️ Important to Know |
|:--------------------:|
| 🧟 DROP leaves "zombie" Iceberg metadata |
| 🔧 ALTER works for hadoop-catalog, not REST |
| ⏳ Iceberg metadata generated **lazily** on writes |
| 📚 Read the docs twice before production |

</div>

> The cross-catalog story makes edges easier to hit if you're not careful

<!--
SPEAKER NOTES:
Key governance considerations when using Paimon with Iceberg compatibility:

1. Dropping a Paimon table leaves "zombie" Iceberg metadata behind
   - Need to manually clean up the Iceberg warehouse directory

2. ALTER TABLE works for hadoop-catalog but not rest-catalog
   - REST catalog tables must have properties set at CREATE time

3. Iceberg metadata is generated lazily on writes
   - You might not see Iceberg metadata immediately after table creation

4. Read the documentation carefully before production deployment
   - The cross-catalog story has edge cases that can cause issues

The cross-catalog feature is powerful but requires understanding these governance implications.
-->

---

<!-- _header: "" -->
<!-- _class: lead part-adoption -->

# Summary and takeaways

---

<!-- header: "Why Paimon exists > How Paimon works > Comparison > **Summary**" -->

# When to reach for Paimon

<div class="center-table">

| Add to your toolbox when |
|:------------------------:|
| 🌊 **Streaming-first** data ingestion |
| 🔑 **Upserts & primary keys** central to workload |
| 📈 **Incremental change streams** without Kafka |
| 🔗 **Iceberg compatibility** for tool access |

</div>

> Treat Paimon as a niche but high-leverage component

<!--
SPEAKER NOTES:
Consider adding Paimon to your toolbox when:
- You need streaming-first data ingestion with low latency
- Upserts and primary keys are central to your workload
- You want incremental change streams without maintaining a separate Kafka cluster
- You can leverage Iceberg compatibility for tool access and ecosystem support

Paimon is not a replacement for Iceberg/Delta — it's a niche tool for specific high-leverage use cases.
-->

---

# The practical path forward

<div class="center-table">

| Step | Action |
|:----:|:-------|
| 1️⃣ | Start with **Iceberg** as default OTF |
| 2️⃣ | Add **Paimon** where latency/update rate are bottlenecks |
| 3️⃣ | Enable **Iceberg compatibility** at table creation |
| 4️⃣ | Query via **Iceberg catalog** with existing tools |
| 5️⃣ | Invest in **understanding governance** implications |

</div>

> Use Paimon where its LSM tree unlocks something you can't get from "Iceberg plus more tuning"

<!--
SPEAKER NOTES:
Practical adoption path:

1. Start with Iceberg as your default Open Table Format for most workloads
2. Add Paimon for specific pipelines where latency and update rate are bottlenecks
3. Enable Iceberg compatibility at table creation time (can't be added later for REST catalog)
4. Query via Iceberg catalog to leverage existing tools and ecosystem
5. Invest time in understanding governance implications before production

The key insight: use Paimon where its LSM tree architecture unlocks something you can't achieve with "Iceberg plus more tuning."
-->

---

# Questions?

**Resources**:
- Apache Paimon docs: `paimon.apache.org`
- This repo: Full working demos with setup scripts
- Medium article: "Can Apache Paimon coexist with Apache Iceberg?"

---

<!-- _class: lead title-slide -->

# Thank you

**Apache Paimon**: A streaming-native tool for your lakehouse toolbox

Questions welcome

