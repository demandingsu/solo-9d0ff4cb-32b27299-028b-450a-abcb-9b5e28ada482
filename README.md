# 环境监测数仓 · 质量操作台

面向值班数据工程师的两个质量关口，同一页面两个标签页：

1. **字段变更核查**：原始站点表字段调整上线前，上传「变更前源字段清单」「变更后源字段清单」
   「跨作业字段依赖」三份 JSON，对照两版逐列给出 **正常 / 断供 / 待核实** 判定和完整依赖路径。
2. **小时读数质量放行**：监测站小时读数入仓前，上传整批 CSV 并设绝对阈值，
   自动隔离疑似传感器异常（**正常 / 异常 / 待补**），数据员复核后放行可信异常，
   下载只含正常及有效放行的可信读数。

技术栈：FastAPI + SQLite + 原生 HTML/JS 单页，Docker Compose 一键运行。

## 快速开始

```bash
docker compose up --build
# 打开 http://localhost:8000
```

两个标签页都可一键体验：「载入内置示例核查」与「载入示例批次」，
示例文件在 `examples/` 下，可下载修改后再上传。

本地无 Docker 运行：

```bash
pip install -r requirements.txt
uvicorn app.main:app --port 8000
```

运行单元测试（仅标准库，无需安装依赖）：

```bash
python3 -m unittest discover -s tests -v
```

## 小时读数质量放行

### CSV 格式

整批上传，UTF-8（可带 BOM），必须恰好包含五列（中英文表头均可）：

| 字段 | 中文表头 | 英文表头别名 | 说明 |
|---|---|---|---|
| 观测ID | `观测ID` | `obs_id` / `observation_id` | 全批唯一 |
| 站点 | `站点` | `station` / `station_id` | |
| UTC小时 | `UTC小时` | `utc_hour` / `hour` | 整点，如 `2026-11-15T13:00Z`；带时区会换算到 UTC |
| 分组 | `分组` | `group` | 对照只在同分组内进行 |
| 读数 | `读数` | `reading` / `value` | 有限数值 |

**整批拒绝**（保留原结果并在历史中留痕）：同站同小时重复、观测 ID 重复、
列缺失/无法识别、读数非数值、小时非整点、字段数不齐、空文件等。

### 判定口径

每条读数与**同分组、同 UTC 小时的其他站点**读数比较：

- 参照站 ≥ 2：取参照读数**中位数**，`|读数 − 中位数| > 绝对阈值` → `异常`，否则 `正常`；
- 参照站 < 2：参照不足 → `待补`。

### 放行与失效

- 页面可对 `异常` 读数放行（可填备注）；`正常`/`待补` 不可放行；
- **下载只含正常及有效放行的记录**（待补与未放行异常被排除）；
- 每次放行绑定**放行依据**（阈值 + 本行全部字段 + 参与对照的全部记录的 SHA-256）。
  重传整批后：依据不变的放行**继续有效**；读数本身、参与对照的记录（增/删/改）
  或阈值有变化的，旧放行**自动失效**（页面显示「放行已失效」）；
- 放行请求必须提交页面当前看到的依据值：若期间批次已更新（旧页面），
  服务端返回 **409 依据已过期**，前端提示并自动刷新，不会沿用过期依据。

### 一致性

- 批次解析与评估全部在写库前完成；发布在单条事务内整版替换（旧批次归档），
  **失败保留原结果**；
- 放行在单条 `BEGIN IMMEDIATE` 事务内完成「读现役批次 → 校验依据 → 写放行」，
  与并发上传互斥；
- 查询与下载在单条读事务内读取批次 + 放行，**只对应一个已提交状态**；
- SQLite WAL：写入期间并发读取不阻塞。

## 字段变更核查判定口径

每个产出列允许 **多组输入**：

- **组内全部输入可用，该组才可用；任一组可用即可产出** → `正常`
  （主用组失效但有完整备用组时，仍然正常，判定说明会标注「含备用组」）
- 所有输入组都确定不可用（源字段缺失、或上游列自身已断供）→ `断供`
- 仅能依靠 **缺失引用**（依赖清单中不存在的作业/列、既非源表也非作业的引用）
  或 **无源支撑的循环** 才能产出 → `待核实`

源字段缺失时会对照另一版清单标注「本版已移除（对照版存在）」还是「两版均无」。
判定沿依赖链传播：上游断供会导致下游断供；上游待核实只会让下游待核实，不会升级为断供。
「最终列」= 未被任何其他产出列消费的列；`external` 标记的列视为对外数据集列，页面默认聚焦这两类。

核查快照同样整版原子替换、失败保留旧版、查询只读 `active` 唯一版本，绝不混用两版。

## 输入文件格式（字段核查）

**源字段清单**（变更前/后各一份，三种写法等价）：

```json
{ "tables": { "raw_air_station_hourly": ["station_id", "pm25"] } }
{ "raw_air_station_hourly": ["station_id", "pm25"] }
[ "raw_air_station_hourly.station_id", "raw_air_station_hourly.pm25" ]
```

**跨作业字段依赖**：

```json
{
  "jobs": [
    {
      "name": "job_air_quality_hourly",
      "outputs": {
        "wind_speed_avg": [
          ["raw_air_station_hourly.wind_speed"],
          ["job_meteo_clean.wind_speed"]
        ]
      }
    }
  ],
  "external": ["job_daily_report.report_wind"]
}
```

- `outputs.列名` 的值是 **组的数组**：外层每一项是一组备用输入，组内是所有必须同时可用的输入；
  简写 `"col": ["t.a", "t.b"]`（元素非数组）等价于单组 `[["t.a", "t.b"]]`。
- 输入引用 `"属主.名"` 自动判定：属主是已定义作业 → 上游列；是源表 → 源字段；都不是 → 缺失引用（待核实）。
  同名歧义时可用 `{"source": "t.c"}` 或 `{"column": "j.c"}` 强制指定。
- `external`（可选）列出对外数据集列，必须引用已定义的产出列。

## API

### 小时读数质量放行

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/api/quality/batches` | multipart 上传 `file`(CSV) + `threshold` + 可选 `name`；成功整版发布，任何问题整批拒绝（400）且保留原结果 |
| GET | `/api/quality/current` | 现役批次全量视图（评估 + 放行状态），单事务读取 |
| GET | `/api/quality/batches` | 批次历史（含整批拒绝记录） |
| POST | `/api/quality/releases` | JSON `{obs_id, basis_hash, note}` 放行异常读数；依据过期 409，非异常 400，观测不存在 404 |
| GET | `/api/quality/download` | 下载可信读数 CSV（正常 + 有效放行） |

### 字段变更核查

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/api/snapshots` | multipart 上传 `sources_before` / `sources_after` / `dependencies` + 可选 `name`；成功整版发布，失败 400 且保留旧版 |
| GET | `/api/snapshots/current` | 现役版本全量内容（输入 + 判定报告） |
| GET | `/api/snapshots` | 版本历史（含失败记录） |
| GET | `/examples/{name}` | 示例文件下载（三份 JSON + 示例 CSV） |
| GET | `/` | 操作页面 |

## 目录结构

```
app/
  main.py            FastAPI 入口与接口（核查 + 质量放行）
  evaluator.py       字段核查：解析 + 三值判定 + 依赖路径生成（纯标准库）
  quality.py         质量放行：CSV 解析校验 + 对照评估 + 放行依据（纯标准库）
  store.py           SQLite 存储（快照/批次整版原子替换、放行事务、WAL）
  static/index.html  操作页面（两个标签页）
examples/            示例（三份 JSON + readings_example.csv）
tests/               单元测试（test_evaluator.py 19 例 + test_quality.py 36 例）
Dockerfile / compose.yaml / requirements.txt
```
