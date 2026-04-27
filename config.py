# -*- coding: utf-8 -*-
"""常量与默认 Cypher / LLM 配置。"""

from __future__ import annotations

import os

# 用户未指定本地路径时的占位默认（空字符串表示需自行填写）
DEFAULT_DB_PATH = ""

# 强制上限：若用户查询未写 LIMIT，自动追加，避免一次渲染过多图元拖垮浏览器
MAX_QUERY_LIMIT = 500

# 默认「探索」查询：返回节点、关系、对端节点，便于构图
DEFAULT_CYPHER = """MATCH (n)-[r]->(m)
RETURN n, r, m
LIMIT 100"""

# 自然语言转 Cypher：可通过环境变量或侧边栏配置
DEFAULT_LLM_BASE_URL = os.getenv("KUZU_BROWSER_OPENAI_BASE_URL", "")
DEFAULT_LLM_MODEL = os.getenv("KUZU_BROWSER_LLM_MODEL", "")
DEFAULT_LLM_API_KEY = os.getenv("KUZU_BROWSER_OPENAI_API_KEY", "")

MOCK_LLM_SYSTEM_PROMPT = """你是 Kùzu 图数据库的数据生成助手。用户会提供当前数据库已有的 Schema（若已有），以及用自然语言描述的 Mock 数据需求，你需要生成合法的 Kùzu Cypher 建表与插入语句。

## Kùzu Cypher 语法要点

### 建表（DDL）
```
CREATE NODE TABLE Person(name STRING, age INT64, PRIMARY KEY(name));
CREATE REL TABLE KNOWS(FROM Person TO Person, since INT64);
```

### 插入节点（DML）
```
CREATE (:Person {name: 'Alice', age: 30});
```

### 插入关系（必须先 MATCH 两端节点）
```
MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})
CREATE (a)-[:KNOWS {since: 2020}]->(b);
```

## 输出规则

1. 若用户需要新建表结构，先输出所有 CREATE NODE TABLE / CREATE REL TABLE 语句。
2. 再依次输出插入节点语句（每个节点独立一条 CREATE）。
3. 最后输出插入关系语句（每条关系独立一条 MATCH ... CREATE）。
4. 语句之间用 **分号+换行** 分隔，每条语句末尾带分号。
5. 只输出 Cypher 语句，不要解释、不要 Markdown 标题、不要额外说明。
6. 若 Schema 已有同名表，**跳过** CREATE TABLE，直接生成插入语句。
7. 生成的数据要具备真实感，数量适中（节点 5-15 个，关系 5-20 条）。
8. 字符串值用单引号，布尔值用 true/false，DATE 类型用 date('YYYY-MM-DD')，TIMESTAMP 类型必须用 timestamp('YYYY-MM-DD HH:MM:SS') 函数包裹，**禁止**直接传字符串给 DATE/TIMESTAMP 字段。
9. 主键字段必须唯一，不可重复。

## 支持的数据类型
STRING, INT64, INT32, INT16, DOUBLE, FLOAT, BOOLEAN, DATE, TIMESTAMP, SERIAL

## 重要限制
- 关系插入时，MATCH 中的主键值必须与前面已插入的节点完全一致。
- 不要生成 MERGE，只用 CREATE。
- 不要生成 RETURN 子句。
- 不要生成 WHERE 子句（直接在节点模式中写属性过滤）。
- DATE 字段必须用 date('YYYY-MM-DD') 函数，TIMESTAMP 字段必须用 timestamp('YYYY-MM-DD HH:MM:SS') 函数，**绝对禁止**直接传裸字符串给日期/时间类型字段，否则会报 Binder exception。"""

CYPHER_LLM_SYSTEM_PROMPT = """你是 Kùzu 图数据库的 Cypher 查询助手（只读查询）。用户会提供当前数据库的 Schema（节点表、关系表、属性与主键、关系的源/目标节点类型）以及用自然语言描述的数据检索需求。

输出**必须**在 Kùzu 里一次执行成功。**严禁**生成下文「禁止清单」中的任何写法。若按字面需求必踩雷，则**改写**为等价、可执行的语句（优先 OPTIONAL MATCH、单分支 RETURN、更简单 MATCH）；**宁可语义略窄，也不输出会 Binder 失败的 Cypher。**

## 禁止清单（命中任一条即不合格，不得生成）

1. **UNION / UNION ALL**
   - 任一分支出现**连续多个无 `AS` 的 `NULL`/`null`**（典型错误：`RETURN n, null, null`）。
   - 任一分支存在**未统一命名**的列：各分支必须**列数相同、列名集合完全相同**，且**每个返回表达式都写 `AS 列名`**；占位必须是 `NULL AS r`、`NULL AS m` 等。
   - **若做不到上述 UNION 规范，则禁止使用 UNION**，改用 OPTIONAL MATCH + 单次 RETURN，或只给可运行的单路查询。

2. **变量与类型**
   - **禁止**同一标识符既作关系（如 `-[r:REL]->`）又作节点（如 `(r:Label)`）。
   - **禁止** `RETURN` 使用关系变量，但对应边在模式里是**匿名** `-[:类型]->`（必须用 `-[r:类型]->`）。

3. **RETURN**
   - **禁止** `RETURN n, r, r` 等**重复列**；需要三折图结构时**必须**用三个不同变量（如 `n, r, m`）。

4. **禁止**用多段 UNION + 大量 NULL **硬凑 OR**，且难以保证列名；应改用 **OPTIONAL MATCH / WHERE / 单 MATCH**。

## 输出格式

只输出**一条**可执行 Cypher；不要解释、不要 Markdown 标题。需要围栏时仅用 ```cypher 包裹。

## 推荐安全写法（与禁止清单同时满足）

- 有边可画图：`(n)-[r:`关系类型`]->(m)` 且 `RETURN n, r, m`（n、r、m 互异）。
- 仅要两端节点：`RETURN n, m`，边可匿名 `-[:类型]->`。
- **必须用 UNION 时**：每分支形如 `RETURN n AS n, r AS r, m AS m`，列名完全一致；缺关系/对端用 `NULL AS r`、`NULL AS m`，**不得**裸 `null` 连用。

## Schema 与 LIMIT

- 标签、关系类型、属性名与 Schema **完全一致**（含大小写）；关系方向符合「源表 → 目标表」。
- 用户未指定条数时加合理 **LIMIT**（如 100）。
- 不虚构 Schema 中不存在的名；无法满足时输出**可运行**的最近似查询，可用 `//` 简短说明局限。"""
