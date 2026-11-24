# 巡测日账的冷镜像换页

普通镜像仍按 README 的回执下界查账。冷镜像单独建档，只办理一个非空分区的一次原位换值。办件和查询串行调用，任意两次调用之间可关闭或重开两份本地存储。两份材料由同一可信正式源发行，运输按内容摘要验件，沿用 V1 的信任边界。

## 公开调用

| 入口 | 参数与结果 |
| --- | --- |
| lake.dispatch.publish.publish_rollover(db, old, new, partition) | old、new 是 OfficialReceipt。返回可原样复领的换页件 bytes。 |
| lake.receiving.accept.initialize_rollover(mirror, medium, old_payload) | 用旧回执的 V1 分区件在空目录和空介质上建档，返回旧 OfficialReceipt。建档完成后用原路径开库即可续办。 |
| lake.receiving.accept.receive_rollover(mirror, medium, payload) | 断源验收换页件，成功返回 ReceivingReceipt。同件重投复领首执，退旧后也可复领。 |
| lake.query.read.read_rollover(mirror, medium, receipt) | 返回带 status 的结果。ready 用现有 MirrorAccount，requested_receipt 和 fulfilled_receipt 均等于所传回执。pending 与 retired 只有状态和请求身份，pending 可沿用 PendingMaterials。 |
| lake.receiving.accept.retire_rollover(mirror, medium, operation_id, old) | 具名退领已实收新版所对应的旧回执。完成时返回 (operation_id, old, new)。中断后用原号原参续办，成功后用原号原参领取相同首执。 |

mirror 通过现有 open_mirror 打开。medium 通过 lake.receiving.slots.FixedSlots(path, main_count, hook=None) 打开，路径独立于 mirror，重开沿用原路径及槽数。换页件的序列化格式由实现选择。非法身份、错绑或坏件报 ValueError，保留此前成功结果。operation_id 沿用 README 的名称规则。

## 发行和实收

两份回执须是同源湖、同日的正式直接父子版，新版是指定分区内唯一一条原行的真实换值。缺测改为零也是换值。原行键、分区、出处、源记录位置和正式原序照旧，其余原行不变。冲突、跨区、等值或不相邻的修订拒发。发行件固定两份正式回执、旧分区原序和目标前后行的核对凭据，只携带新版替换行的正文。重复发行回传首次冻结字节。

建档按旧分区件原序占用编号 0 到 main_count−1 的主槽，main_count 等于该件行数。正式原序 position 来自源账，槽址由分区件行序确定。新件须与该确切旧投影及目标原行匹配，坏件或错绑拒收且槽位、首执和查账结果保持原状。冷镜像的持久正文只可在固定槽中出现，目录可存身份、摘要及办理元数据。旧件和换页件的原始字节及可恢复正文的编码也属于正文。已收新版的查账和退领只用本地目录与介质。

建档后旧回执即可领取。实收落定前，新回执报 pending，旧回执仍可完整领取。成功实收后两版精确可领，直到值班员办理退旧。领取须交原序行、出处、毫米合计和缺测数。合法形态但无本地账的回执返回 pending。

## 退领与介质边界

退领途中旧回执只可交完整旧版或报 retired，首次报 retired 后永久退领。新版自成功实收起始终完整可领。只有新版完全由主槽兑现且周转槽实空，才可返回退领成功。中断时调用可能无结果，重开后用原号续办或复领，改绑拒绝。

FixedSlots.read(index) 返回 SlotRecord 或 None。SlotRecord(row, marker=None) 以 OfficialRow 为正文，marker 为可选的非空可打印元数据字符串，至多 256 字符。周转槽编号为 medium.spare。replace(index, record) 原子持久写一个槽的整行及标记。clear_spare() 原子清空周转槽。close() 关闭介质。镜像只经这些介质入口访问槽，目录和介质各自提交，目录事务无法回滚介质写入。

可选的 hook(action, phase, index) 在每次槽变更的提交前后被调用。action 为 replace 或 clear_spare，phase 为 before 或 after。回调抛出的异常传到调用者。before 抛出时该次槽写尚未发生，after 抛出时该次槽写已提交。可用它核验新版正文写入后实收尚未落定、主槽换行前后及清空周转槽前后的中断和重开。

## 两组样例

两组各用独立的源库和冷镜像。湖号 survey，观测日 2025-08-14，以 submit 和 post 登记原账，再用 correct 原位更正。以下 rows 可直接交 submit，参数中的更正目标为 ObservationKey(batch_id, change.observation)。两组均用原回执发行 east 的 V1 旧件。

~~~json
[
  {
    "case": "same-value-neighbor",
    "lake_id": "survey",
    "day": "2025-08-14",
    "partition": "east",
    "batch_id": "roll",
    "rows": [
      {"observation": "R", "partition": "east", "source": "gage-A", "line": 4, "unit": "mm", "mm": 7},
      {"observation": "S", "partition": "east", "source": "gage-B", "line": 9, "unit": "mm", "mm": 7},
      {"observation": "T", "partition": "east", "source": "gage-C", "line": 12, "unit": "mm", "mm": null}
    ],
    "change": {"correction_id": "remeasure-R", "observation": "R", "mm": 11},
    "old": {"observations": ["R", "S", "T"], "positions": [1, 2, 3], "mm": [7, 7, null], "total_mm": 14, "missing_count": 1},
    "new": {"observations": ["R", "S", "T"], "positions": [1, 2, 3], "mm": [11, 7, null], "total_mm": 18, "missing_count": 1}
  },
  {
    "case": "missing-to-zero",
    "lake_id": "survey",
    "day": "2025-08-14",
    "partition": "east",
    "batch_id": "zero",
    "rows": [
      {"observation": "N", "partition": "north", "source": "gage-N", "line": 2, "unit": "mm", "mm": 5},
      {"observation": "T", "partition": "east", "source": "gage-C", "line": 12, "unit": "mm", "mm": null},
      {"observation": "S", "partition": "east", "source": "gage-B", "line": 9, "unit": "mm", "mm": 7}
    ],
    "change": {"correction_id": "remeasure-T", "observation": "T", "mm": 0},
    "old": {"observations": ["T", "S"], "positions": [2, 3], "mm": [null, 7], "total_mm": 7, "missing_count": 1},
    "new": {"observations": ["T", "S"], "positions": [2, 3], "mm": [0, 7], "total_mm": 7, "missing_count": 0}
  }
]
~~~

第一组断源后先查新版待件，实收后按两份回执分别领 14 和 18 mm。检查 11 mm 的出处是 gage-A 第 4 条，S 仍为 7 mm。第二组的两版均为 7 mm，缺测数由 1 变为 0，东区原序仍是 2、3。

沿用 .pointwise-runtime.json 的 version 1 和 cli 入口。setup 是准备命令列表，verify 和 demo 各指一条命令，命令以参数数组表示。把换页联动核验并入原 verify，核验失败以非零退出。demo 展示第一组断源双领和退旧中断后重开结清，原调用和原回归照常使用。
