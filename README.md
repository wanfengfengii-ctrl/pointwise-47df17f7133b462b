# 流域巡测日账与离线镜像

供数班交付有序观测，核账员领取正式日账，资料员办理更正，发行员分区交运。次班可关闭源湖，只凭收到的材料及正式入仓回执对账。

## 运行

需要 Python 3.11 和标准库。在项目根目录执行：

```sh
python3 -B check_project.py
python3 -B demo.py
```

控制台物化时提供 `check_project.py`。也可直接运行原套件 `python3 -B -m unittest discover -v`。演示和测试自行使用临时目录。

## 开库

一个源湖文件对应一个由 `lake_id`、`day` 标识的观测日。`day` 使用 `YYYY-MM-DD`。使用 `lake.database.open_database(path)` 打开源库，然后在空闲连接上依次调用 `lake.intake.store.initialize_intake(db)`、`lake.ledger.revisions.initialize_ledger(db, lake_id, day)` 和 `lake.dispatch.catalog.initialize_catalog(db)`。独立镜像文件用 `lake.receiving.store.open_mirror(path)` 打开并初始化。初始化可重复执行，已存业务数据保留。用毕调用连接的 `close()`。业务入口管理自己的事务，调用时连接须处于空闲状态。每个并发办理者使用自己的连接。一个湖号和观测日由同一正式源链发行。

## 业务接口

| 操作 | 导入路径和调用形式 | 结果 |
| --- | --- | --- |
| 登记收件 | `lake.intake.batch.submit(db, batch_id, rows)` | `IntakeReceipt` |
| 正式入仓 | `lake.ledger.commit.post(db, batch_id)` | `OfficialReceipt` |
| 提交更正 | `lake.ledger.corrections.correct(db, correction_id, parent_revision, target, *, partition=None, mm=...)` | `OfficialReceipt` |
| 查源账 | `lake.ledger.history.source_read(db, revision, partitions)` | `SourceAccount` |
| 发行 | `lake.dispatch.publish.publish(db, receipt)` | `ReleaseRecord`，`release.part(name)` 领取分区件字节 |
| 镜像实收 | `lake.receiving.accept.receive(mirror, payload)` | `ReceivingReceipt` |
| 镜像查账 | `lake.query.read.mirror_read(mirror, receipt, partitions)` | 状态为 `ready` 的 `MirrorAccount` 或状态为 `pending` 的 `PendingMaterials` |

收件 `rows` 为非空有序序列。每行提供 `observation`、`partition`、`source`、`line`、`unit` 和 `mm`。观测名在批内唯一。批号、湖号、分区、观测名和更正号使用 1–80 个 ASCII 字母、数字、点、下划线或连字符，以字母或数字开头。`source` 为非空可打印的出处字符串，`line` 为从 1 开始的源记录位置。单位为 `mm`。`mm` 接受 Python 整数，含零和负值，`None` 表示显式缺测。布尔值、浮点值、其他单位和缺少 `mm` 的行均非法。样例 JSON 用 `null` 表示缺测。

源、镜像明细的 `key` 为 `ObservationKey(batch_id, observation)`。更正接口的 `target` 使用此键。省略 `mm` 沿用原值，显式传 `None` 记作缺测。传入新 `partition` 可单独转记，也可同时改值。更正号在当日唯一，父修订须是当前正式修订。更正保留原出处与正式原序。显式等值更正也会形成正式修订。批次重办复领其首次正式回执。

`OfficialReceipt` 由 `lake_id`、`day` 和从 1 开始的 `revision` 组成。读取范围为非空分区序列或集合，输出分区小计按名称排序，逐行明细按首次正式入仓和批内源顺序排列。`totals` 包含 `total_mm`、`missing_count`、`row_count` 和 `by_partition`。各分区小计包含相同三项指标。已证实为空的分区小计为零。

发行件带有固定修订的覆盖说明和一个分区的完整投影。已出现过的分区即使撤空也有发行件。覆盖说明记录各分区投影的末次受影响修订。某份已收投影经覆盖说明证明适用时，可供另一正式修订使用。接收端校验规范化 JSON 正文的 SHA-256，等价的 JSON 排版复领同一实收。运输校验以配置好的单一正式发行源为业务信任边界。

镜像从本地证据中选择达到回执下界的最早完整修订，并随核账结果给出请求回执和交账回执。合法形态的回执在证据不足时得到待件结果。待件仅含请求身份和范围。`samples/daily.json` 与 `samples/transfer.json` 的业务对照为东区 R 由 7 mm 转为西区 11 mm，西区先到时双区待件，北区仍报 5 mm，补齐后双区报 11 mm。

非法日批抛出 `BatchRejected`，同号异参抛出其子类 `BatchConflict`。未知收件抛出 `ReceiptNotFound`。非法更正、未知修订、无效发行回执分别抛出 `CorrectionRejected`、`RevisionNotFound`、`PublicationRejected`。坏件抛出 `PackageRejected`，身份或投影冲突抛出其子类 `PackageConflict`。非法镜像请求抛出 `InvalidMirrorRequest`。SQLite 写入故障回滚当前办理并传播原异常。

## 冷镜像换页的公开面

`ROLLOVER.md` 定义冷镜像换页（`publish_rollover`、`initialize_rollover`、`receive_rollover`、`read_rollover`、`retire_rollover`）。实现位于 `lake/dispatch/rollover_wire.py` 与 `lake/receiving/rollover.py`，目录表随 `open_mirror` 建好。冷镜像的营业正文只存在固定槽中：目录保存身份、投影摘要与办理元数据，槽内保存 `OfficialRow` 正文。两组样例在 `samples/rollover.json`。

- 换页件冻结两份直接父子正式回执、旧分区原序和旧/新投影摘要，只携带新版替换行正文。
- 建档按旧分区件原序占用编号 0..main_count-1 的主槽；实收先把旧目标行备份进周转槽，再改写主槽，最后提交目录。
- 退旧先在目录记下 retired，再清空周转槽；仅当新版完全由主槽兑现且周转槽实空才返回成功。首次报 retired 后旧版永久退领，重开用原号续办或复领。
- `python3 -B verify_rollover.py` 覆盖两组样例与中断矩阵，并已并入 `check_project.py`（`ROLLOVER_VERIFY` 行）。`demo.py` 演示断源双领与退旧中断后续办。

