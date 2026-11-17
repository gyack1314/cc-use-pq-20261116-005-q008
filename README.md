# 气候指标批次采集台账

本项目供研究团队离线接收不同机构交付的气候指标摘录。系统核验来源和指标契约，保留每次批次修订，记录质量证据，并发布不可变、可校验的快照。演示数据均为本地合成数据，不代表真实气候观测，也不在运行时访问外部网站。

使用 Python 3.11 或更新版本即可运行，无第三方依赖。以下命令均从项目根目录执行。

## 快速演示

运行 python -m climateledger demo --workspace fixtures，完成本地演示：创建 SQLite 数据库、注册合成指标与来源、摄入两行数据、发布快照并校验导出。再次执行会重用相同批次和发布版本。

单独使用时，把 --db path/to/ledger.sqlite3 放在子命令前。依次执行 register-indicator contract.json、register-source contract.json；需要单位换算时执行 register-unit rule.json。用 ingest batch.manifest.json 摄入数据，batch ID 查看记录；待审批次用 approve ID REVISION 审核，然后用 publish ID REVISION 发布。

export VERSION --output bundle.json 导出指定快照，verify bundle.json 检查摘要，series INDICATOR --version VERSION 查询发布值。coverage、discrepancies、compare、replay 和 audit 分别用于查看覆盖、来源分歧、版本变化、原始文件重放及审计链。

## 参考基线与更正传播

研究团队把已发布的历史月份作为后续批次的异常参考。register-anomaly rule.json 为指标登记突变规则：sigma_threshold 给出相对参考窗口标准差的倍数阈值，min_baseline_points 给出最少参考点数，可选 min_delta 给出绝对最小偏移（与 sigma 取或，弥补两点窗口标准差恒等于半间距、z 分数被压低的问题）。摄入新批次时，质量门会用已发布的历史窗口计算突变（baseline_jump，severity 为 review），证据中保留参考点数、均值、标准差、得分与偏移，结论可解释、可复算。

机构补正早期观测时（含 revised_value 的修订批次），系统在同一事务内开启一个不可变的基线纪元 baseline epoch：记录被更正期间的旧值与新值，并找出同源同指标、期间晚于被更正期的后续采集批次，用“当前发布序列叠加更正值”重建参考窗口重新判定。每个受影响批次的旧结论、新结论、突变证据和是否需要人工确认都追加保存在纪元里。已发布批次属于不可变快照，其批次行上的原状态和审核理由不会被改写，刷新后的结论只存在于纪元中，旧快照仍可独立导出和校验；尚未发布的后续批次若新结论改变则就地更新并回到待审核队列。

confirm-baseline ASSESSMENT_ID 用于人工确认刷新出的突变结论，重复确认幂等。publish 在发布前检查：只要触发批次打开的纪元仍有未确认项，或该批次自身带着未确认的刷新结论，就以 baseline_confirmation_pending 拒绝，避免过期审核结果进入新版本；全部确认后才能发布。baseline-epochs、baseline-epoch ID、baseline-pending、batch-baseline ID、anomaly-rules 分别用于查看纪元、单轮传播明细、待确认队列、单批次基线结论和已登记规则。

纪元按触发批次幂等：相同字节重复交付走幂等摄入，中断后重试只复用既有纪元，不会重复触发同一轮变更；纪元与重判记录带唯一约束，整个传播在单事务内完成，中途失败不留半成品。

## 输入与状态

指标契约包含 key、title、canonical_unit 和 cadence（月份或年份）；可附带上下限、精度和负值许可。来源契约包含标识、标题、授权说明及允许提交的指标列表。相同标识下契约不可静默改写。单位规则采用“原值乘以 factor 加 offset”换算为规范单位。

批次清单指定来源、外部批次编号、同目录的 CSV 或 JSON 数据文件、文件 SHA-256、行数及四个字段的列映射：indicator、period、value、unit。文件仅从本地读取。非法行形成拒收证据；序列内部缺期和已发布值的变更需人工审核，不会被悄悄修正。相同字节重复提交幂等，内容变化形成修订。清单中的行数和摘要可检测不完整或被修改的输入。

发布版本包含上个版本的摘要、来源文件摘要、批次修订、当前测量集合及导出摘要。不同来源对同一指标和期间的观测保持独立。旧版本始终可以独立导出和验证。

## 验证命令

python -m unittest discover -s tests -v

python -m compileall -q climateledger

python -m climateledger demo --workspace fixtures
