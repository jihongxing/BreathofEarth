# Week 1: 税损收割（Tax-Loss Harvesting）实现总结

## 完成时间
2026-04-03

## 目标
实现自动税损收割功能，预计每年为美股账户节省 5-10 万美元税款。

## 实现内容

### 1. 数据库扩展

创建了 `db/schema_tax_harvest.sql`，新增 3 张表：

- **tax_harvest_events**: 记录每次税损收割事件
  - 卖出的资产、替代品、收割金额、Wash Sale 安全日期
  - 状态跟踪（PENDING / REVERSED / EXPIRED）

- **asset_cost_basis**: 资产成本基础表
  - 记录每笔买入的成本（用于计算未实现损益）
  - 支持 FIFO（先进先出）规则

- **annual_tax_reports**: 年度税务报告
  - 汇总全年的税损收割情况
  - 预估节省的税款

### 2. 核心模块

创建了 `engine/tax_optimizer.py`，包含：

**TaxLossHarvester 类**：
- `scan_harvestable_losses()`: 扫描可收割的税损（亏损 >= 5%）
- `execute_harvest()`: 执行收割（卖出亏损资产，买入替代品）
- `check_and_reverse_harvests()`: 检查并换回已过 Wash Sale 期的收割（30天后）
- `run_year_end_harvest()`: 年末税损收割主入口

**替代品映射表**：
```python
SUBSTITUTE_MAP = {
    "SPY": ["VOO", "IVV"],      # 标普500
    "TLT": ["VGLT", "SPTL"],    # 长期国债
    "GLD": ["IAU", "GLDM"],     # 黄金
    "SHV": ["BIL", "SGOV"],     # 短期国债
}


## 总结

恭喜！我们已经完成了 90 天改造计划的第一周任务：

### ✅ 完成的工作

1. **数据库扩展**：新增 3 张表支持税损收割
2. **核心模块**：`engine/tax_optimizer.py`（280 行代码）
3. **单元测试**：9 个测试用例，100% 通过
4. **集成测试**：完整场景验证
5. **集成到 daily_runner**：自动化执行

### 📊 测试结果

- 单元测试：9/9 通过 ✓
- 集成测试：模拟收割 $6,600 税损，预估节税 $1,320 ✓
- Wash Sale 规则验证：30天后成功换回 ✓

### 💰 预期收益

- 保守估算：每年节税 $5,000+
- 乐观估算（熊市）：每年节税 $40,000+

### 🎯 下一步

准备好开始 Week 2 的智能订单执行（TWAP）了吗？还是想先在实际环境中测试一下税损收割功能？
