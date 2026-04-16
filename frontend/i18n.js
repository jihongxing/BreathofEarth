/**
 * 息壤 Xi-Rang — 国际化 (i18n)
 * 支持中文 / English 双语切换
 */

const LANGS = {
  zh: {
    // ── 通用 ──
    appName: "息壤",
    appSubtitle: "家族财富自动化配置系统",
    login: "登录",
    logout: "退出",
    username: "用户名",
    password: "密码",
    submit: "提交",
    cancel: "取消",
    approve: "批准",
    reject: "拒绝",
    unauthorized: "未授权",
    loadFailed: "加载失败",

    // ── 选项卡 ──
    tabDashboard: "仪表盘",
    tabWithdrawals: "出金管理",
    tabTransactions: "调仓历史",
    tabAlpha: "达尔文沙盒",
    tabReport: "家族月报",

    // ── 仪表盘 ──
    netAsset: "净资产",
    status: "状态",
    currentDrawdown: "当前回撤",
    navTrend: "净资产走势",
    assetAllocation: "资产配置",
    drawdownMonitor: "回撤监控",
    riskEvents: "风控事件",
    noRiskEvents: "暂无风控事件",
    drawdownLabel: "回撤",

    // ── 出金管理 ──
    withdrawalMgmt: "出金管理",
    newWithdrawal: "发起出金",
    amountUSD: "金额 (USD)",
    reason: "原因",
    noWithdrawals: "暂无出金记录",
    withdrawFail: "出金失败",
    thId: "ID",
    thAmount: "金额",
    thReason: "原因",
    thRequester: "发起人",
    thStatus: "状态",
    thApproval: "审批",
    thAction: "操作",
    approveBtn: "审批",
    approvalTitle: "审批出金请求",
    approvalDetail: "金额: {amount} | 原因: {reason}",
    commentPlaceholder: "备注（可选）",
    approvalFail: "审批失败",

    // ── 资金管理（入金+出金）──
    tabFunds: "资金管理",
    fundsMgmt: "资金管理",
    depositSection: "入金",
    withdrawalSection: "出金",
    newDeposit: "发起入金",
    depositAmount: "入金金额",
    depositPreview: "预览分配方案",
    depositConfirm: "确认入金",
    depositSuccess: "入金成功",
    depositFail: "入金失败",
    depositPreviewTitle: "入金分配方案",
    depositStep1: "第一步：全额进入 Stability 层",
    depositStep2: "第二步：系统按目标配比分配到 Core 层",
    depositStayStability: "留在 Stability",
    depositTransferCore: "转入 Core",
    depositCoreDetail: "Core 层分配明细",
    depositBefore: "入金前",
    depositAfter: "入金后",
    depositProtectionNote: "当前 PROTECTION 状态，资金全额留在 Stability，下一周期系统自动分配",
    depositNormalNote: "确认后资金立即入账，系统按上述方案分配",
    depositPhaseTitle: "入金流程说明",
    depositPhase1: "立即到账",
    depositPhase1Desc: "资金全额进入 Stability（流动性层），您的净资产即刻更新",
    depositPhase2: "下一周期自动分配",
    depositPhase2Desc: "系统在下一交易周期按目标配比自动将部分资金从 Stability 转入 Core（抗通胀层）",
    depositInputAmount: "入金金额",
    depositAllocationTitle: "资金分配方案",
    depositImmediateAlloc: "立即分配",
    depositNextCycleAlloc: "下一周期系统自动执行",
    depositTotalDeposit: "本次入金总额",
    depositAllToStability: "→ 全部进入 Stability 层",
    depositNextCyclePlan: "下一周期分配计划",
    depositKeepStability: "保留在 Stability",
    depositMoveToCore: "转入 Core 层",
    depositTimeline: "时间线",
    depositNow: "现在",
    depositNextCycle: "下一交易周期",
    depositProtectionModeTitle: "⚠️ 保护模式",
    depositProtectionModeDesc: "当前组合处于 PROTECTION 状态，本次入金将全额留在 Stability 层，不会转入 Core。系统将在保护期结束后的下一个周期自动分配。",
    depositConfirmHint: "请确认上述分配方案，确认后资金立即到账",
    withdrawPhaseTitle: "出金流程说明",
    withdrawPhase1: "提交申请",
    withdrawPhase1Desc: "系统生成扣减方案，优先从 Stability 层扣减，不足部分从 Core 层扣减",
    withdrawPhase2: "审批通过后执行",
    withdrawPhase2Desc: "出金请求需要审批通过后才会实际执行扣减",
    withdrawDeductPlan: "扣减方案",
    withdrawConfirmHint: "请确认上述扣减方案，提交后需等待审批",
    layerCore: "Core（抗通胀层）",
    layerStability: "Stability（流动性层）",
    layerRatio: "占比",
    withdrawPreview: "预览扣减方案",
    withdrawPreviewTitle: "出金扣减方案",
    withdrawDeductFrom: "扣减来源",
    withdrawFriction: "预估摩擦成本",
    withdrawStabilityWarn: "⚠️ 出金后 Stability 低于安全线",
    noDeposits: "暂无入金记录",
    depositHistory: "入金记录",

    // ── 调仓历史 ──
    rebalanceHistory: "调仓历史",
    noTransactions: "暂无调仓记录",
    thDate: "日期",
    thType: "类型",
    thTurnover: "换手率",
    thFriction: "摩擦成本",

    // ── 达尔文沙盒 ──
    alphaTitle: "达尔文沙盒 — Alpha 策略竞技场",
    alphaLedgerTitle: "Alpha 独立账本",
    alphaManualOnlyHint: "Alpha 出金只允许提交人工出金申请，系统不会自动打款或自动扣账。",
    alphaManualInflow: "人工入账",
    alphaManualOutflow: "人工出账记账",
    alphaWithdrawRequest: "提交人工出金申请",
    alphaWithdrawReason: "申请原因",
    alphaWithdrawFormHint: "提交后仅登记人工出金申请，不会自动扣减 Alpha 账本余额。",
    alphaLedgerDirection: "方向",
    alphaDirectionIn: "入账",
    alphaDirectionOut: "出账",
    alphaEntryNote: "备注",
    alphaEntryExtRef: "外部流水号",
    alphaEntryLinkRequest: "关联申请ID",
    alphaEntryFormHint: "这里只记录线下已完成的资金变动，不会触发真实打款。",
    alphaEntrySubmit: "登记账务流水",
    alphaLedgerEntriesTitle: "人工账务流水",
    alphaNoLedgerEntries: "暂无 Alpha 手工账务流水",
    alphaBalanceAfter: "记账后余额",
    alphaActor: "操作人",
    alphaEntrySuccess: "Alpha 手工账务流水已登记",
    alphaCashBalance: "账本余额",
    alphaInflows: "累计入金",
    alphaOutflows: "累计人工出金",
    alphaLastAdjust: "最近人工备注",
    alphaWithdrawalDeskTitle: "Alpha 人工出金处理台",
    alphaWithdrawalDeskHint: "这里仅登记申请与回填人工处理结果，不代表系统已自动出金。",
    alphaStatusFilter: "状态筛选",
    alphaStatusAll: "全部状态",
    alphaStatusPending: "待人工处理",
    alphaStatusHandled: "已人工处理",
    alphaStatusRejected: "已拒绝",
    alphaStatusCancelled: "已取消",
    alphaNoWithdrawRequests: "暂无 Alpha 人工出金申请",
    alphaHandleBtn: "处理",
    alphaHandleTitle: "回填人工处理结果",
    alphaHandleNote: "处理备注",
    alphaExternalRef: "外部流水号",
    alphaHandleFormHint: "这一步只回填人工处理结果，不会触发系统自动出金或自动扣账。",
    alphaSubmitHandle: "提交处理结果",
    alphaHandleSuccess: "人工处理结果已回填",
    alphaSandboxOnly: "仅沙盒观察",
    alphaFormalExcluded: "未纳入正式收益汇报",
    alphaFormalOnlyLeaderboard: "正式排行榜只展示已完成真实账本闭环的策略；当前策略仍停留在沙盒层。",
    alphaFormalEvalBlocked: "当前没有可进入正式排行榜的 Alpha 策略",
    runAll: "运行所有策略",
    quarterlyEval: "季度评估",
    noStrategies: "暂无可用策略",
    statusEnabled: "运行中",
    statusSuspended: "已暂停",
    statusDisabled: "未启用",
    toggleEnable: "开启",
    toggleDisable: "关闭",
    manualRun: "手动执行",
    labelAllocation: "分配",
    labelCapital: "资金",
    labelPnl: "损益",
    labelTrades: "交易",
    leaderboard: "策略排行榜",
    noLeaderboard: "暂无策略数据",
    thRank: "#",
    thStrategy: "策略",
    thSharpe: "夏普",
    thAnnualReturn: "年化收益",
    thMaxDrawdown: "最大回撤",
    thWinRate: "胜率",
    thTradeCount: "交易次数",
    thTotalPnl: "总损益",
    strategyTxTitle: "策略交易记录",
    selectStrategy: "选择策略",
    noAlphaTx: "暂无交易记录",
    thActionCol: "动作",
    thUnderlying: "标的",
    thStrike: "价格/行权价",
    thExpiry: "到期/数量",
    thContracts: "合约/股数",
    thIncome: "收入",
    thDetail: "详情",
    evalResultTitle: "季度评估结果",
    ranStrategies: "已运行 {n} 个策略",
    runFail: "运行失败",
    evalFail: "评估失败",
    shares: "股",

    // ── 家族月报 ──
    monthlyReport: "家族月报",
    generateReport: "生成最新报告",
    noReport: "暂无月报",
    noReportHint: "点击「生成最新报告」创建第一份月报",
    reportGenerated: "报告已生成",
    reportGenFail: "生成失败",
    reportBack: "返回",
    reportLoading: "加载中...",
    wisdomConfirm: "我已阅读并谨记",

    // ── 数据状态 ──
    tabDataStatus: "数据状态",
    dataStatusTitle: "数据新鲜度监控",
    dataStatusDesc: "所有 Ticker 的本地数据状态，陈旧数据可能导致决策偏差",
    dataCheckedAt: "检查时间",
    dataTickerCol: "标的",
    dataRowsCol: "数据行数",
    dataRangeCol: "日期范围",
    dataStaleDaysCol: "陈旧天数",
    dataLevelCol: "状态",
    dataLevelFresh: "最新",
    dataLevelStale: "需更新",
    dataLevelOutdated: "严重过期",
    dataLevelMissing: "缺失",
    dataMarketFiles: "市场数据文件",
    dataLiveFiles: "Live 数据",
    dataRawTickers: "原始 Ticker 缓存",
    dataNoData: "暂无数据",
    dataUpdateHint: "运行 python -m data.data_manager --update 更新数据",
    dataStaleDayUnit: "天未更新",
    dataUpdateAll: "更新全部数据",
    dataUpdateLive: "更新 Live",
    dataUpdateTicker: "更新此标的",
    dataUpdating: "更新中...",
    dataUpdateDone: "更新完成",
    dataUpdateError: "更新失败",
    dataUpdateBusy: "已有任务在运行",
    dataUpdateStarted: "更新任务已启动",
    dataViewLogs: "查看日志",
    dataHideLogs: "收起日志",
    schedulerEnabled: "自动更新已开启",
    schedulerNextLive: "下次 Live",
    schedulerNextFull: "下次全量",

    // ── 策略展示 ──
    defaultIncomeLabel: "收入",
    strategies: {
      covered_call: {
        incomeLabel: "权利金",
        actionLabels: {
          SELL_CALL: "卖出看涨", EXPIRE: "到期作废", ASSIGN: "被行权", HOLD: "持有",
        },
      },
      grid_trading: {
        incomeLabel: "网格利润",
        actionLabels: {
          GRID_INIT: "初始化", GRID_BUY: "网格买入", GRID_SELL: "网格卖出",
          GRID_STOP: "止损", HOLD: "持有",
        },
      },
      momentum_rotation: {
        incomeLabel: "轮动收入",
        actionLabels: {
          MOM_BUY: "买入", MOM_SELL: "卖出", MOM_SWITCH: "轮动切换", HOLD: "持有",
        },
      },
    },
  },

  en: {
    // ── General ──
    appName: "Terragen",
    appSubtitle: "Family Wealth Automation System",
    login: "Login",
    logout: "Logout",
    username: "Username",
    password: "Password",
    submit: "Submit",
    cancel: "Cancel",
    approve: "Approve",
    reject: "Reject",
    unauthorized: "Unauthorized",
    loadFailed: "Load failed",

    // ── Tabs ──
    tabDashboard: "Dashboard",
    tabWithdrawals: "Withdrawals",
    tabTransactions: "Rebalance History",
    tabAlpha: "Darwin Sandbox",
    tabReport: "Monthly Report",

    // ── Dashboard ──
    netAsset: "Net Asset",
    status: "Status",
    currentDrawdown: "Drawdown",
    navTrend: "NAV Trend",
    assetAllocation: "Asset Allocation",
    drawdownMonitor: "Drawdown Monitor",
    riskEvents: "Risk Events",
    noRiskEvents: "No risk events",
    drawdownLabel: "Drawdown",

    // ── Withdrawals ──
    withdrawalMgmt: "Withdrawal Management",
    newWithdrawal: "New Withdrawal",
    amountUSD: "Amount (USD)",
    reason: "Reason",
    noWithdrawals: "No withdrawal records",
    withdrawFail: "Withdrawal failed",
    thId: "ID",
    thAmount: "Amount",
    thReason: "Reason",
    thRequester: "Requester",
    thStatus: "Status",
    thApproval: "Approval",
    thAction: "Action",
    approveBtn: "Review",
    approvalTitle: "Approve Withdrawal",
    approvalDetail: "Amount: {amount} | Reason: {reason}",
    commentPlaceholder: "Comment (optional)",
    approvalFail: "Approval failed",

    // ── Fund Management ──
    tabFunds: "Fund Management",
    fundsMgmt: "Fund Management",
    depositSection: "Deposit",
    withdrawalSection: "Withdrawal",
    newDeposit: "New Deposit",
    depositAmount: "Deposit Amount",
    depositPreview: "Preview Allocation",
    depositConfirm: "Confirm Deposit",
    depositSuccess: "Deposit successful",
    depositFail: "Deposit failed",
    depositPreviewTitle: "Deposit Allocation Plan",
    depositStep1: "Step 1: 100% enters Stability layer",
    depositStep2: "Step 2: System allocates to Core per target ratio",
    depositStayStability: "Stays in Stability",
    depositTransferCore: "Transfer to Core",
    depositCoreDetail: "Core Layer Allocation Detail",
    depositBefore: "Before Deposit",
    depositAfter: "After Deposit",
    depositProtectionNote: "PROTECTION mode: funds stay in Stability, auto-allocated next cycle",
    depositNormalNote: "Funds will be allocated per the plan above upon confirmation",
    depositPhaseTitle: "Deposit Flow",
    depositPhase1: "Instant",
    depositPhase1Desc: "Funds enter Stability (liquidity layer) in full, your NAV updates immediately",
    depositPhase2: "Next Cycle Auto-Allocation",
    depositPhase2Desc: "System will automatically transfer part of the funds from Stability to Core (inflation shield) per target ratio in the next trading cycle",
    depositInputAmount: "Deposit Amount",
    depositAllocationTitle: "Allocation Plan",
    depositImmediateAlloc: "Immediate Allocation",
    depositNextCycleAlloc: "Executed by system next cycle",
    depositTotalDeposit: "Total Deposit",
    depositAllToStability: "→ All enters Stability layer",
    depositNextCyclePlan: "Next Cycle Allocation Plan",
    depositKeepStability: "Remains in Stability",
    depositMoveToCore: "Transfers to Core",
    depositTimeline: "Timeline",
    depositNow: "Now",
    depositNextCycle: "Next Trading Cycle",
    depositProtectionModeTitle: "⚠️ Protection Mode",
    depositProtectionModeDesc: "Portfolio is in PROTECTION state. All funds will stay in Stability layer and will not transfer to Core. The system will auto-allocate in the next cycle after protection ends.",
    depositConfirmHint: "Please review the allocation plan above. Funds will be credited immediately upon confirmation.",
    withdrawPhaseTitle: "Withdrawal Flow",
    withdrawPhase1: "Submit Request",
    withdrawPhase1Desc: "System generates deduction plan, prioritizing Stability layer, then Core if needed",
    withdrawPhase2: "Executed After Approval",
    withdrawPhase2Desc: "Withdrawal requires approval before actual deduction is executed",
    withdrawDeductPlan: "Deduction Plan",
    withdrawConfirmHint: "Please review the deduction plan above. Approval required after submission.",
    layerCore: "Core (Inflation Shield)",
    layerStability: "Stability (Liquidity)",
    layerRatio: "Ratio",
    withdrawPreview: "Preview Deduction",
    withdrawPreviewTitle: "Withdrawal Deduction Plan",
    withdrawDeductFrom: "Deducted From",
    withdrawFriction: "Est. Friction Cost",
    withdrawStabilityWarn: "⚠️ Stability below safety threshold after withdrawal",
    noDeposits: "No deposit records",
    depositHistory: "Deposit History",

    // ── Rebalance History ──
    rebalanceHistory: "Rebalance History",
    noTransactions: "No rebalance records",
    thDate: "Date",
    thType: "Type",
    thTurnover: "Turnover",
    thFriction: "Friction Cost",

    // ── Darwin Sandbox ──
    alphaTitle: "Darwin Sandbox — Alpha Strategy Arena",
    alphaLedgerTitle: "Alpha Ledger",
    alphaManualOnlyHint: "Alpha withdrawals are request-only. The system will not auto-pay or auto-deduct funds.",
    alphaManualInflow: "Manual Inflow",
    alphaManualOutflow: "Record Manual Outflow",
    alphaWithdrawRequest: "Submit Manual Withdrawal Request",
    alphaWithdrawReason: "Request Reason",
    alphaWithdrawFormHint: "Submission only records a manual withdrawal request. Alpha ledger balance will not be deducted automatically.",
    alphaLedgerDirection: "Direction",
    alphaDirectionIn: "Inflow",
    alphaDirectionOut: "Outflow",
    alphaEntryNote: "Note",
    alphaEntryExtRef: "External Reference",
    alphaEntryLinkRequest: "Linked Request ID",
    alphaEntryFormHint: "This only records offline-completed cash movements. It will not trigger any real payout.",
    alphaEntrySubmit: "Record Ledger Entry",
    alphaLedgerEntriesTitle: "Manual Ledger Entries",
    alphaNoLedgerEntries: "No Alpha manual ledger entries",
    alphaBalanceAfter: "Balance After",
    alphaActor: "Actor",
    alphaEntrySuccess: "Alpha manual ledger entry recorded",
    alphaCashBalance: "Ledger Balance",
    alphaInflows: "Total Inflows",
    alphaOutflows: "Total Manual Withdrawals",
    alphaLastAdjust: "Latest Manual Note",
    alphaWithdrawalDeskTitle: "Alpha Manual Withdrawal Desk",
    alphaWithdrawalDeskHint: "This desk records requests and manual handling results only. It does not mean the system executed a withdrawal.",
    alphaStatusFilter: "Status Filter",
    alphaStatusAll: "All Statuses",
    alphaStatusPending: "Pending Manual Handling",
    alphaStatusHandled: "Handled Manually",
    alphaStatusRejected: "Rejected",
    alphaStatusCancelled: "Cancelled",
    alphaNoWithdrawRequests: "No Alpha manual withdrawal requests",
    alphaHandleBtn: "Handle",
    alphaHandleTitle: "Record Manual Handling Result",
    alphaHandleNote: "Handling Note",
    alphaExternalRef: "External Reference",
    alphaHandleFormHint: "This step records the manual handling result only. It will not trigger any automatic payout or ledger deduction.",
    alphaSubmitHandle: "Submit Result",
    alphaHandleSuccess: "Manual handling result recorded",
    alphaSandboxOnly: "Sandbox Only",
    alphaFormalExcluded: "Excluded from formal performance reporting",
    alphaFormalOnlyLeaderboard: "The formal leaderboard only shows strategies with a real closed ledger. Current Alpha strategies remain sandbox-only.",
    alphaFormalEvalBlocked: "No Alpha strategy is currently eligible for the formal leaderboard",
    runAll: "Run All Strategies",
    quarterlyEval: "Quarterly Evaluation",
    noStrategies: "No strategies available",
    statusEnabled: "Running",
    statusSuspended: "Suspended",
    statusDisabled: "Disabled",
    toggleEnable: "Enable",
    toggleDisable: "Disable",
    manualRun: "Manual Run",
    labelAllocation: "Allocation",
    labelCapital: "Capital",
    labelPnl: "PnL",
    labelTrades: "Trades",
    leaderboard: "Strategy Leaderboard",
    noLeaderboard: "No strategy data",
    thRank: "#",
    thStrategy: "Strategy",
    thSharpe: "Sharpe",
    thAnnualReturn: "Annual Return",
    thMaxDrawdown: "Max Drawdown",
    thWinRate: "Win Rate",
    thTradeCount: "Trades",
    thTotalPnl: "Total PnL",
    strategyTxTitle: "Strategy Transactions",
    selectStrategy: "Select Strategy",
    noAlphaTx: "No transaction records",
    thActionCol: "Action",
    thUnderlying: "Underlying",
    thStrike: "Strike/Price",
    thExpiry: "Expiry/Qty",
    thContracts: "Contracts",
    thIncome: "Income",
    thDetail: "Detail",
    evalResultTitle: "Quarterly Evaluation Result",
    ranStrategies: "Ran {n} strategies",
    runFail: "Run failed",
    evalFail: "Evaluation failed",
    shares: "shares",

    // ── Monthly Report ──
    monthlyReport: "Monthly Report",
    generateReport: "Generate Report",
    noReport: "No report available",
    noReportHint: "Click \"Generate Report\" to create the first one",
    reportGenerated: "Report generated",
    reportGenFail: "Generation failed",
    reportBack: "Back",
    reportLoading: "Loading...",
    wisdomConfirm: "I have read and will remember",

    // ── Data Status ──
    tabDataStatus: "Data Status",
    dataStatusTitle: "Data Freshness Monitor",
    dataStatusDesc: "Local data status for all tickers — stale data may cause decision errors",
    dataCheckedAt: "Checked at",
    dataTickerCol: "Ticker",
    dataRowsCol: "Rows",
    dataRangeCol: "Date Range",
    dataStaleDaysCol: "Stale Days",
    dataLevelCol: "Status",
    dataLevelFresh: "Fresh",
    dataLevelStale: "Needs Update",
    dataLevelOutdated: "Outdated",
    dataLevelMissing: "Missing",
    dataMarketFiles: "Market Data Files",
    dataLiveFiles: "Live Data",
    dataRawTickers: "Raw Ticker Cache",
    dataNoData: "No data available",
    dataUpdateHint: "Run python -m data.data_manager --update to refresh data",
    dataStaleDayUnit: "days since update",
    dataUpdateAll: "Update All",
    dataUpdateLive: "Update Live",
    dataUpdateTicker: "Update",
    dataUpdating: "Updating...",
    dataUpdateDone: "Update complete",
    dataUpdateError: "Update failed",
    dataUpdateBusy: "A task is already running",
    dataUpdateStarted: "Update task started",
    dataViewLogs: "View Logs",
    dataHideLogs: "Hide Logs",
    schedulerEnabled: "Auto-update enabled",
    schedulerNextLive: "Next Live",
    schedulerNextFull: "Next Full",

    // ── Strategy Display ──
    defaultIncomeLabel: "Income",
    strategies: {
      covered_call: {
        incomeLabel: "Premium",
        actionLabels: {
          SELL_CALL: "Sell Call", EXPIRE: "Expired", ASSIGN: "Assigned", HOLD: "Hold",
        },
      },
      grid_trading: {
        incomeLabel: "Grid Profit",
        actionLabels: {
          GRID_INIT: "Initialize", GRID_BUY: "Grid Buy", GRID_SELL: "Grid Sell",
          GRID_STOP: "Stop Loss", HOLD: "Hold",
        },
      },
      momentum_rotation: {
        incomeLabel: "Rotation Income",
        actionLabels: {
          MOM_BUY: "Buy", MOM_SELL: "Sell", MOM_SWITCH: "Rotate", HOLD: "Hold",
        },
      },
    },
  },
};


// ── i18n 引擎 ──

let currentLang = localStorage.getItem("xirang_lang") || (navigator.language.startsWith("zh") ? "zh" : "en");

/** 获取翻译文本，支持 t("key") 或嵌套 t("strategies.covered_call.incomeLabel") */
function t(key, params) {
  const keys = key.split(".");
  let val = LANGS[currentLang];
  for (const k of keys) {
    if (val == null) break;
    val = val[k];
  }
  if (val == null) {
    // fallback 到中文
    val = LANGS.zh;
    for (const k of keys) {
      if (val == null) break;
      val = val[k];
    }
  }
  if (typeof val !== "string") return key;
  // 替换 {param} 占位符
  if (params) {
    for (const [pk, pv] of Object.entries(params)) {
      val = val.replace(new RegExp(`\\{${pk}\\}`, "g"), pv);
    }
  }
  return val;
}

/** 获取当前语言 */
function getLang() { return currentLang; }

/** 切换语言并刷新所有 data-i18n 元素 */
function setLang(lang) {
  if (!LANGS[lang]) return;
  currentLang = lang;
  localStorage.setItem("xirang_lang", lang);
  document.documentElement.lang = lang === "zh" ? "zh-CN" : "en";
  applyI18n();
}

/** 将 data-i18n 属性应用到 DOM（用于静态 HTML 文本） */
function applyI18n() {
  document.querySelectorAll("[data-i18n]").forEach(el => {
    const key = el.getAttribute("data-i18n");
    const text = t(key);
    if (text !== key) el.textContent = text;
  });
  document.querySelectorAll("[data-i18n-placeholder]").forEach(el => {
    const key = el.getAttribute("data-i18n-placeholder");
    const text = t(key);
    if (text !== key) el.placeholder = text;
  });
  document.querySelectorAll("[data-i18n-aria]").forEach(el => {
    const key = el.getAttribute("data-i18n-aria");
    const text = t(key);
    if (text !== key) el.setAttribute("aria-label", text);
  });
  // 更新语言切换按钮状态 + aria-pressed
  document.querySelectorAll(".lang-btn").forEach(btn => {
    const isActive = btn.dataset.lang === currentLang;
    btn.classList.toggle("active", isActive);
    btn.setAttribute("aria-pressed", isActive ? "true" : "false");
  });
}
