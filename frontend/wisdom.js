// ── 登录提醒（防败家子）──────────────────────────────

const WISDOM_ZH = [
  {
    title: "息壤家训 · 时间是最好的朋友",
    body: "这套系统的核心不是「选对时机」，而是「待够时间」。\n\n" +
      "标普 500 过去 30 年年化约 10%，但如果你错过涨幅最大的 10 个交易日，收益直接腰斩。" +
      "这些天几乎无法预测——唯一的策略就是一直在场。\n\n" +
      "📌 定投 + 持有 + 再平衡 = 时间复利的完整公式。\n" +
      "❌ 恐慌卖出 = 把浮亏变成真亏，永远失去回本的机会。",
  },
  {
    title: "息壤家训 · 败家子才频繁操作",
    body: "巴菲特说：「如果你不打算持有一只股票十年，那连十分钟都不要持有。」\n\n" +
      "这套永久组合的设计初衷，就是让你在任何市场环境下都能安睡。" +
      "系统会自动调仓、自动避险——你要做的只有一件事：不要碰它。\n\n" +
      "📌 每次想「抄底」或「逃顶」时，请重读这段话。\n" +
      "❌ 打败这套系统最可靠的方式，就是管不住手。",
  },
  {
    title: "息壤家训 · 定投是普通人的超级武器",
    body: "定期定额投入不需要你判断市场方向。\n\n" +
      "市场下跌时，同样的金额买到更多份额——这就是「微笑曲线」。" +
      "只要坚持得够久，成本会被时间拉到令人满意的水平。\n\n" +
      "📌 最好的投资时间是十年前，其次是现在。\n" +
      "📌 最差的投资决定是「等一等，再看看」。\n" +
      "❌ 停止定投 = 亲手关闭复利引擎。",
  },
  {
    title: "息壤家训 · 波动不是风险，退出才是",
    body: "市场每年平均回撤 14%，这是正常的呼吸节奏。\n\n" +
      "1987 黑色星期一跌了 22%，一年后全部收复。\n" +
      "2008 金融危机跌了 56%，五年后创历史新高。\n" +
      "2020 疫情闪崩跌了 34%，五个月后收复失地。\n\n" +
      "📌 每一次恐慌都是对持有者的考试，通过的人拿走了所有回报。\n" +
      "❌ 在恐慌中卖出 = 用最低价把筹码交给别人。",
  },
  {
    title: "息壤家训 · 家族财富的敌人是情绪",
    body: "贪婪让你追高，恐惧让你割肉，无聊让你频繁交易——每一种情绪都在蚕食复利。\n\n" +
      "这套系统存在的意义，就是把投资决策从人脑交给规则。" +
      "规则不会恐惧，不会贪婪，不会因为一条新闻就改变策略。\n\n" +
      "📌 信任系统 > 相信直觉。你的直觉已经被市场噪音污染了。\n" +
      "📌 富不过三代的本质，不是钱不够多，而是纪律不够久。",
  },
  {
    title: "息壤家训 · 四等分的智慧",
    body: "股票、债券、黄金、现金各占 25%——这不是偷懒，这是经过半个世纪验证的永久组合。\n\n" +
      "股票提供长期增长，债券在衰退中稳住阵脚，黄金对冲通胀和黑天鹅，现金是最后的弹药。" +
      "四种资产的相关性天然互补：总有一个在涨，总有一个在兜底。\n\n" +
      "📌 不要因为某一类资产暂时跑输就觉得它「没用」——它正在为下一次危机站岗。\n" +
      "❌ 擅自调高股票比例 = 用你的判断赌赢整个市场，历史证明这几乎不可能。",
  },
  {
    title: "息壤家训 · 再平衡是免费的午餐",
    body: "当某类资产偏离目标权重超过 5%，系统会自动再平衡——卖出涨多的，买入跌多的。\n\n" +
      "这看起来违反直觉：为什么要卖掉「赢家」去买「输家」？" +
      "因为均值回归是市场最古老的规律。今天的赢家常常是明天的输家，反之亦然。" +
      "再平衡本质上是在强制执行「低买高卖」。\n\n" +
      "📌 5% 的偏离阈值经过 6 轮回测验证，不要手动干预这个过程。\n" +
      "❌ 「让赢家继续跑」听起来聪明，实际是在积累集中度风险。",
  },
  {
    title: "息壤家训 · 风控是系统的安全带",
    body: "回撤达到 -12% 时系统自动进入保护模式，现金提升至 50%；\n" +
      "回撤达到 -14% 时触发硬止损，现金提升至 75%。\n" +
      "20 天冷却期结束且风险解除后，系统才会恢复正常配置。\n\n" +
      "这套机制的意义不是「躲过每一次下跌」——没有任何系统能做到。" +
      "它的意义是确保在极端情况下，家族资产不会遭受致命一击。\n\n" +
      "📌 看到系统进入保护模式时不要恐慌，这恰恰说明它在正常工作。\n" +
      "❌ 手动关闭风控 = 在暴风雨中解开安全带。",
  },
  {
    title: "息壤家训 · 股债相关性是隐形哨兵",
    body: "正常市场中，股票和债券呈负相关——股票跌时债券涨，组合天然对冲。\n" +
      "但当股债相关性突破 0.5 且同时下跌时，意味着市场结构出现异常（如 2022 年）。\n\n" +
      "息壤会实时监测 30 天滚动相关性。一旦相关性崩溃，系统在回撤阈值之前就会提前预警并切入保护模式。" +
      "这就是为什么我们不只看回撤，还看资产之间的「配合关系」。\n\n" +
      "📌 相关性监控 + 回撤阈值 = 双保险。普通投资者根本意识不到这层风险。\n" +
      "❌ 别因为「最近股债都在涨」就觉得风控多余——相关性会在你最不注意时翻脸。",
  },
  {
    title: "息壤家训 · 双市场是家族的护城河",
    body: "美股组合（SPY + TLT + GLD + SHV）和中国组合（沪深300 + 国债ETF + 黄金ETF + 货币基金）同时运行。\n\n" +
      "中美两个市场的经济周期、政策节奏、货币环境往往不同步。" +
      "当一个市场进入熊市时，另一个可能正在复苏——这是国家级别的分散化。\n\n" +
      "📌 两套组合用同一套风控规则，但标的完全独立，互不干扰。\n" +
      "📌 不要因为某个市场短期表现好就把资金全部转移过去——你无法预测下一轮牛市在哪。\n" +
      "❌ 单押一个市场 = 把家族命运绑在一个国家的经济周期上。",
  },
  {
    title: "息壤家训 · 我们不追求最亮眼，只追求活得最久",
    body: "这套策略从来不会是市面上收益最高的。任何一年，你都能找到跑赢我们几倍的基金、策略、甚至某个朋友的账户。\n\n" +
      "但请记住：Harry Browne 的永久组合从 1972 年运行至今，穿越了石油危机、互联网泡沫、金融海啸、疫情闪崩，从未遭受致命打击。" +
      "Ray Dalio 的全天候策略、Swensen 的耶鲁模型，核心思想完全一致——不预测、不押注、不择时，用资产配置的数学规律对抗不确定性。\n\n" +
      "华尔街最古老的格言是：「市场上有老交易员，也有大胆的交易员，但没有又老又大胆的交易员。」" +
      "我们选择做老的那个。收益率可能不够刺激，但我们的目标从来不是刺激——是在任何风暴中都能站着走出来。\n\n" +
      "📌 规则就是规则。不因牛市而激进，不因熊市而恐慌，不因别人赚得多就怀疑自己。\n" +
      "📌 活得久才是最强的复利。一个年化 8% 跑 30 年的系统，终值碾压年化 30% 但第 5 年爆仓的天才。\n" +
      "❌ 羡慕别人的高收益 = 只看到了幸存者，忽略了背后倒下的 99%。",
  },
];

const WISDOM_EN = [
  {
    title: "Family Rule · Time Is Your Greatest Ally",
    body: "This system's edge isn't about timing the market — it's about time IN the market.\n\n" +
      "The S&P 500 has returned ~10% annually over the past 30 years. But miss the best 10 trading days and your return is cut in half. " +
      "Those days are almost impossible to predict — the only strategy is to stay invested.\n\n" +
      "📌 DCA + Hold + Rebalance = The complete compound interest formula.\n" +
      "❌ Panic selling = turning paper losses into permanent ones.",
  },
  {
    title: "Family Rule · Don't Be the Weak Link",
    body: "Buffett said: \"If you aren't willing to own a stock for 10 years, don't even think about owning it for 10 minutes.\"\n\n" +
      "This permanent portfolio is designed to let you sleep well in any market condition. " +
      "The system auto-rebalances and auto-hedges. Your only job is to leave it alone.\n\n" +
      "📌 Every time you want to \"buy the dip\" or \"sell the top,\" re-read this.\n" +
      "❌ The most reliable way to beat this system is to not keep your hands off it.",
  },
  {
    title: "Family Rule · DCA Is a Superpower",
    body: "Dollar-cost averaging doesn't require you to predict market direction.\n\n" +
      "When the market drops, the same amount buys more shares — that's the \"smile curve.\" " +
      "Stick with it long enough and time will pull your average cost to a satisfying level.\n\n" +
      "📌 The best time to invest was 10 years ago. The second best time is now.\n" +
      "📌 The worst decision is \"let me wait and see.\"\n" +
      "❌ Stopping your DCA = manually shutting down the compounding engine.",
  },
  {
    title: "Family Rule · Volatility Is Not Risk. Quitting Is.",
    body: "The market drops an average of 14% per year. That's normal breathing.\n\n" +
      "1987 Black Monday fell 22% — recovered within a year.\n" +
      "2008 Financial Crisis fell 56% — hit new highs within 5 years.\n" +
      "2020 COVID crash fell 34% — recovered in 5 months.\n\n" +
      "📌 Every panic is a test. Those who pass collect all the returns.\n" +
      "❌ Selling in panic = handing your shares to someone else at the worst price.",
  },
  {
    title: "Family Rule · Emotion Is the Enemy of Wealth",
    body: "Greed makes you chase highs. Fear makes you sell lows. Boredom makes you overtrade. Every emotion erodes compounding.\n\n" +
      "This system exists to take investment decisions out of the human brain and into rules. " +
      "Rules don't fear, don't get greedy, don't change strategy because of one headline.\n\n" +
      "📌 Trust the system > Trust your gut. Your gut is already contaminated by market noise.\n" +
      "📌 \"Wealth doesn't survive three generations\" isn't about money — it's about discipline.",
  },
  {
    title: "Family Rule · The Wisdom of Four Equal Parts",
    body: "Stocks, Bonds, Gold, Cash — each 25%. This isn't laziness. It's a permanent portfolio validated over half a century.\n\n" +
      "Stocks provide long-term growth. Bonds stabilize during recessions. Gold hedges inflation and black swans. Cash is your last ammunition. " +
      "These four assets are naturally complementary: something is always rising, something is always cushioning.\n\n" +
      "📌 Don't dismiss an asset class because it's temporarily underperforming — it's standing guard for the next crisis.\n" +
      "❌ Overweighting stocks = betting your judgment against the entire market. History says you'll lose.",
  },
  {
    title: "Family Rule · Rebalancing Is the Only Free Lunch",
    body: "When any asset drifts more than 5% from its target weight, the system auto-rebalances — selling winners, buying losers.\n\n" +
      "This feels counterintuitive: why sell what's winning to buy what's losing? " +
      "Because mean reversion is the market's oldest law. Today's winner is often tomorrow's laggard. " +
      "Rebalancing is forced \"buy low, sell high.\"\n\n" +
      "📌 The 5% drift threshold has been validated through 6 backtest cycles. Don't interfere.\n" +
      "❌ \"Let winners run\" sounds smart but actually accumulates concentration risk.",
  },
  {
    title: "Family Rule · Risk Control Is Your Seatbelt",
    body: "At -12% drawdown, the system enters protection mode (cash raised to 50%).\n" +
      "At -14% drawdown, hard stop triggers (cash raised to 75%).\n" +
      "A 20-day cooling period must pass before normal allocation resumes.\n\n" +
      "This mechanism doesn't dodge every decline — no system can. " +
      "Its purpose is to ensure the family's assets never suffer a fatal blow in extreme scenarios.\n\n" +
      "📌 Seeing protection mode activate means the system is working, not failing.\n" +
      "❌ Manually disabling risk controls = unbuckling your seatbelt in a storm.",
  },
  {
    title: "Family Rule · Stock-Bond Correlation Is the Hidden Sentinel",
    body: "Normally, stocks and bonds are negatively correlated — when stocks fall, bonds rise, providing a natural hedge.\n" +
      "But when the 30-day rolling correlation exceeds 0.5 and both decline simultaneously, something structural has broken (like 2022).\n\n" +
      "Xi-Rang monitors correlation in real-time. When the hedge relationship breaks down, " +
      "the system triggers protective measures even before drawdown thresholds are reached.\n\n" +
      "📌 Correlation monitoring + drawdown thresholds = double insurance.\n" +
      "❌ Don't think risk controls are unnecessary because \"stocks and bonds are both rising\" — correlation flips when you least expect it.",
  },
  {
    title: "Family Rule · Dual Markets Are the Family's Moat",
    body: "US Portfolio (SPY + TLT + GLD + SHV) and China Portfolio (CSI 300 + Gov Bond + Gold + Money Market) run simultaneously.\n\n" +
      "The US and China have different economic cycles, policy rhythms, and monetary environments. " +
      "When one market enters a bear phase, the other may be recovering — this is nation-level diversification.\n\n" +
      "📌 Both portfolios use identical risk rules but completely independent assets.\n" +
      "📌 Don't shift all funds to whichever market is hot — you can't predict where the next bull run starts.\n" +
      "❌ Betting on one market = tying the family's fate to a single country's economic cycle.",
  },
  {
    title: "Family Rule · We Don't Chase the Spotlight. We Outlast Everyone.",
    body: "This strategy will never be the top performer in any given year. You'll always find a fund, a trader, or a friend's portfolio that crushed ours over the last 12 months.\n\n" +
      "But consider this: Harry Browne's Permanent Portfolio has run since 1972 — through the oil crisis, the dot-com bust, the 2008 meltdown, and the COVID crash — and has never suffered a fatal blow. " +
      "Ray Dalio's All Weather, David Swensen's Yale Model — the core philosophy is identical: no predictions, no bets, no market timing. Just the mathematical laws of asset allocation against uncertainty.\n\n" +
      "Wall Street's oldest saying: \"There are old traders, and there are bold traders, but there are no old, bold traders.\" " +
      "We choose to be old. Returns may not be thrilling, but our goal was never to thrill — it's to walk out of every storm still standing.\n\n" +
      "📌 Rules are rules. Don't get aggressive in bull markets, don't panic in bear markets, don't doubt yourself because someone else got lucky.\n" +
      "📌 Longevity is the ultimate compounding edge. An 8% system that runs for 30 years crushes a 30% genius that blows up in year 5.\n" +
      "❌ Envying others' high returns = seeing only the survivors, ignoring the 99% who fell behind them.",
  },
];

let _wisdomTimer = null;

function showWisdomReminder() {
  return new Promise(function(resolve) {
    const lang = getLang();
    const pool = lang === "en" ? WISDOM_EN : WISDOM_ZH;
    const pick = pool[Math.floor(Math.random() * pool.length)];

    document.getElementById("wisdom-title").textContent = pick.title;
    document.getElementById("wisdom-body").innerText = pick.body;

    const btn = document.getElementById("btn-wisdom-confirm");
    const countdown = document.getElementById("wisdom-countdown");
    btn.disabled = true;

    let remaining = 5;
    const cdLabel = lang === "en" ? "Please read carefully… (%ds)" : "请认真阅读…（%d秒）";
    countdown.textContent = cdLabel.replace("%d", remaining);

    var overlay = document.getElementById("wisdom-overlay");
    overlay.classList.add("open");

    // Focus trap for wisdom modal
    setTimeout(function() {
      var modal = overlay.querySelector(".wisdom-modal");
      if (modal && typeof trapFocus === "function") trapFocus(modal);
    }, 100);

    _wisdomTimer = setInterval(() => {
      remaining--;
      if (remaining > 0) {
        countdown.textContent = cdLabel.replace("%d", remaining);
      } else {
        clearInterval(_wisdomTimer);
        _wisdomTimer = null;
        countdown.textContent = "";
        btn.disabled = false;
      }
    }, 1000);

    // 使用一次性监听器，确认后 resolve Promise
    function onConfirm() {
      if (_wisdomTimer) { clearInterval(_wisdomTimer); _wisdomTimer = null; }
      if (typeof releaseFocus === "function") releaseFocus();
      overlay.classList.remove("open");
      btn.removeEventListener("click", onConfirm);
      resolve();
    }
    btn.addEventListener("click", onConfirm);
  });
}

// 保留旧的监听器兼容（如果有外部直接点击的场景）
document.getElementById("btn-wisdom-confirm").addEventListener("click", () => {
  if (_wisdomTimer) { clearInterval(_wisdomTimer); _wisdomTimer = null; }
  if (typeof releaseFocus === "function") releaseFocus();
  document.getElementById("wisdom-overlay").classList.remove("open");
});
