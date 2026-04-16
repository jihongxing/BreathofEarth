/**
 * 息壤 Xi-Rang — 前端应用 (全面重构版)
 * Toast 替代 alert / 骨架屏 / 焦点陷阱 / 行内校验 / A11y
 */

const API = "";
let token = localStorage.getItem("xirang_token");
let currentUser = null;
let currentPortfolio = "us";
let navChart = null, drawdownChart = null, weightsChart = null;
let currentAlphaHandleRequestId = null;

// ── Toast 通知系统 ─────────────────────────────────

function showToast(message, type) {
  if (!type) type = "info";
  const container = document.getElementById("toast-container");
  if (!container) return;
  const toast = document.createElement("div");
  toast.className = "toast toast-" + type;
  toast.setAttribute("role", "alert");
  toast.textContent = message;
  container.appendChild(toast);
  requestAnimationFrame(function() {
    toast.classList.add("show");
  });
  setTimeout(function() {
    toast.classList.remove("show");
    setTimeout(function() { toast.remove(); }, 300);
  }, 3500);
}

// ── 焦点陷阱 ──────────────────────────────────────

let _trapCleanup = null;

function trapFocus(el) {
  const focusable = el.querySelectorAll('button:not([disabled]):not([hidden]), input:not([disabled]), textarea:not([disabled]), select:not([disabled]), [tabindex]:not([tabindex="-1"])');
  if (!focusable.length) return;
  const first = focusable[0];
  const last = focusable[focusable.length - 1];
  first.focus();

  function handler(e) {
    if (e.key !== "Tab") return;
    if (e.shiftKey) {
      if (document.activeElement === first) { e.preventDefault(); last.focus(); }
    } else {
      if (document.activeElement === last) { e.preventDefault(); first.focus(); }
    }
  }
  function escHandler(e) {
    if (e.key === "Escape") {
      closeModal();
      document.getElementById("wisdom-overlay").classList.remove("open");
    }
  }
  el.addEventListener("keydown", handler);
  el.addEventListener("keydown", escHandler);
  _trapCleanup = function() {
    el.removeEventListener("keydown", handler);
    el.removeEventListener("keydown", escHandler);
  };
}

function releaseFocus() {
  if (_trapCleanup) { _trapCleanup(); _trapCleanup = null; }
}

// ── 工具函数 ───────────────────────────────────────

function formatNum(n) {
  if (n == null) return "--";
  return Number(n).toLocaleString("en-US", { maximumFractionDigits: 2 });
}

function esc(str) {
  if (str == null) return "";
  const d = document.createElement("div");
  d.textContent = String(str);
  return d.innerHTML;
}

function isAuthed() {
  return !!(token && currentUser);
}

function withPortfolio(path) {
  var sep = path.indexOf("?") === -1 ? "?" : "&";
  return path + sep + "portfolio_id=" + encodeURIComponent(currentPortfolio);
}

function getActiveTabName() {
  var activeMain = document.querySelector(".tab.active");
  if (activeMain) return activeMain.dataset.tab;
  var activeDropdown = document.querySelector(".tab-dropdown-item.active");
  return activeDropdown ? activeDropdown.dataset.tab : null;
}

// ── 骨架屏辅助 ─────────────────────────────────────

function showSkeleton(id) {
  const el = document.getElementById(id);
  if (el) el.style.display = "";
}
function hideSkeleton(id, canvasId) {
  const el = document.getElementById(id);
  if (el) el.style.display = "none";
  if (canvasId) {
    const c = document.getElementById(canvasId);
    if (c) c.style.display = "";
  }
}

// ── 策略展示辅助 ───────────────────────────────────

const STRATEGY_ICONS = {
  covered_call: "📋",
  grid_trading: "📊",
  momentum_rotation: "🔄",
};

function getStrategyDisplay(strategyId) {
  const lang = LANGS[currentLang] || LANGS.zh;
  const s = (lang.strategies && lang.strategies[strategyId]) || {};
  return {
    incomeLabel: s.incomeLabel || t("defaultIncomeLabel"),
    icon: STRATEGY_ICONS[strategyId] || "📈",
    actionLabels: s.actionLabels || {},
  };
}

function translateAction(strategyId, action) {
  var display = getStrategyDisplay(strategyId);
  return display.actionLabels[action] || action;
}

// ── 空状态渲染 ─────────────────────────────────────

function emptyStateHTML(icon, textKey, hintKey) {
  return '<div class="empty-state"><div class="empty-state-icon">' + icon + '</div><div class="empty-state-text">' + esc(t(textKey)) + '</div>' + (hintKey ? '<div class="empty-state-hint">' + esc(t(hintKey)) + '</div>' : '') + '</div>';
}

function emptyRowHTML(colspan, textKey) {
  return '<tr><td colspan="' + colspan + '"><div class="empty-state" style="padding:24px 0"><div class="empty-state-icon">📭</div><div class="empty-state-text">' + esc(t(textKey)) + '</div></div></td></tr>';
}

// ── API 请求封装 ───────────────────────────────────

async function api(path, opts) {
  if (!opts) opts = {};
  var headers = { "Content-Type": "application/json" };
  if (token) headers["Authorization"] = "Bearer " + token;
  var res = await fetch(API + path, Object.assign({}, opts, { headers: headers }));
  if (res.status === 401) { logout(); throw new Error(t("unauthorized")); }
  if (!res.ok) {
    var err = await res.json().catch(function() { return {}; });
    throw new Error(err.detail || res.statusText);
  }
  return res.json();
}

// ── 页面路由 ───────────────────────────────────────

function showPage(id) {
  document.querySelectorAll(".page").forEach(function(p) { p.classList.remove("visible"); });
  document.getElementById(id).classList.add("visible");
  closeModal();
}

// ── 登录/登出 ──────────────────────────────────────

document.getElementById("login-form").addEventListener("submit", async function(e) {
  e.preventDefault();
  var errBox = document.getElementById("login-error");
  var errText = document.getElementById("login-error-text");
  errBox.classList.add("hidden");

  // inline validation
  var userInput = document.getElementById("login-user");
  var passInput = document.getElementById("login-pass");
  if (!userInput.value.trim()) {
    userInput.classList.add("border-red-500"); userInput.focus(); return;
  } else { userInput.classList.remove("border-red-500"); }
  if (!passInput.value) {
    passInput.classList.add("border-red-500"); passInput.focus(); return;
  } else { passInput.classList.remove("border-red-500"); }

  var btn = e.target.querySelector('button[type="submit"]');
  btn.disabled = true;
  try {
    var data = await api("/api/auth/login", {
      method: "POST",
      body: JSON.stringify({ username: userInput.value, password: passInput.value }),
    });
    token = data.access_token;
    localStorage.setItem("xirang_token", token);
    currentUser = null;
    await enterApp();
  } catch (err) {
    errText.textContent = err.message;
    errBox.classList.remove("hidden");
  } finally {
    btn.disabled = false;
  }
});

// inline validation on blur
["login-user", "login-pass"].forEach(function(id) {
  document.getElementById(id).addEventListener("blur", function() {
    if (!this.value.trim()) {
      this.classList.add("border-red-500");
      this.classList.remove("border-green-500");
    } else {
      this.classList.remove("border-red-500");
      this.classList.add("border-green-500");
    }
  });
  document.getElementById(id).addEventListener("input", function() {
    if (this.value.trim()) {
      this.classList.remove("border-red-500");
      this.classList.add("border-green-500");
    }
  });
});

function logout() {
  token = null;
  currentUser = null;
  localStorage.removeItem("xirang_token");
  closeModal();
  showPage("login-page");
}
document.getElementById("btn-logout").addEventListener("click", logout);

// ── 进入主界面 ─────────────────────────────────────

async function enterApp() {
  if (!currentUser) {
    try {
      var me = await api("/api/auth/me");
      currentUser = { username: me.username, role: me.role, display_name: me.display_name };
    } catch (e) {
      logout();
      return;
    }
  }

  showPage("app");
  document.getElementById("user-display").textContent = currentUser.display_name;

  var canWithdraw = ["admin", "member"].indexOf(currentUser.role) !== -1;
  document.getElementById("btn-new-withdraw").hidden = !canWithdraw;

  var withdrawTab = document.querySelector('[data-tab="withdrawals"]');
  if (withdrawTab) withdrawTab.style.display = canWithdraw ? "" : "none";

  try {
    var portfolios = await api("/api/portfolios");
    var sel = document.getElementById("portfolio-select");
    sel.innerHTML = "";
    portfolios.forEach(function(p) {
      var opt = document.createElement("option");
      opt.value = p.id;
      opt.textContent = p.name + " (" + p.currency + formatNum(p.nav) + ")";
      sel.appendChild(opt);
    });
    sel.value = currentPortfolio;
  } catch (e) { /* ignore */ }

  applyI18n();
  await showWisdomReminder();
  loadDashboard();
  if (canWithdraw) loadWithdrawals();
}

function refreshUI() {
  var tab = getActiveTabName();
  if (!tab) return;
  if (tab === "dashboard") loadDashboard();
  else if (tab === "transactions") loadTransactions();
  else if (tab === "withdrawals") loadWithdrawals();
  else if (tab === "alpha") loadAlpha();
  else if (tab === "report") loadReport();
  else if (tab === "datastatus") loadDataStatus();
}

// ── 选项卡切换 ─────────────────────────────────────

function activateTab(tabName) {
  document.querySelectorAll(".tab").forEach(function(t) {
    t.classList.remove("active");
    t.setAttribute("aria-selected", "false");
  });
  document.querySelectorAll(".tab-dropdown-item").forEach(function(t) {
    t.classList.remove("active");
  });
  document.querySelectorAll(".tab-content").forEach(function(c) { c.classList.remove("active"); });

  // Check if it's a main tab or dropdown item
  var mainTab = document.querySelector('.tab[data-tab="' + tabName + '"]');
  var dropdownItem = document.querySelector('.tab-dropdown-item[data-tab="' + tabName + '"]');
  if (mainTab) {
    mainTab.classList.add("active");
    mainTab.setAttribute("aria-selected", "true");
  }
  if (dropdownItem) {
    dropdownItem.classList.add("active");
  }

  document.getElementById("tab-" + tabName).classList.add("active");

  if (tabName === "transactions") loadTransactions();
  if (tabName === "withdrawals") loadWithdrawals();
  if (tabName === "report") loadReport();
  if (tabName === "alpha") loadAlpha();
  if (tabName === "datastatus") loadDataStatus();
}

document.querySelectorAll(".tab").forEach(function(tab) {
  tab.addEventListener("click", function() {
    activateTab(tab.dataset.tab);
  });
});

// "More" dropdown
(function() {
  var moreBtn = document.getElementById("btn-tab-more");
  var dropdown = document.getElementById("tab-more-dropdown");
  if (!moreBtn || !dropdown) return;

  moreBtn.addEventListener("click", function(e) {
    e.stopPropagation();
    var isOpen = dropdown.classList.contains("open");
    dropdown.classList.toggle("open", !isOpen);
    moreBtn.setAttribute("aria-expanded", !isOpen ? "true" : "false");
  });

  dropdown.addEventListener("click", function(e) {
    e.stopPropagation();
  });

  dropdown.querySelectorAll(".tab-dropdown-item").forEach(function(item) {
    item.addEventListener("click", function() {
      dropdown.classList.remove("open");
      moreBtn.setAttribute("aria-expanded", "false");
      activateTab(item.dataset.tab);
    });
  });

  // Close dropdown on outside click
  document.addEventListener("click", function() {
    dropdown.classList.remove("open");
    moreBtn.setAttribute("aria-expanded", "false");
  });
})();

document.getElementById("portfolio-select").addEventListener("change", function(e) {
  currentPortfolio = e.target.value;
  refreshUI();
});

// ── 仪表盘 ─────────────────────────────────────────

async function loadDashboard() {
  if (!isAuthed()) return;
  try {
    var d = await api("/api/dashboard/" + currentPortfolio + "?days=90");

    document.getElementById("stat-nav").textContent = d.currency + formatNum(d.current_nav);

    var stateEl = document.getElementById("stat-state");
    stateEl.innerHTML = '<span class="badge badge-' + esc(d.state.toLowerCase()) + '">' + esc(d.state) + '</span>';

    var lastDD = d.drawdown_series.length ? d.drawdown_series[d.drawdown_series.length - 1].drawdown : 0;
    var ddEl = document.getElementById("stat-drawdown");
    ddEl.textContent = (lastDD * 100).toFixed(1) + "%";
    ddEl.style.color = lastDD < -0.15 ? "#c08060" : "var(--text-dim)";

    renderNavChart(d.nav_series, d.currency);
    renderWeightsChart(d.current_weights);
    renderDrawdownChart(d.drawdown_series);
    renderRiskEvents(d.risk_events);
  } catch (err) {
    console.error("Dashboard load error:", err);
  }
}

function renderNavChart(series, currency) {
  hideSkeleton("chart-nav-skeleton", "chart-nav");
  var ctx = document.getElementById("chart-nav");
  if (navChart) navChart.destroy();
  navChart = new Chart(ctx, {
    type: "line",
    data: {
      labels: series.map(function(s) { return s.date; }),
      datasets: [{
        label: "NAV",
        data: series.map(function(s) { return s.nav; }),
        borderColor: "#7b7d9e",
        backgroundColor: "rgba(123,125,158,0.08)",
        fill: true, tension: 0.4,
        pointRadius: 0,
        borderWidth: 1.5,
      }],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { display: false },
        tooltip: { enabled: false },
      },
      scales: {
        x: { display: false },
        y: { display: false },
      },
    },
  });
}

function renderWeightsChart(weights) {
  hideSkeleton("chart-weights-skeleton", "chart-weights");
  var ctx = document.getElementById("chart-weights");
  if (weightsChart) weightsChart.destroy();
  var labels = Object.keys(weights);
  if (!labels.length) return;
  var data = labels.map(function(k) { return weights[k].weight * 100; });
  var names = labels.map(function(k) { return weights[k].name; });
  weightsChart = new Chart(ctx, {
    type: "doughnut",
    data: {
      labels: names,
      datasets: [{ data: data, backgroundColor: ["#7b7d9e", "#5a9a6e", "#b0a060", "#6b6d77"], borderWidth: 0 }],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { position: "bottom", labels: { color: "#8b8d97", padding: 8, font: { size: 10 } } },
        tooltip: { callbacks: { label: function(c) { return c.label + ": " + c.parsed.toFixed(0) + "%"; } } },
      },
    },
  });
}

function renderDrawdownChart(series) {
  hideSkeleton("chart-dd-skeleton", "chart-drawdown");
  var ctx = document.getElementById("chart-drawdown");
  if (drawdownChart) drawdownChart.destroy();
  drawdownChart = new Chart(ctx, {
    type: "line",
    data: {
      labels: series.map(function(s) { return s.date; }),
      datasets: [{
        label: t("drawdownLabel"),
        data: series.map(function(s) { return s.drawdown * 100; }),
        borderColor: "#8b6b6b",
        backgroundColor: "rgba(139,107,107,0.08)",
        fill: true, tension: 0.4,
        pointRadius: 0,
        borderWidth: 1.5,
      }],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: { legend: { display: false }, tooltip: { enabled: false } },
      scales: {
        x: { display: false },
        y: { display: false },
      },
    },
  });
}

function renderRiskEvents(events) {
  var el = document.getElementById("risk-events-list");
  if (!events.length) {
    el.innerHTML = '<div class="empty-state" style="padding:16px 0"><div class="empty-state-icon" style="font-size:1.5rem">✅</div><div class="empty-state-text" style="font-size:0.85rem">' + esc(t("noRiskEvents")) + '</div></div>';
    return;
  }
  el.innerHTML = events.map(function(e) {
    return '<div class="event-item"><span class="severity-' + esc(e.severity) + '">' + esc(e.event_type) + '</span> <span class="event-date">' + esc(e.date) + '</span><div style="font-size:0.8rem;color:var(--text-dim)">' + esc(e.action_taken) + '</div></div>';
  }).join("");
}

// ── 资金管理（入金 + 出金）───────────────────────

// 子选项卡切换
document.querySelectorAll(".funds-subtab").forEach(function(tab) {
  tab.addEventListener("click", function() {
    document.querySelectorAll(".funds-subtab").forEach(function(t) {
      t.classList.remove("active");
      t.setAttribute("aria-selected", "false");
    });
    document.querySelectorAll(".funds-panel").forEach(function(p) { p.classList.remove("active"); });
    tab.classList.add("active");
    tab.setAttribute("aria-selected", "true");
    var panel = document.getElementById("ftab-" + tab.dataset.ftab);
    if (panel) panel.classList.add("active");
    if (tab.dataset.ftab === "deposit") loadLayerStatus();
    if (tab.dataset.ftab === "history") loadDepositHistory();
  });
});

// ── 三层状态 ──

async function loadLayerStatus() {
  if (!isAuthed()) return;
  var bar = document.getElementById("layer-status-bar");
  try {
    var d = await api("/api/governance/layers/" + currentPortfolio);
    var core = d.core || {};
    var stab = d.stability || {};
    bar.innerHTML =
      '<div class="layer-card layer-core">' +
        '<div class="layer-card-label">' + esc(t("layerCore")) + '</div>' +
        '<div class="layer-card-value">$' + formatNum(core.balance) + '</div>' +
        '<div class="layer-card-ratio">' + esc(t("layerRatio")) + ' ' + ((core.ratio || 0) * 100).toFixed(1) + '% / ' + ((core.target || 0) * 100).toFixed(0) + '%</div>' +
      '</div>' +
      '<div class="layer-card layer-stability">' +
        '<div class="layer-card-label">' + esc(t("layerStability")) + '</div>' +
        '<div class="layer-card-value">$' + formatNum(stab.balance) + '</div>' +
        '<div class="layer-card-ratio">' + esc(t("layerRatio")) + ' ' + ((stab.ratio || 0) * 100).toFixed(1) + '% / ' + ((stab.target || 0) * 100).toFixed(0) + '%</div>' +
      '</div>';
  } catch (e) {
    bar.innerHTML = '';
  }
}

// ── 入金预览 + 确认 ──

var _depositPreviewData = null;

document.getElementById("deposit-form").addEventListener("submit", async function(e) {
  e.preventDefault();
  if (!isAuthed()) return;
  var amountEl = document.getElementById("d-amount");
  if (!amountEl.value || parseFloat(amountEl.value) <= 0) {
    amountEl.classList.add("input-error");
    return;
  }
  amountEl.classList.remove("input-error");

  var btn = document.getElementById("btn-deposit-preview");
  btn.disabled = true;
  try {
    var result = await api("/api/governance/deposit/preview", {
      method: "POST",
      body: JSON.stringify({
        amount: parseFloat(amountEl.value),
        portfolio_id: currentPortfolio,
      }),
    });
    _depositPreviewData = result;
    renderDepositPreview(result);
    document.getElementById("deposit-preview-box").hidden = false;
    document.getElementById("deposit-form-box").style.display = "none";
  } catch (err) {
    showToast(t("depositFail") + ": " + err.message, "error");
  } finally {
    btn.disabled = false;
  }
});

function renderDepositPreview(d) {
  var html = '';

  // ── 流程时间线 ──
  html += '<div class="deposit-timeline">';
  html += '<div class="timeline-phase">';
  html += '<div class="timeline-dot now"></div>';
  html += '<div class="timeline-label">' + esc(t("depositNow")) + '</div>';
  html += '</div>';
  html += '<div class="timeline-line"></div>';
  html += '<div class="timeline-phase">';
  html += '<div class="timeline-dot next"></div>';
  html += '<div class="timeline-label">' + esc(t("depositNextCycle")) + '</div>';
  html += '</div>';
  html += '</div>';

  // ── PROTECTION 模式特殊提示 ──
  if (d.protection_hold) {
    html += '<div class="deposit-protection-banner">';
    html += '<div class="protection-title">' + esc(t("depositProtectionModeTitle")) + '</div>';
    html += '<div class="protection-desc">' + esc(t("depositProtectionModeDesc")) + '</div>';
    html += '</div>';
  }

  // ── 第一阶段：立即到账 ──
  html += '<div class="alloc-phase">';
  html += '<div class="alloc-phase-header">';
  html += '<div class="phase-badge phase-now">1</div>';
  html += '<div class="phase-info"><div class="phase-title">' + esc(t("depositPhase1")) + '</div>';
  html += '<div class="phase-desc">' + esc(t("depositPhase1Desc")) + '</div></div>';
  html += '</div>';
  html += '<div class="alloc-phase-body">';
  html += '<div class="alloc-row total">';
  html += '<span class="alloc-label">' + esc(t("depositTotalDeposit")) + '</span>';
  html += '<span class="alloc-value">$' + formatNum(d.amount || d.step1_to_stability) + '</span>';
  html += '</div>';
  html += '<div class="alloc-row">';
  html += '<span class="alloc-label flow-arrow">' + esc(t("depositAllToStability")) + '</span>';
  html += '<span class="alloc-value stability-color">$' + formatNum(d.step1_to_stability) + '</span>';
  html += '</div>';
  html += '</div></div>';

  // ── 第二阶段：下一周期自动分配 ──
  if (!d.protection_hold && d.step2_to_core > 0) {
    html += '<div class="alloc-phase">';
    html += '<div class="alloc-phase-header">';
    html += '<div class="phase-badge phase-next">2</div>';
    html += '<div class="phase-info"><div class="phase-title">' + esc(t("depositPhase2")) + '</div>';
    html += '<div class="phase-desc">' + esc(t("depositPhase2Desc")) + '</div></div>';
    html += '</div>';
    html += '<div class="alloc-phase-body">';

    // 分配概览
    html += '<div class="alloc-split">';
    html += '<div class="alloc-split-item">';
    html += '<div class="alloc-split-label">' + esc(t("depositKeepStability")) + '</div>';
    html += '<div class="alloc-split-value stability-color">$' + formatNum(d.stay_in_stability) + '</div>';
    html += '</div>';
    html += '<div class="alloc-split-arrow">\u2192</div>';
    html += '<div class="alloc-split-item">';
    html += '<div class="alloc-split-label">' + esc(t("depositMoveToCore")) + '</div>';
    html += '<div class="alloc-split-value core-color">$' + formatNum(d.step2_to_core) + '</div>';
    html += '</div>';
    html += '</div>';

    // Core 明细
    if (d.core_allocation && Object.keys(d.core_allocation).length) {
      html += '<div class="core-alloc-detail">';
      html += '<div class="core-alloc-title">' + esc(t("depositCoreDetail")) + '</div>';
      html += '<div class="core-detail-grid">';
      for (var asset in d.core_allocation) {
        var info = d.core_allocation[asset];
        html += '<div class="core-detail-item">';
        html += '<div class="core-detail-name">' + esc(info.name || asset) + '</div>';
        html += '<div class="core-detail-add">+$' + formatNum(info.add) + '</div>';
        html += '</div>';
      }
      html += '</div></div>';
    }

    html += '<div class="alloc-auto-hint">';
    html += '<span class="hint-icon">\uD83E\uDD16</span>';
    html += '<span>' + esc(t("depositNextCycleAlloc")) + '</span>';
    html += '</div>';
    html += '</div></div>';
  } else if (!d.protection_hold) {
    html += '<div class="alloc-phase alloc-phase-muted">';
    html += '<div class="alloc-phase-header">';
    html += '<div class="phase-badge phase-next">2</div>';
    html += '<div class="phase-info"><div class="phase-title">' + esc(t("depositPhase2")) + '</div>';
    html += '<div class="phase-desc">' + esc(t("depositNormalNote")) + '</div></div>';
    html += '</div></div>';
  }

  // ── 前后对比 ──
  if (d.before && d.after) {
    html += '<div class="preview-compare">';
    html += '<div class="preview-compare-col">';
    html += '<h5>' + esc(t("depositBefore")) + '</h5>';
    html += '<div class="preview-compare-row"><span class="label">NAV</span><span class="value">$' + formatNum(d.before.nav) + '</span></div>';
    html += '<div class="preview-compare-row"><span class="label">Core</span><span class="value">$' + formatNum(d.before.core) + ' <small>(' + ((d.before.core_ratio || 0) * 100).toFixed(1) + '%)</small></span></div>';
    html += '<div class="preview-compare-row"><span class="label">Stability</span><span class="value">$' + formatNum(d.before.stability) + ' <small>(' + ((d.before.stability_ratio || 0) * 100).toFixed(1) + '%)</small></span></div>';
    html += '</div>';
    html += '<div class="preview-compare-col compare-after">';
    html += '<h5>' + esc(t("depositAfter")) + '</h5>';
    html += '<div class="preview-compare-row"><span class="label">NAV</span><span class="value highlight">$' + formatNum(d.after.nav) + '</span></div>';
    html += '<div class="preview-compare-row"><span class="label">Core</span><span class="value">$' + formatNum(d.after.core) + ' <small>(' + ((d.after.core_ratio || 0) * 100).toFixed(1) + '%)</small></span></div>';
    html += '<div class="preview-compare-row"><span class="label">Stability</span><span class="value">$' + formatNum(d.after.stability) + ' <small>(' + ((d.after.stability_ratio || 0) * 100).toFixed(1) + '%)</small></span></div>';
    html += '</div></div>';
  }

  // 确认提示
  html += '<div class="preview-note">' + esc(t("depositConfirmHint")) + '</div>';

  document.getElementById("deposit-preview-content").innerHTML = html;
}

document.getElementById("btn-deposit-confirm").addEventListener("click", async function() {
  if (!isAuthed() || !_depositPreviewData) return;
  var btn = this;
  btn.disabled = true;
  try {
    var result = await api("/api/governance/deposit", {
      method: "POST",
      body: JSON.stringify({
        amount: _depositPreviewData.amount,
        portfolio_id: _depositPreviewData.portfolio_id || currentPortfolio,
      }),
    });
    showToast(t("depositSuccess") + " $" + formatNum(result.new_nav), "success");
    _depositPreviewData = null;
    document.getElementById("deposit-preview-box").hidden = true;
    document.getElementById("deposit-form-box").style.display = "";
    document.getElementById("deposit-form").reset();
    loadLayerStatus();
    loadDashboard();
  } catch (err) {
    showToast(t("depositFail") + ": " + err.message, "error");
  } finally {
    btn.disabled = false;
  }
});

document.getElementById("btn-deposit-cancel").addEventListener("click", function() {
  _depositPreviewData = null;
  document.getElementById("deposit-preview-box").hidden = true;
  document.getElementById("deposit-form-box").style.display = "";
});

// ── 出金预览 + 提交 ──

var _withdrawPreviewData = null;

document.getElementById("btn-new-withdraw").addEventListener("click", function() {
  if (!isAuthed()) return;
  document.getElementById("withdraw-form-box").hidden = false;
  document.getElementById("withdraw-preview-box").hidden = true;
});
document.getElementById("btn-cancel-withdraw").addEventListener("click", function() {
  document.getElementById("withdraw-form-box").hidden = true;
  document.getElementById("withdraw-preview-box").hidden = true;
});

document.getElementById("btn-withdraw-preview").addEventListener("click", async function() {
  if (!isAuthed()) return;
  var amountEl = document.getElementById("w-amount");
  var reasonEl = document.getElementById("w-reason");
  var valid = true;
  if (!amountEl.value || parseFloat(amountEl.value) <= 0) {
    amountEl.classList.add("input-error"); valid = false;
  } else { amountEl.classList.remove("input-error"); }
  if (!reasonEl.value.trim()) {
    reasonEl.classList.add("input-error"); valid = false;
  } else { reasonEl.classList.remove("input-error"); }
  if (!valid) return;

  var btn = this;
  btn.disabled = true;
  try {
    var result = await api("/api/governance/withdraw/preview", {
      method: "POST",
      body: JSON.stringify({
        amount: parseFloat(amountEl.value),
        reason: reasonEl.value,
        portfolio_id: currentPortfolio,
      }),
    });
    _withdrawPreviewData = { amount: parseFloat(amountEl.value), reason: reasonEl.value, preview: result };
    renderWithdrawPreview(result);
    document.getElementById("withdraw-preview-box").hidden = false;
    document.getElementById("withdraw-form-box").hidden = true;
  } catch (err) {
    showToast(t("withdrawFail") + ": " + err.message, "error");
  } finally {
    btn.disabled = false;
  }
});

function renderWithdrawPreview(d) {
  var html = '';

  // ── 出金流程时间线 ──
  html += '<div class="deposit-timeline">';
  html += '<div class="timeline-phase">';
  html += '<div class="timeline-dot now"></div>';
  html += '<div class="timeline-label">' + esc(t("depositNow")) + '</div>';
  html += '</div>';
  html += '<div class="timeline-line"></div>';
  html += '<div class="timeline-phase">';
  html += '<div class="timeline-dot next"></div>';
  html += '<div class="timeline-label">' + esc(t("withdrawPhase2")) + '</div>';
  html += '</div>';
  html += '</div>';

  // ── 扣减方案 ──
  html += '<div class="alloc-phase">';
  html += '<div class="alloc-phase-header">';
  html += '<div class="phase-badge phase-now">1</div>';
  html += '<div class="phase-info"><div class="phase-title">' + esc(t("withdrawDeductPlan")) + '</div>';
  html += '<div class="phase-desc">' + esc(t("withdrawPhase1Desc")) + '</div></div>';
  html += '</div>';
  html += '<div class="alloc-phase-body">';

  // 扣减明细表格
  if (d.deductions && d.deductions.length) {
    html += '<table class="deduction-table"><thead><tr>';
    html += '<th>' + esc(t("withdrawDeductFrom")) + '</th>';
    html += '<th>' + esc(t("thAmount")) + '</th>';
    html += '<th>' + esc(t("withdrawFriction")) + '</th>';
    html += '</tr></thead><tbody>';
    d.deductions.forEach(function(dd) {
      var layerClass = dd.layer === "Stability" ? "stability" : "core";
      html += '<tr>';
      html += '<td><span class="layer-tag ' + layerClass + '">' + esc(dd.layer) + '</span> ' + esc(dd.name) + '</td>';
      html += '<td>$' + formatNum(dd.amount) + '</td>';
      html += '<td>' + (dd.friction > 0 ? '$' + formatNum(dd.friction) : '-') + '</td>';
      html += '</tr>';
    });
    html += '</tbody></table>';
  }

  // 总摩擦
  if (d.estimated_friction > 0) {
    html += '<div class="withdraw-friction-total">';
    html += '<span>' + esc(t("withdrawFriction")) + '</span>';
    html += '<span class="friction-value">$' + formatNum(d.estimated_friction) + '</span>';
    html += '</div>';
  }

  html += '</div></div>';

  // ── 审批阶段 ──
  html += '<div class="alloc-phase alloc-phase-muted">';
  html += '<div class="alloc-phase-header">';
  html += '<div class="phase-badge phase-next">2</div>';
  html += '<div class="phase-info"><div class="phase-title">' + esc(t("withdrawPhase2")) + '</div>';
  html += '<div class="phase-desc">' + esc(t("withdrawPhase2Desc")) + '</div></div>';
  html += '</div></div>';

  // ── 前后对比 ──
  if (d.before && d.after) {
    html += '<div class="preview-compare">';
    html += '<div class="preview-compare-col">';
    html += '<h5>' + esc(t("depositBefore")) + '</h5>';
    html += '<div class="preview-compare-row"><span class="label">NAV</span><span class="value">$' + formatNum(d.before.nav) + '</span></div>';
    html += '<div class="preview-compare-row"><span class="label">Core</span><span class="value">$' + formatNum(d.before.core) + '</span></div>';
    html += '<div class="preview-compare-row"><span class="label">Stability</span><span class="value">$' + formatNum(d.before.stability) + '</span></div>';
    html += '</div>';
    html += '<div class="preview-compare-col compare-after">';
    html += '<h5>' + esc(t("depositAfter")) + '</h5>';
    html += '<div class="preview-compare-row"><span class="label">NAV</span><span class="value highlight-red">$' + formatNum(d.after.nav) + '</span></div>';
    html += '<div class="preview-compare-row"><span class="label">Core</span><span class="value">$' + formatNum(d.after.core) + '</span></div>';
    html += '<div class="preview-compare-row"><span class="label">Stability</span><span class="value">$' + formatNum(d.after.stability) + '</span></div>';
    html += '</div></div>';
  }

  // 警告
  if (d.stability_warning) {
    html += '<div class="preview-note warning">' + esc(t("withdrawStabilityWarn")) + '</div>';
  }

  // 确认提示
  html += '<div class="preview-note">' + esc(t("withdrawConfirmHint")) + '</div>';

  document.getElementById("withdraw-preview-content").innerHTML = html;
}

document.getElementById("btn-withdraw-submit").addEventListener("click", async function() {
  if (!isAuthed() || !_withdrawPreviewData) return;
  var btn = this;
  btn.disabled = true;
  try {
    var result = await api("/api/governance/withdraw", {
      method: "POST",
      body: JSON.stringify({
        amount: _withdrawPreviewData.amount,
        reason: _withdrawPreviewData.reason,
        portfolio_id: currentPortfolio,
      }),
    });
    showToast(result.message, "success");
    _withdrawPreviewData = null;
    document.getElementById("withdraw-preview-box").hidden = true;
    document.getElementById("withdraw-form-box").hidden = true;
    document.getElementById("withdraw-form").reset();
    loadWithdrawals();
  } catch (err) {
    showToast(t("withdrawFail") + ": " + err.message, "error");
  } finally {
    btn.disabled = false;
  }
});

document.getElementById("btn-withdraw-preview-cancel").addEventListener("click", function() {
  _withdrawPreviewData = null;
  document.getElementById("withdraw-preview-box").hidden = true;
  document.getElementById("withdraw-form-box").hidden = false;
});

// ── 出金列表 ──

async function loadWithdrawals() {
  if (!isAuthed()) return;
  try {
    var list = await api("/api/governance/withdrawals");
    var tbody = document.getElementById("withdrawals-body");
    if (!list.length) {
      tbody.innerHTML = emptyRowHTML(7, "noWithdrawals");
      return;
    }
    var canApprove = ["admin", "member"].indexOf(currentUser.role) !== -1;
    tbody.innerHTML = list.map(function(w) {
      var approvals = (w.approvals || []).map(function(a) { return esc(a.approver) + ":" + esc(a.decision); }).join(", ") || "-";
      var isSelf = currentUser.username === w.requester;
      var showApprove = canApprove && w.status === "PENDING" && !isSelf;
      return '<tr><td>' + esc(w.id) + '</td><td>' + formatNum(w.amount) + '</td><td>' + esc(w.reason) + '</td><td>' + esc(w.requester) + '</td><td><span class="badge badge-' + esc(w.status.toLowerCase()) + '">' + esc(w.status) + '</span></td><td>' + approvals + '</td><td>' + (showApprove ? '<button class="btn-sm" data-wid="' + esc(w.id) + '" data-amount="' + w.amount + '" data-reason="' + esc(w.reason) + '" aria-label="Review withdrawal ' + esc(w.id) + '">' + esc(t("approveBtn")) + '</button>' : '') + '</td></tr>';
    }).join("");

    tbody.querySelectorAll("button[data-wid]").forEach(function(btn) {
      btn.addEventListener("click", function() {
        openApprovalModal(btn.dataset.wid, parseFloat(btn.dataset.amount), btn.dataset.reason);
      });
    });
  } catch (err) {
    console.error("Withdrawals load error:", err);
  }
}

// ── 入金记录 ──

async function loadDepositHistory() {
  if (!isAuthed()) return;
  try {
    var list = await api("/api/governance/deposits?portfolio_id=" + currentPortfolio);
    var tbody = document.getElementById("deposits-body");
    if (!list.length) {
      tbody.innerHTML = emptyRowHTML(5, "noDeposits");
      return;
    }
    tbody.innerHTML = list.map(function(d) {
      var alloc = "";
      try {
        var a = typeof d.allocation === "string" ? JSON.parse(d.allocation) : d.allocation;
        if (a) {
          var parts = [];
          if (a.Stability != null) parts.push("Stability: $" + formatNum(a.Stability));
          if (a["Core转入"] != null) parts.push("→Core: $" + formatNum(a["Core转入"]));
          alloc = parts.join(", ");
        }
      } catch (e) { alloc = esc(d.allocation || ""); }
      return '<tr><td>' + esc(d.id) + '</td><td>$' + formatNum(d.amount) + '</td><td>' + esc(d.depositor) + '</td><td style="font-size:0.8rem">' + alloc + '</td><td>' + esc(d.created_at || "") + '</td></tr>';
    }).join("");
  } catch (err) {
    console.error("Deposit history load error:", err);
  }
}

// ── 资金管理 tab 加载入口 ──

function loadFundsTab() {
  var canOperate = ["admin", "member"].indexOf(currentUser.role) !== -1;
  document.getElementById("btn-new-withdraw").hidden = !canOperate;
  loadLayerStatus();
  loadWithdrawals();
}

// ── 审批弹窗 ───────────────────────────────────────

var pendingApprovalId = null;
var _modalTrigger = null;

function openApprovalModal(id, amount, reason) {
  if (!isAuthed()) return;
  if (["admin", "member"].indexOf(currentUser.role) === -1) return;

  _modalTrigger = document.activeElement;
  pendingApprovalId = id;
  document.getElementById("modal-wid").textContent = "#" + id;
  document.getElementById("modal-detail").textContent = t("approvalDetail", { amount: formatNum(amount), reason: reason });
  document.getElementById("modal-comment").value = "";
  var overlay = document.getElementById("modal-overlay");
  overlay.classList.add("open");
  trapFocus(overlay.querySelector(".modal"));
}

function closeModal() {
  releaseFocus();
  document.getElementById("modal-overlay").classList.remove("open");
  pendingApprovalId = null;
  if (_modalTrigger) { _modalTrigger.focus(); _modalTrigger = null; }
}

document.getElementById("btn-modal-close").addEventListener("click", closeModal);
document.getElementById("modal-overlay").addEventListener("click", function(e) {
  if (e.target === e.currentTarget) closeModal();
});

async function submitApproval(decision) {
  if (!isAuthed() || !pendingApprovalId) return;
  try {
    var result = await api("/api/governance/withdraw/" + pendingApprovalId + "/approve", {
      method: "POST",
      body: JSON.stringify({
        decision: decision,
        comment: document.getElementById("modal-comment").value,
      }),
    });
    showToast(result.message, "success");
    closeModal();
    loadWithdrawals();
  } catch (err) {
    showToast(t("approvalFail") + ": " + err.message, "error");
  }
}

document.getElementById("btn-approve").addEventListener("click", function() { submitApproval("APPROVED"); });
document.getElementById("btn-reject").addEventListener("click", function() { submitApproval("REJECTED"); });

// ── 调仓历史 ───────────────────────────────────────

async function loadTransactions() {
  if (!isAuthed()) return;
  try {
    var list = await api("/api/transactions/" + currentPortfolio + "?limit=50");
    var tbody = document.getElementById("transactions-body");
    if (!list.length) {
      tbody.innerHTML = emptyRowHTML(5, "noTransactions");
      return;
    }
    tbody.innerHTML = list.map(function(tx) {
      return '<tr><td>' + esc(tx.date) + '</td><td>' + esc(tx.type) + '</td><td>' + (tx.turnover ? (tx.turnover * 100).toFixed(2) + "%" : "-") + '</td><td>' + (tx.friction_cost ? "$" + tx.friction_cost.toFixed(2) : "-") + '</td><td>' + esc(tx.reason) + '</td></tr>';
    }).join("");
  } catch (err) {
    console.error("Transactions load error:", err);
  }
}

// ── 达尔文沙盒 + 竞技场 ──────────────────────────────

async function loadAlpha() {
  if (!isAuthed()) return;
  try {
    var strategies = await api(withPortfolio("/api/alpha/strategies"));
    var ledger = await api(withPortfolio("/api/alpha/ledger"));
    var container = document.getElementById("alpha-strategies");
    var isAdmin = currentUser.role === "admin";

    renderAlphaLedger(ledger, isAdmin);
    loadAlphaLedgerEntries();
    document.getElementById("arena-controls").hidden = !isAdmin;
    loadAlphaWithdrawalRequests();

    if (!strategies.length) {
      container.innerHTML = emptyStateHTML("🧪", "noStrategies");
      return;
    }

    var txSelect = document.getElementById("alpha-tx-strategy");
    txSelect.innerHTML = strategies.map(function(s) {
      return '<option value="' + esc(s.id) + '">' + esc(s.name) + '</option>';
    }).join("");
    txSelect.onchange = function() { loadAlphaTransactions(txSelect.value); };

    container.innerHTML = strategies.map(function(s) {
      var enabled = s.status === "ENABLED";
      var suspended = s.status === "SUSPENDED";
      var badge = enabled ? "badge-approved" : suspended ? "badge-rejected" : "badge-expired";
      var statusText = enabled ? t("statusEnabled") : suspended ? t("statusSuspended") : t("statusDisabled");
      var toggleLabel = enabled ? t("toggleDisable") : t("toggleEnable");
      var toggleAction = enabled ? "disable" : "enable";
      var display = getStrategyDisplay(s.id);
      var sandboxOnly = !s.formal_reporting_eligible;
      var reportingBadge = sandboxOnly
        ? '<span class="badge badge-pending" style="margin-left:8px">' + esc(t("alphaSandboxOnly")) + '</span>'
        : "";
      var reportingNote = sandboxOnly
        ? '<div style="color:var(--text-dim);font-size:0.75rem;margin-top:6px">' + esc(s.reporting_note || t("alphaFormalExcluded")) + '</div>'
        : "";

      return '<div class="stat-card" style="margin-bottom:12px"><div style="display:flex;justify-content:space-between;align-items:center"><div><span style="font-size:1.1rem">' + display.icon + '</span> <strong>' + esc(s.name) + '</strong> <span class="badge ' + badge + '" style="margin-left:8px">' + esc(statusText) + '</span>' + reportingBadge + '</div><div style="display:flex;gap:8px">' + (isAdmin ? '<button class="btn-sm" data-sid="' + esc(s.id) + '" data-action="' + toggleAction + '" aria-label="' + esc(toggleLabel) + " " + esc(s.name) + '">' + esc(toggleLabel) + '</button>' : '') + (isAdmin && enabled ? '<button class="btn-primary" data-sid="' + esc(s.id) + '" data-run="1" aria-label="' + esc(t("manualRun")) + " " + esc(s.name) + '">' + esc(t("manualRun")) + '</button>' : '') + '</div></div><div style="color:var(--text-dim);font-size:0.8rem;margin-top:6px">' + esc(s.description) + '</div>' + reportingNote + '<div class="stats-row" style="margin-top:8px"><div class="stat-card" style="padding:6px 10px"><div class="stat-label">' + esc(t("labelAllocation")) + '</div><div class="stat-value" style="font-size:0.9rem">' + (s.allocation_pct * 100).toFixed(0) + '%</div></div><div class="stat-card" style="padding:6px 10px"><div class="stat-label">' + esc(t("labelCapital")) + '</div><div class="stat-value" style="font-size:0.9rem">' + formatNum(s.capital) + '</div></div><div class="stat-card" style="padding:6px 10px"><div class="stat-label">' + esc(display.incomeLabel) + '</div><div class="stat-value" style="font-size:0.9rem">' + formatNum(s.total_premium) + '</div></div><div class="stat-card" style="padding:6px 10px"><div class="stat-label">' + esc(t("labelPnl")) + '</div><div class="stat-value" style="font-size:0.9rem;color:var(--text-dim)">' + (s.total_pnl >= 0 ? "+" : "") + formatNum(s.total_pnl) + '</div></div><div class="stat-card" style="padding:6px 10px"><div class="stat-label">' + esc(t("labelTrades")) + '</div><div class="stat-value" style="font-size:0.9rem">' + s.trade_count + '</div></div></div></div>';
    }).join("");

    container.querySelectorAll("button[data-action]").forEach(function(btn) {
      btn.addEventListener("click", async function() {
        try {
          var result = await api(withPortfolio("/api/alpha/strategies/" + btn.dataset.sid + "/toggle"), {
            method: "POST",
            body: JSON.stringify({ action: btn.dataset.action }),
          });
          showToast(result.message, "success");
          loadAlpha();
        } catch (err) { showToast(err.message, "error"); }
      });
    });

    container.querySelectorAll("button[data-run]").forEach(function(btn) {
      btn.addEventListener("click", async function() {
        try {
          var result = await api(withPortfolio("/api/alpha/strategies/" + btn.dataset.sid + "/run"), { method: "POST" });
          showToast(t("manualRun") + " ✓", "success");
          loadAlpha();
          loadAlphaTransactions(btn.dataset.sid);
        } catch (err) { showToast(err.message, "error"); }
      });
    });

    loadArenaLeaderboard(strategies);
    if (strategies.length) {
      txSelect.value = strategies[0].id;
      loadAlphaTransactions(strategies[0].id);
    }
  } catch (err) {
    console.error("Alpha load error:", err);
  }
}

async function loadAlphaTransactions(strategyId) {
  if (!isAuthed()) return;
  try {
    var txs = await api(withPortfolio("/api/alpha/strategies/" + strategyId + "/transactions?limit=20"));
    var tbody = document.getElementById("alpha-tx-body");
    if (!txs.length) {
      tbody.innerHTML = emptyRowHTML(9, "noAlphaTx");
      return;
    }
    tbody.innerHTML = txs.map(function(tx) {
      var actionText = translateAction(strategyId, tx.action);
      return '<tr><td>' + esc(tx.date) + '</td><td>' + esc(actionText) + '</td><td>' + esc(tx.underlying || "-") + '</td><td>' + (tx.strike ? "$" + tx.strike.toFixed(2) : "-") + '</td><td>' + esc(tx.expiry || (tx.contracts ? tx.contracts + " " + t("shares") : "-")) + '</td><td>' + (tx.contracts || "-") + '</td><td>' + formatNum(tx.premium) + '</td><td style="color:var(--text-dim)">' + (tx.pnl >= 0 ? "+" : "") + formatNum(tx.pnl) + '</td><td style="font-size:0.75rem;color:var(--text-dim)">' + esc(tx.detail || "") + '</td></tr>';
    }).join("");
  } catch (err) {
    console.error("Alpha tx load error:", err);
  }
}

async function loadArenaLeaderboard(strategies) {
  if (!isAuthed()) return;
  try {
    var board = await api(withPortfolio("/api/alpha/arena/leaderboard"));
    var tbody = document.getElementById("arena-board-body");
    var eligibleCount = (strategies || []).filter(function(item) { return item.formal_reporting_eligible; }).length;
    if (!board.length) {
      tbody.innerHTML = emptyRowHTML(9, eligibleCount ? "noLeaderboard" : "alphaFormalEvalBlocked");
      if (!eligibleCount) {
        tbody.innerHTML += '<tr><td colspan="9"><div class="empty-state-hint">' + esc(t("alphaFormalOnlyLeaderboard")) + '</div></td></tr>';
      }
      return;
    }
    tbody.innerHTML = board.map(function(b) {
      var badge = b.status === "ENABLED" ? "badge-approved" : b.status === "SUSPENDED" ? "badge-rejected" : "badge-expired";
      return '<tr><td>' + b.rank + '</td><td>' + esc(b.name) + '</td><td><span class="badge ' + badge + '">' + esc(b.status) + '</span></td><td>' + b.sharpe.toFixed(2) + '</td><td>' + (b.annualized_return * 100).toFixed(1) + '%</td><td style="color:var(--text-dim)">' + (b.max_drawdown * 100).toFixed(1) + '%</td><td>' + (b.win_rate * 100).toFixed(0) + '%</td><td>' + b.trade_count + '</td><td style="color:var(--text-dim)">' + (b.total_pnl >= 0 ? "+" : "") + formatNum(b.total_pnl) + '</td></tr>';
    }).join("");
  } catch (err) {
    console.error("Leaderboard load error:", err);
  }
}

function renderAlphaLedger(ledger, isAdmin) {
  var summary = document.getElementById("alpha-ledger-summary");
  var withdrawBtn = document.getElementById("btn-alpha-withdraw-request");
  var manualInBtn = document.getElementById("btn-alpha-manual-in");
  var manualOutBtn = document.getElementById("btn-alpha-manual-out");
  withdrawBtn.hidden = !isAdmin;
  manualInBtn.hidden = !isAdmin;
  manualOutBtn.hidden = !isAdmin;

  summary.innerHTML =
    '<div class="stat-card"><div class="stat-label">' + esc(t("alphaCashBalance")) + '</div><div class="stat-value">$' + formatNum(ledger.cash_balance) + '</div></div>' +
    '<div class="stat-card"><div class="stat-label">' + esc(t("alphaInflows")) + '</div><div class="stat-value">$' + formatNum(ledger.total_inflows) + '</div></div>' +
    '<div class="stat-card"><div class="stat-label">' + esc(t("alphaOutflows")) + '</div><div class="stat-value">$' + formatNum(ledger.total_outflows) + '</div></div>' +
    '<div class="stat-card"><div class="stat-label">' + esc(t("alphaLastAdjust")) + '</div><div class="stat-value alpha-note-value">' + esc(ledger.last_manual_adjustment || "--") + '</div></div>';
}

async function loadAlphaLedgerEntries() {
  if (!isAuthed()) return;
  try {
    var list = await api(withPortfolio("/api/alpha/ledger/entries?limit=10"));
    var tbody = document.getElementById("alpha-ledger-entries-body");
    if (!list.length) {
      tbody.innerHTML = emptyRowHTML(6, "alphaNoLedgerEntries");
      return;
    }

    tbody.innerHTML = list.map(function(entry) {
      return '<tr>' +
        '<td>' + esc((entry.created_at || "").slice(0, 10) || "-") + '</td>' +
        '<td><span class="badge ' + (entry.direction === "IN" ? "badge-approved" : "badge-rejected") + '">' + esc(entry.direction === "IN" ? t("alphaDirectionIn") : t("alphaDirectionOut")) + '</span></td>' +
        '<td>' + (entry.direction === "IN" ? "+" : "-") + '$' + formatNum(entry.amount) + '</td>' +
        '<td>$' + formatNum(entry.balance_after) + '</td>' +
        '<td>' + esc(entry.note || "") + '</td>' +
        '<td>' + esc(entry.actor || "-") + '</td>' +
      '</tr>';
    }).join("");
  } catch (err) {
    console.error("Alpha ledger entries load error:", err);
  }
}

function alphaWithdrawalBadgeClass(status) {
  if (status === "PENDING_MANUAL") return "badge-pending";
  if (status === "HANDLED") return "badge-approved";
  if (status === "REJECTED") return "badge-rejected";
  return "badge-expired";
}

function alphaWithdrawalStatusText(status) {
  if (status === "PENDING_MANUAL") return t("alphaStatusPending");
  if (status === "HANDLED") return t("alphaStatusHandled");
  if (status === "REJECTED") return t("alphaStatusRejected");
  if (status === "CANCELLED") return t("alphaStatusCancelled");
  return status;
}

async function loadAlphaWithdrawalRequests() {
  if (!isAuthed()) return;
  try {
    var status = document.getElementById("alpha-withdraw-status-filter").value;
    var path = "/api/alpha/ledger/withdrawals?limit=50" + (status ? "&status=" + encodeURIComponent(status) : "");
    var list = await api(withPortfolio(path));
    var tbody = document.getElementById("alpha-withdrawals-body");
    var isAdmin = currentUser.role === "admin";

    if (!list.length) {
      tbody.innerHTML = emptyRowHTML(7, "alphaNoWithdrawRequests");
      return;
    }

    tbody.innerHTML = list.map(function(item) {
      var canHandle = isAdmin && item.status === "PENDING_MANUAL";
      return '<tr>' +
        '<td>' + esc(item.id) + '</td>' +
        '<td>$' + formatNum(item.amount) + '</td>' +
        '<td>' + esc(item.reason) + '</td>' +
        '<td>' + esc(item.requester) + '</td>' +
        '<td><span class="badge ' + alphaWithdrawalBadgeClass(item.status) + '">' + esc(alphaWithdrawalStatusText(item.status)) + '</span></td>' +
        '<td>' + esc((item.created_at || "").slice(0, 10) || "-") + '</td>' +
        '<td>' + (canHandle ? '<button class="btn-sm" data-alpha-handle="' + esc(item.id) + '">' + esc(t("alphaHandleBtn")) + '</button>' : '<span class="text-dim">-</span>') + '</td>' +
      '</tr>';
    }).join("");

    tbody.querySelectorAll("button[data-alpha-handle]").forEach(function(btn) {
      btn.addEventListener("click", function() {
        openAlphaHandleForm(btn.dataset.alphaHandle);
      });
    });
  } catch (err) {
    console.error("Alpha withdrawal requests load error:", err);
  }
}

async function openAlphaHandleForm(requestId) {
  try {
    var request = await api(withPortfolio("/api/alpha/ledger/withdrawals/" + requestId));
    currentAlphaHandleRequestId = request.id;
    document.getElementById("alpha-handle-request-id").textContent = "#" + request.id;
    document.getElementById("alpha-handle-status").value = "HANDLED";
    document.getElementById("alpha-handle-note").value = request.handled_note || "";
    document.getElementById("alpha-handle-ref").value = request.external_reference || "";
    document.getElementById("alpha-withdraw-handle-box").hidden = false;
  } catch (err) {
    showToast(err.message, "error");
  }
}

function closeAlphaHandleForm() {
  currentAlphaHandleRequestId = null;
  document.getElementById("alpha-withdraw-handle-box").hidden = true;
  document.getElementById("alpha-handle-request-id").textContent = "";
  document.getElementById("alpha-withdraw-handle-form").reset();
  document.getElementById("alpha-handle-note").classList.remove("input-error");
}

// ── 家族月报 ───────────────────────────────────────

var PORTFOLIO_LABELS = { us: "美股组合", cn: "中国组合", _all: "合并报告" };
var PORTFOLIO_LABELS_EN = { us: "US Portfolio", cn: "China Portfolio", _all: "Combined" };

function pfLabel(pid) {
  return getLang() === "en" ? (PORTFOLIO_LABELS_EN[pid] || pid) : (PORTFOLIO_LABELS[pid] || pid);
}

var _reportList = [];

async function loadReport() {
  if (!isAuthed()) return;
  document.getElementById("report-portfolio-bar").style.display = "none";
  document.getElementById("report-frame").style.display = "none";
  var cal = document.getElementById("report-calendar");
  cal.style.display = "";
  cal.innerHTML = '<div class="empty-state" style="padding:32px 0"><div class="skeleton h-6 w-32 mx-auto mb-4" style="margin:0 auto"></div><div class="skeleton h-4 w-48 mx-auto" style="margin:0 auto"></div></div>';

  try {
    _reportList = await api("/api/reports");
    renderReportCalendar(_reportList);
  } catch (e) {
    console.error("loadReport error:", e);
    cal.innerHTML = emptyStateHTML("📋", "loadFailed");
  }
}

function renderReportCalendar(list) {
  var cal = document.getElementById("report-calendar");
  if (!list.length) {
    cal.innerHTML = emptyStateHTML("📋", "noReport", "noReportHint");
    return;
  }

  var byYear = {};
  for (var i = 0; i < list.length; i++) {
    var item = list[i];
    if (!byYear[item.year]) byYear[item.year] = [];
    byYear[item.year].push(item);
  }

  var MONTH_NAMES_ZH = ["1月","2月","3月","4月","5月","6月","7月","8月","9月","10月","11月","12月"];
  var MONTH_NAMES_EN = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"];
  var monthNames = getLang() === "en" ? MONTH_NAMES_EN : MONTH_NAMES_ZH;

  var html = "";
  var years = Object.keys(byYear).sort(function(a, b) { return b - a; });
  for (var yi = 0; yi < years.length; yi++) {
    var year = years[yi];
    var items = byYear[year];
    var monthSet = {};
    for (var j = 0; j < items.length; j++) monthSet[items[j].month] = items[j];

    html += '<div style="margin-bottom:20px">';
    html += '<div style="font-size:1rem;font-weight:600;margin-bottom:10px;color:var(--text)">' + year + '</div>';
    html += '<div style="display:grid;grid-template-columns:repeat(6,1fr);gap:8px">';

    for (var m = 1; m <= 12; m++) {
      var mi = monthSet[m];
      if (mi) {
        var pids = Object.keys(mi.portfolios).filter(function(k) { return k !== "_all"; });
        var count = pids.length;
        var tooltip = pids.map(function(p) { return pfLabel(p); }).join(", ");
        html += '<button class="report-month-btn has-report" onclick="openReportMonth(' + year + ',' + m + ')" title="' + esc(tooltip) + '" aria-label="' + monthNames[m-1] + ' ' + year + ' - ' + count + ' portfolios">';
        html += '<span class="month-name">' + monthNames[m - 1] + '</span>';
        html += '<span class="month-count">' + count + ' ' + (getLang() === "en" ? "portfolio" + (count > 1 ? "s" : "") : "个组合") + '</span>';
        html += '</button>';
      } else {
        html += '<div class="report-month-btn empty"><span class="month-name">' + monthNames[m - 1] + '</span></div>';
      }
    }
    html += '</div></div>';
  }
  cal.innerHTML = html;
}

function openReportMonth(year, month) {
  var item = _reportList.find(function(r) { return r.year === year && r.month === month; });
  if (!item) return;

  document.getElementById("report-calendar").style.display = "none";
  var bar = document.getElementById("report-portfolio-bar");
  bar.style.display = "flex";

  var MONTH_NAMES_ZH = ["1月","2月","3月","4月","5月","6月","7月","8月","9月","10月","11月","12月"];
  var MONTH_NAMES_EN = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"];
  var monthNames = getLang() === "en" ? MONTH_NAMES_EN : MONTH_NAMES_ZH;
  document.getElementById("report-month-label").textContent = year + " " + monthNames[month - 1];

  var tabs = document.getElementById("report-portfolio-tabs");
  var pids = Object.keys(item.portfolios).filter(function(k) { return k !== "_all"; });
  tabs.innerHTML = "";

  for (var i = 0; i < pids.length; i++) {
    (function(pid) {
      var btn = document.createElement("button");
      btn.className = "btn-tab";
      btn.textContent = pfLabel(pid);
      btn.addEventListener("click", function() {
        tabs.querySelectorAll(".btn-tab").forEach(function(b) { b.classList.remove("active"); });
        btn.classList.add("active");
        loadPortfolioReport(year, month, pid);
      });
      tabs.appendChild(btn);
    })(pids[i]);
  }

  if (tabs.firstChild) tabs.firstChild.click();
}

async function loadPortfolioReport(year, month, portfolioId) {
  var frame = document.getElementById("report-frame");
  frame.style.display = "";
  frame.srcdoc = '<html><body style="background:#0f1117;color:#888;padding:40px;font-family:sans-serif"><p>' + esc(t("reportLoading")) + '</p></body></html>';

  try {
    var res = await fetch(
      API + "/api/reports/" + year + "/" + month + "/" + portfolioId + "?lang=" + getLang(),
      { headers: { "Authorization": "Bearer " + token } }
    );
    if (res.ok) {
      frame.srcdoc = await res.text();
    } else {
      frame.srcdoc = '<html><body style="background:#0f1117;color:#888;padding:40px;font-family:sans-serif"><h2>' + esc(t("noReport")) + '</h2></body></html>';
    }
  } catch (e) {
    frame.srcdoc = '<html><body style="background:#0f1117;color:#888;padding:40px;font-family:sans-serif"><p>' + esc(t("loadFailed")) + '</p></body></html>';
  }
}

document.getElementById("btn-report-back").addEventListener("click", function() {
  document.getElementById("report-portfolio-bar").style.display = "none";
  document.getElementById("report-frame").style.display = "none";
  var cal = document.getElementById("report-calendar");
  cal.style.display = "";
  renderReportCalendar(_reportList);
});

document.getElementById("btn-gen-report").addEventListener("click", async function() {
  if (!isAuthed()) return;
  try {
    var result = await api("/api/report/generate?lang=" + getLang(), { method: "POST" });
    showToast(result.message || t("reportGenerated"), "success");
    loadReport();
  } catch (err) {
    showToast(t("reportGenFail") + ": " + err.message, "error");
  }
});

// ── 竞技场按钮 ─────────────────────────────────────

document.getElementById("btn-arena-run-all").addEventListener("click", async function() {
  if (!isAuthed()) return;
  try {
    var result = await api(withPortfolio("/api/alpha/arena/run-all"), { method: "POST" });
    showToast(t("ranStrategies", { n: result.strategies_run }), "success");
    loadAlpha();
  } catch (err) {
    showToast(t("runFail") + ": " + err.message, "error");
  }
});

document.getElementById("btn-arena-evaluate").addEventListener("click", async function() {
  if (!isAuthed()) return;
  try {
    var result = await api(withPortfolio("/api/alpha/arena/evaluate"), { method: "POST" });
    document.getElementById("eval-result").hidden = false;
    document.getElementById("eval-report").textContent = JSON.stringify(result, null, 2);
    showToast(t("evalResultTitle") + " ✓", "success");
    loadAlpha();
  } catch (err) {
    showToast(t("evalFail") + ": " + err.message, "error");
  }
});

// ── 数据状态监控 ───────────────────────────────────

var _dataTaskPollTimer = null;

function levelText(level) {
  var map = { fresh: "dataLevelFresh", stale: "dataLevelStale", outdated: "dataLevelOutdated", missing: "dataLevelMissing" };
  return t(map[level] || "dataLevelMissing");
}

function renderDataCard(item, labelField) {
  var level = item.level || "missing";
  var label = item[labelField];
  if (level === "missing") {
    return '<div class="data-card level-missing"><div class="data-card-title">' + esc(label) + ' <span class="data-badge level-missing">' + esc(levelText("missing")) + '</span></div><div class="data-card-meta"><span>' + esc(t("dataNoData")) + '</span></div></div>';
  }
  return '<div class="data-card level-' + esc(level) + '"><div class="data-card-title">' + esc(label) + ' <span class="data-badge level-' + esc(level) + '">' + esc(levelText(level)) + '</span></div><div class="data-card-meta"><span>' + formatNum(item.rows) + ' ' + esc(t("dataRowsCol")) + '</span><span>' + esc(item.start) + ' → ' + esc(item.end) + '</span><span>' + item.stale_days + ' ' + esc(t("dataStaleDayUnit")) + '</span></div></div>';
}

function renderTaskBox(task) {
  var box = document.getElementById("data-task-box");
  if (!task) {
    box.hidden = true;
    _stopTaskPoll();
    return;
  }

  box.hidden = false;
  var statusEl = document.getElementById("data-task-status");
  var bar = document.getElementById("data-task-bar");
  var lastLog = document.getElementById("data-task-lastlog");
  var logsEl = document.getElementById("data-task-logs");
  var dismissBtn = document.getElementById("btn-dismiss-task");

  var modeLabel = task.mode === "all" ? t("dataUpdateAll")
    : task.mode === "live" ? t("dataUpdateLive")
    : task.ticker || task.mode;

  if (task.status === "running") {
    statusEl.innerHTML = '<span class="task-label">⏳ ' + esc(t("dataUpdating")) + ' — ' + esc(modeLabel) + '</span>';
    bar.className = "data-task-bar running";
    bar.style.width = "60%";
    dismissBtn.hidden = true;
    _startTaskPoll();
  } else if (task.status === "done") {
    statusEl.innerHTML = '<span class="task-label" style="color:var(--green)">✓ ' + esc(t("dataUpdateDone")) + ' — ' + esc(modeLabel) + '</span>';
    bar.className = "data-task-bar done";
    dismissBtn.hidden = false;
    _stopTaskPoll();
  } else if (task.status === "error") {
    statusEl.innerHTML = '<span class="task-label" style="color:var(--red)">✗ ' + esc(t("dataUpdateError")) + ' — ' + esc(modeLabel) + '</span>';
    bar.className = "data-task-bar error";
    dismissBtn.hidden = false;
    _stopTaskPoll();
  }

  lastLog.textContent = task.last_log || "";
  if (!logsEl.hidden && task.logs) {
    logsEl.textContent = task.logs.join("\n");
    logsEl.scrollTop = logsEl.scrollHeight;
  }
}

function _startTaskPoll() {
  if (_dataTaskPollTimer) return;
  _dataTaskPollTimer = setInterval(async function() {
    try {
      var d = await api("/api/data/update/status");
      renderTaskBox(d.task);
      if (d.task && d.task.status !== "running") {
        _stopTaskPoll();
        loadDataStatus();
      }
    } catch (e) { /* ignore */ }
  }, 2000);
}

function _stopTaskPoll() {
  if (_dataTaskPollTimer) {
    clearInterval(_dataTaskPollTimer);
    _dataTaskPollTimer = null;
  }
}

async function triggerUpdate(mode, extra) {
  if (!extra) extra = {};
  if (!isAuthed()) return;
  try {
    var body = Object.assign({ mode: mode }, extra);
    var result = await api("/api/data/update", {
      method: "POST",
      body: JSON.stringify(body),
    });
    showToast(t("dataUpdateStarted"), "info");
    renderTaskBox(result.task);
  } catch (err) {
    if (err.message.indexOf("409") !== -1 || err.message.indexOf("运行中") !== -1) {
      showToast(t("dataUpdateBusy"), "error");
    } else {
      showToast(t("dataUpdateError") + ": " + err.message, "error");
    }
  }
}

async function loadDataStatus() {
  if (!isAuthed()) return;
  try {
    var d = await api("/api/data/status");

    document.getElementById("data-checked-at").textContent =
      t("dataCheckedAt") + ": " + d.checked_at;

    document.getElementById("data-markets").innerHTML =
      d.markets.map(function(m) { return renderDataCard(m, "name"); }).join("");

    document.getElementById("data-live").innerHTML =
      d.live.map(function(l) { return renderDataCard(l, "name"); }).join("");

    var tbody = document.getElementById("data-tickers-body");
    var isAdmin = currentUser && currentUser.role === "admin";
    if (!d.tickers.length) {
      tbody.innerHTML = emptyRowHTML(6, "dataNoData");
    } else {
      tbody.innerHTML = d.tickers.map(function(tk) {
        var level = tk.level || "missing";
        var needsUpdate = level !== "fresh";
        return '<tr><td class="ticker-name">' + esc(tk.ticker) + '</td><td>' + formatNum(tk.rows) + '</td><td class="range">' + esc(tk.start) + ' → ' + esc(tk.end) + '</td><td class="stale-days level-' + esc(level) + '">' + tk.stale_days + '</td><td><span class="data-badge level-' + esc(level) + '">' + esc(levelText(level)) + '</span></td><td>' + (isAdmin && needsUpdate ? '<button class="btn-update-ticker" data-ticker="' + esc(tk.ticker) + '" aria-label="' + esc(t("dataUpdateTicker")) + ' ' + esc(tk.ticker) + '">' + esc(t("dataUpdateTicker")) + '</button>' : '') + '</td></tr>';
      }).join("");

      tbody.querySelectorAll(".btn-update-ticker").forEach(function(btn) {
        btn.addEventListener("click", function() {
          btn.disabled = true;
          triggerUpdate("ticker", { ticker: btn.dataset.ticker });
        });
      });
    }

    renderTaskBox(d.task);

    var schedBox = document.getElementById("scheduler-status-box");
    var schedText = document.getElementById("scheduler-status-text");
    if (d.scheduler && d.scheduler.enabled) {
      schedBox.hidden = false;
      var sched = d.scheduler;
      var taskInfo = sched.current_task
        ? '<span style="color:var(--accent)">⏳ ' + esc(sched.current_task) + '</span> | '
        : '';
      schedText.innerHTML =
        '🤖 ' + esc(t("schedulerEnabled")) + ' | ' + taskInfo +
        esc(t("schedulerNextLive")) + ': <strong>' + esc(sched.next_live) + '</strong> | ' +
        esc(t("schedulerNextFull")) + ': <strong>' + esc(sched.next_full) + '</strong>';
    } else {
      schedBox.hidden = true;
    }
  } catch (err) {
    console.error("Data status load error:", err);
  }
}

document.getElementById("btn-update-all").addEventListener("click", function() { triggerUpdate("all"); });
document.getElementById("btn-update-live").addEventListener("click", function() { triggerUpdate("live"); });

document.getElementById("btn-toggle-logs").addEventListener("click", function() {
  var logsEl = document.getElementById("data-task-logs");
  var btn = document.getElementById("btn-toggle-logs");
  logsEl.hidden = !logsEl.hidden;
  btn.textContent = logsEl.hidden ? t("dataViewLogs") : t("dataHideLogs");
  if (!logsEl.hidden) {
    logsEl.scrollTop = logsEl.scrollHeight;
  }
});

document.getElementById("btn-dismiss-task").addEventListener("click", async function() {
  try {
    await api("/api/data/update/dismiss", { method: "POST" });
    document.getElementById("data-task-box").hidden = true;
  } catch (e) { /* ignore */ }
});

document.getElementById("btn-alpha-withdraw-request").addEventListener("click", function() {
  document.getElementById("alpha-withdraw-form-box").hidden = false;
});

document.getElementById("btn-alpha-manual-in").addEventListener("click", function() {
  document.getElementById("alpha-entry-direction").value = "IN";
  document.getElementById("alpha-ledger-entry-form-box").hidden = false;
});

document.getElementById("btn-alpha-manual-out").addEventListener("click", function() {
  document.getElementById("alpha-entry-direction").value = "OUT";
  document.getElementById("alpha-ledger-entry-form-box").hidden = false;
});

document.getElementById("btn-alpha-withdraw-cancel").addEventListener("click", function() {
  document.getElementById("alpha-withdraw-form-box").hidden = true;
  document.getElementById("alpha-withdraw-form").reset();
});

document.getElementById("btn-alpha-entry-cancel").addEventListener("click", function() {
  document.getElementById("alpha-ledger-entry-form-box").hidden = true;
  document.getElementById("alpha-ledger-entry-form").reset();
});

document.getElementById("alpha-ledger-entry-form").addEventListener("submit", async function(e) {
  e.preventDefault();
  if (!isAuthed()) return;

  var directionEl = document.getElementById("alpha-entry-direction");
  var amountEl = document.getElementById("alpha-entry-amount");
  var noteEl = document.getElementById("alpha-entry-note");
  var refEl = document.getElementById("alpha-entry-ref");
  var requestIdEl = document.getElementById("alpha-entry-request-id");

  if (!amountEl.value || parseFloat(amountEl.value) <= 0) {
    amountEl.classList.add("input-error");
    amountEl.focus();
    return;
  }
  if (!noteEl.value.trim()) {
    noteEl.classList.add("input-error");
    noteEl.focus();
    return;
  }

  amountEl.classList.remove("input-error");
  noteEl.classList.remove("input-error");

  try {
    var result = await api(withPortfolio("/api/alpha/ledger/entries"), {
      method: "POST",
      body: JSON.stringify({
        direction: directionEl.value,
        amount: parseFloat(amountEl.value),
        note: noteEl.value.trim(),
        external_reference: refEl.value.trim(),
        related_request_id: requestIdEl.value.trim(),
      }),
    });
    showToast(t("alphaEntrySuccess"), "success");
    document.getElementById("alpha-ledger-entry-form-box").hidden = true;
    document.getElementById("alpha-ledger-entry-form").reset();
    loadAlpha();
    loadAlphaLedgerEntries();
  } catch (err) {
    showToast(err.message, "error");
  }
});

document.getElementById("alpha-withdraw-form").addEventListener("submit", async function(e) {
  e.preventDefault();
  if (!isAuthed()) return;

  var amountEl = document.getElementById("alpha-w-amount");
  var reasonEl = document.getElementById("alpha-w-reason");
  if (!amountEl.value || parseFloat(amountEl.value) <= 0) {
    amountEl.classList.add("input-error");
    amountEl.focus();
    return;
  }
  if (!reasonEl.value.trim()) {
    reasonEl.classList.add("input-error");
    reasonEl.focus();
    return;
  }

  amountEl.classList.remove("input-error");
  reasonEl.classList.remove("input-error");

  try {
    var result = await api(withPortfolio("/api/alpha/ledger/withdraw"), {
      method: "POST",
      body: JSON.stringify({
        amount: parseFloat(amountEl.value),
        reason: reasonEl.value.trim(),
      }),
    });
    showToast(result.message, "success");
    document.getElementById("alpha-withdraw-form-box").hidden = true;
    document.getElementById("alpha-withdraw-form").reset();
    loadAlpha();
    loadAlphaLedgerEntries();
    loadAlphaWithdrawalRequests();
  } catch (err) {
    showToast(err.message, "error");
  }
});

document.getElementById("alpha-withdraw-status-filter").addEventListener("change", function() {
  loadAlphaWithdrawalRequests();
});

document.getElementById("btn-alpha-handle-cancel").addEventListener("click", function() {
  closeAlphaHandleForm();
});

document.getElementById("alpha-withdraw-handle-form").addEventListener("submit", async function(e) {
  e.preventDefault();
  if (!isAuthed() || !currentAlphaHandleRequestId) return;

  var noteEl = document.getElementById("alpha-handle-note");
  var refEl = document.getElementById("alpha-handle-ref");
  var statusEl = document.getElementById("alpha-handle-status");

  if (statusEl.value === "HANDLED" && !noteEl.value.trim() && !refEl.value.trim()) {
    noteEl.classList.add("input-error");
    noteEl.focus();
    return;
  }
  noteEl.classList.remove("input-error");

  try {
    var result = await api(withPortfolio("/api/alpha/ledger/withdrawals/" + currentAlphaHandleRequestId + "/status"), {
      method: "POST",
      body: JSON.stringify({
        status: statusEl.value,
        note: noteEl.value.trim(),
        external_reference: refEl.value.trim(),
      }),
    });
    showToast(t("alphaHandleSuccess"), "success");
    closeAlphaHandleForm();
    loadAlphaWithdrawalRequests();
    loadAlpha();
    loadAlphaLedgerEntries();
  } catch (err) {
    showToast(err.message, "error");
  }
});

// ── 初始化 ─────────────────────────────────────────

(async function init() {
  applyI18n();
  if (token) {
    try {
      await enterApp();
    } catch (e) {
      showPage("login-page");
    }
  } else {
    showPage("login-page");
  }
})();
