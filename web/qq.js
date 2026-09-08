(function () {
  "use strict";

  const $ = (id) => document.getElementById(id);
  const tabs = [$("lark-tab"), $("qq-tab")];
  let token = "";
  let data = { rules: [], groups: [] };
  let editingId = null;
  let busy = false;
  let ready = false;
  let targetSignature = "";
  let ruleSignature = "";
  let revision = 0;

  function selectTab(index, focus) {
    tabs.forEach((tab, i) => {
      tab.setAttribute("aria-selected", String(i === index));
      tab.tabIndex = i === index ? 0 : -1;
      $(tab.getAttribute("aria-controls")).hidden = i !== index;
    });
    if (focus) tabs[index].focus();
    history.replaceState(null, "", index ? "#qq" : "#lark");
  }

  tabs.forEach((tab, index) => {
    tab.addEventListener("click", () => selectTab(index, false));
    tab.addEventListener("keydown", (event) => {
      if (!["ArrowLeft", "ArrowRight", "Home", "End"].includes(event.key)) return;
      event.preventDefault();
      let nextIndex = 1 - index;
      if (event.key === "Home") nextIndex = 0;
      if (event.key === "End") nextIndex = 1;
      selectTab(nextIndex, true);
    });
  });
  selectTab(location.hash === "#qq" ? 1 : 0, false);

  function node(tag, className, text) {
    const element = document.createElement(tag);
    element.className = className;
    if (text !== undefined) element.textContent = text;
    return element;
  }

  function error(message) {
    $("qq-error").textContent = message;
    $("qq-error").hidden = !message;
  }

  async function request(path, payload) {
    const options = { cache: "no-store" };
    if (payload !== undefined) Object.assign(options, {
      method: "POST",
      headers: { "X-Control-Token": token, "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    const response = await fetch(path, options);
    const result = await response.json();
    if (!response.ok || !result.ok) throw new Error(result.error || "QQ 配置请求失败");
    return result.data;
  }

  function render() {
    const groups = new Map(data.groups.map(group => [group.binding_id, group]));
    const active = data.groups.filter(group => group.status === "active");
    const signature = JSON.stringify(active);
    if (signature !== targetSignature) {
      const selected = new Set(Array.from($("qq-targets").selectedOptions, option => option.value));
      $("qq-targets").replaceChildren(...active.map(group => {
        const option = node("option", "", group.label + " · " + group.display_id);
        option.value = group.binding_id;
        option.selected = selected.has(group.binding_id);
        return option;
      }));
      targetSignature = signature;
    }
    $("qq-target-note").textContent = active.length
      ? "按住 ⌘ / Ctrl 可多选。目标绑定与飞书管理共用，转发规则独立保存。"
      : "暂无可用目标群，请先到「飞书管理」绑定目标 QQ 群，再返回这里。";
    $("qq-save-rule").disabled = busy || !ready || !token || !active.length;
    $("qq-rule-count").textContent = data.rules.length + " 条规则";
    const nextRuleSignature = JSON.stringify([data.rules, data.groups, busy, ready]);
    if (nextRuleSignature === ruleSignature) return;
    ruleSignature = nextRuleSignature;
    $("qq-rule-list").replaceChildren();
    if (!data.rules.length) {
      $("qq-rule-list").append(node("p", "empty-state", "还没有监听源。添加一个源 QQ 群，并选择转发目标。"));
    }
    data.rules.forEach(rule => {
      const row = node("article", "qq-rule-card");
      row.append(node("h3", "", rule.group_name));
      row.append(node("p", "panel-footnote", rule.sender ? "仅监听：" + rule.sender : "群内所有发送人"));
      row.append(node("p", "operation-detail", "转发至：" + rule.binding_ids.map(id => {
        const group = groups.get(id);
        if (!group) return "目标已移除";
        return group.label + (group.status === "active" ? "" : "（不可用）");
      }).join("、")));
      row.append(node("span", "status-badge", rule.enabled ? "规则启用 · 待接入采集" : "规则停用"));
      const actions = node("div", "operation-actions");
      [["编辑", () => edit(rule)], [rule.enabled ? "停用规则" : "启用规则", () => mutate("/api/qq/sources", Object.assign({}, rule, { enabled: !rule.enabled }))],
        ["删除", () => {
          if (window.confirm("删除「" + rule.group_name + "」的监听规则？")) {
            mutate("/api/qq/sources/remove", { id: rule.id });
          }
        }]].forEach(([label, handler]) => {
        const button = node("button", "text-button", label);
        button.type = "button";
        button.disabled = busy || !ready;
        button.addEventListener("click", handler);
        actions.append(button);
      });
      row.append(actions);
      $("qq-rule-list").append(row);
    });
  }

  function resetForm() {
    editingId = null;
    $("qq-rule-form").reset();
    Array.from($("qq-targets").options).forEach(option => { option.selected = false; });
    $("qq-form-title").textContent = "新增监听规则";
    $("qq-save-rule").textContent = "保存规则";
    $("qq-cancel-edit").hidden = true;
  }

  function edit(rule) {
    editingId = rule.id;
    $("qq-source-name").value = rule.group_name;
    $("qq-source-sender").value = rule.sender;
    $("qq-rule-enabled").checked = rule.enabled;
    Array.from($("qq-targets").options).forEach(option => {
      option.selected = rule.binding_ids.includes(option.value);
    });
    $("qq-form-title").textContent = "编辑监听规则";
    $("qq-save-rule").textContent = "保存修改";
    $("qq-cancel-edit").hidden = false;
    $("qq-feedback").textContent = "";
    $("qq-source-name").focus();
  }

  async function mutate(path, payload, resetOnSuccess = false) {
    if (busy) return;
    busy = true;
    revision += 1;
    error("");
    render();
    try {
      data = await request(path, payload);
      if (resetOnSuccess || (path.endsWith("/remove") && editingId === payload.id)) resetForm();
      $("qq-feedback").textContent = "配置已保存。QQ 通知采集尚未接入，当前不会转发消息。";
    } catch (failure) {
      error(failure.message || "保存失败，请稍后重试");
    } finally {
      busy = false;
      render();
    }
  }

  $("qq-rule-form").addEventListener("submit", event => {
    event.preventDefault();
    const payload = {
      group_name: $("qq-source-name").value.trim(),
      sender: $("qq-source-sender").value.trim(),
      binding_ids: Array.from($("qq-targets").selectedOptions, option => option.value),
      enabled: $("qq-rule-enabled").checked,
    };
    if (editingId) payload.id = editingId;
    mutate("/api/qq/sources", payload, true);
  });
  $("qq-cancel-edit").addEventListener("click", resetForm);

  async function refresh() {
    if (busy) return;
    const startedRevision = revision;
    try {
      if (!token) token = (await request("/api/session")).control_token;
      const result = await request("/api/qq/sources");
      if (busy || startedRevision !== revision) return;
      data = result;
      ready = true;
      error("");
      render();
    } catch (failure) {
      if (startedRevision !== revision) return;
      ready = false;
      render();
      error(failure.message || "无法读取 QQ 配置");
    }
  }
  refresh();
  const timer = window.setInterval(refresh, 5000);
  window.addEventListener("beforeunload", () => window.clearInterval(timer));
}());
