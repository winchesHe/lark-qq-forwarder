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
  let routingGroupId = null;
  let groupSignature = "";
  let pendingMessage = null;
  let sending = false;
  let actionBlocked = true;
  let sendState = {};
  let sendError = "";

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
      const previous = $("qq-send-target").value;
      $("qq-send-target").replaceChildren(...active.map(group => {
        const option = node("option", "", group.label + " · " + group.display_id);
        option.value = group.binding_id;
        return option;
      }));
      if (active.some(group => group.binding_id === previous)) $("qq-send-target").value = previous;
      targetSignature = signature;
    }
    $("qq-target-note").textContent = active.length
      ? "按住 ⌘ / Ctrl 可多选。目标绑定与飞书管理共用，转发规则独立保存。"
      : "暂无可用目标群。可以先保存监听源，再到「飞书管理」绑定目标群。";
    $("qq-save-rule").disabled = busy || !ready || !token;
    renderSending();
    renderGroups();
    $("qq-rule-count").textContent = data.rules.length + " 条规则";
    const nextRuleSignature = JSON.stringify([data.rules, data.groups, busy, ready]);
    if (nextRuleSignature === ruleSignature) return;
    ruleSignature = nextRuleSignature;
    $("qq-rule-list").replaceChildren();
    if (!data.rules.length) {
      $("qq-rule-list").append(node("p", "empty-state", "还没有监听源。先添加源 QQ 群，再为目标群分配发送来源。"));
    }
    data.rules.forEach(rule => {
      const row = node("article", "qq-rule-card");
      row.append(node("h3", "", rule.group_name));
      row.append(node("p", "panel-footnote", rule.sender ? "仅监听：" + rule.sender : "群内所有发送人"));
      row.append(node("p", "operation-detail", "转发至：" + rule.binding_ids.map(id => {
        const group = groups.get(id);
        if (!group) return "目标已移除";
        return group.label + (group.status === "active" ? "" : "（不可用）");
      }).join("、") + (rule.binding_ids.length ? "" : "尚未分配目标群")));
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

  function renderGroups() {
    const signature = JSON.stringify([data.rules, data.groups, busy, ready]);
    if (signature === groupSignature) return;
    groupSignature = signature;
    $("qq-group-list").replaceChildren();
    if (!data.groups.length) {
      $("qq-group-list").append(node("p", "empty-state", "暂无目标 QQ 群，请先在飞书管理中绑定。"));
    }
    data.groups.forEach(group => {
      const row = node("article", "qq-rule-card");
      row.append(node("h3", "", group.label + " · " + group.display_id));
      const sources = data.rules.filter(rule => rule.binding_ids.includes(group.binding_id));
      row.append(node("p", "operation-detail", sources.length ? sources.map(rule =>
        rule.group_name + (rule.sender ? " / " + rule.sender : "") + (rule.enabled ? "" : "（停用）")
      ).join("、") : "未分配发送来源"));
      const button = node("button", "button button-secondary", "管理发送来源");
      button.type = "button";
      button.disabled = busy || !ready || group.status !== "active";
      button.addEventListener("click", () => openRouting(group));
      row.append(button);
      $("qq-group-list").append(row);
    });
  }

  function openRouting(group) {
    routingGroupId = group.binding_id;
    $("qq-routing-summary").textContent = "目标群：" + group.label + "。清空勾选将停止向本群自动转发，不会删除监听源。";
    $("qq-routing-feedback").textContent = "";
    $("qq-routing-list").replaceChildren(...data.rules.map(rule => {
      const label = node("label", "routing-source-row");
      const checkbox = document.createElement("input");
      checkbox.type = "checkbox";
      checkbox.value = rule.id;
      checkbox.checked = rule.binding_ids.includes(group.binding_id);
      label.append(checkbox, node("span", "", rule.group_name + (rule.sender ? " / " + rule.sender : " / 全部发送人") + (rule.enabled ? "" : "（规则停用）")));
      return label;
    }));
    if (!data.rules.length) $("qq-routing-list").append(node("p", "empty-state", "请先添加监听源。"));
    $("qq-routing-dialog").showModal();
  }

  $("qq-routing-cancel").addEventListener("click", () => $("qq-routing-dialog").close());
  $("qq-routing-form").addEventListener("submit", async event => {
    event.preventDefault();
    if (busy) return;
    busy = true;
    revision += 1;
    $("qq-routing-save").disabled = true;
    try {
      data = await request("/api/qq/routing", {
        binding_id: routingGroupId,
        source_ids: Array.from($("qq-routing-list").querySelectorAll("input:checked"), input => input.value),
      });
      $("qq-routing-dialog").close();
      if (editingId) {
        const rule = data.rules.find(value => value.id === editingId);
        if (rule) Array.from($("qq-targets").options).forEach(option => {
          option.selected = rule.binding_ids.includes(option.value);
        });
      }
      $("qq-feedback").textContent = "发送来源已保存，自动转发将在通知采集接入后生效。";
    } catch (failure) {
      $("qq-routing-feedback").textContent = failure.message || "保存失败";
    } finally {
      busy = false;
      $("qq-routing-save").disabled = false;
      render();
    }
  });

  function applyStatus(status) {
    const operations = status.operations || {};
    actionBlocked = ["starting", "stopping"].includes((status.overall || {}).state)
      || Object.values(operations).some(operation => ["running", "cancelling", "binding"].includes(operation.state));
    sendState = operations.test || {};
  }

  function renderSending() {
    $("qq-send-button").disabled = !ready || !token || sending || actionBlocked || !$("qq-send-target").value;
    $("qq-send-target").disabled = sending;
    $("qq-send-text").disabled = sending;
    if (sending) {
      $("qq-send-badge").textContent = "提交中";
      $("qq-send-feedback").textContent = "正在提交发送请求，请勿重复发送。";
      return;
    }
    if (sendError) {
      $("qq-send-badge").textContent = "请核对";
      $("qq-send-feedback").textContent = sendError;
      return;
    }
    if (sendState.mode === "qq_message") {
      const labels = { running: "发送中", succeeded: "发送成功", failed: "发送失败", cancelled: "已取消" };
      $("qq-send-badge").textContent = labels[sendState.state] || "尚未发送";
      $("qq-send-feedback").textContent = sendState.failure_message || sendState.effect || "发送请求处理中，请勿重复发送。";
    }
  }

  $("qq-send-form").addEventListener("submit", event => {
    event.preventDefault();
    if (sending || actionBlocked || !ready) return;
    const text = $("qq-send-text").value;
    if (!text.trim() || new TextEncoder().encode(text).length > 3000) {
      sendError = "消息不能为空，且不能超过 3000 字节（约 1000 个汉字）。";
      renderSending();
      return;
    }
    const group = data.groups.find(value => value.binding_id === $("qq-send-target").value && value.status === "active");
    if (!group) return;
    pendingMessage = { binding_id: group.binding_id, text, confirmed: true };
    $("qq-send-summary").textContent = "目标群：" + group.label + " · " + group.display_id;
    $("qq-send-preview").textContent = text;
    $("qq-send-dialog").showModal();
  });
  $("qq-send-cancel").addEventListener("click", () => $("qq-send-dialog").close());
  $("qq-send-confirm").addEventListener("click", async () => {
    if (sending || !pendingMessage) return;
    sending = true;
    revision += 1;
    sendError = "";
    const payload = pendingMessage;
    pendingMessage = null;
    $("qq-send-dialog").close();
    renderSending();
    try {
      applyStatus(await request("/api/qq/send", payload));
    } catch (failure) {
      sendError = (failure.message || "发送结果无法确认") + "；请先核对目标群，再决定是否重试。";
    } finally {
      sending = false;
      renderSending();
    }
  });

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
    if (busy || sending) return;
    const startedRevision = revision;
    try {
      if (!token) token = (await request("/api/session")).control_token;
      const [result, status] = await Promise.all([request("/api/qq/sources"), request("/api/status")]);
      if (busy || startedRevision !== revision) return;
      data = result;
      applyStatus(status);
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
