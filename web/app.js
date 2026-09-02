(function () {
  "use strict";

  let controlToken = "";
  let pollTimer = null;
  let busy = false;

  const elements = {
    sessionTime: document.querySelector("#session-time"),
    overallBadge: document.querySelector("#overall-badge"),
    statusOrb: document.querySelector("#status-orb"),
    overallLabel: document.querySelector("#overall-label"),
    overallDetail: document.querySelector("#overall-detail"),
    processList: document.querySelector("#process-list"),
    runtimeList: document.querySelector("#runtime-list"),
    eventsList: document.querySelector("#events-list"),
    recoveryList: document.querySelector("#recovery-list"),
    feedback: document.querySelector("#action-feedback"),
    globalError: document.querySelector("#global-error"),
    startButton: document.querySelector("#start-button"),
    stopButton: document.querySelector("#stop-button"),
    restartButton: document.querySelector("#restart-button"),
    checkButton: document.querySelector("#check-button"),
    recoveryCheckButton: document.querySelector("#recovery-check-button"),
    recoveryRestartButton: document.querySelector("#recovery-restart-button"),
    bindingBadge: document.querySelector("#binding-badge"),
    bindingTitle: document.querySelector("#binding-title-copy"),
    bindingDetail: document.querySelector("#binding-detail"),
    bindingGroupList: document.querySelector("#binding-group-list"),
    bindButton: document.querySelector("#bind-button"),
    addBindButton: document.querySelector("#add-bind-button"),
    rebindButton: document.querySelector("#rebind-button"),
    cancelBindButton: document.querySelector("#cancel-bind-button"),
    testBadge: document.querySelector("#test-badge"),
    testTitle: document.querySelector("#test-title-copy"),
    testDetail: document.querySelector("#test-detail"),
    testButton: document.querySelector("#test-button"),
    primeBadge: document.querySelector("#prime-badge"),
    primeTitle: document.querySelector("#prime-title-copy"),
    primeDetail: document.querySelector("#prime-detail"),
    primeButton: document.querySelector("#prime-button"),
    forceEndButton: document.querySelector("#force-end-button"),
    replayBadge: document.querySelector("#replay-badge"),
    replayTitle: document.querySelector("#replay-title-copy"),
    replayDetail: document.querySelector("#replay-detail"),
    channelSelect: document.querySelector("#channel-select"),
    replayButton: document.querySelector("#replay-button"),
    cancelReplayButton: document.querySelector("#cancel-replay-button"),
    replaySummary: document.querySelector("#replay-summary"),
    messageIds: document.querySelector("#message-ids"),
    previewReplayButton: document.querySelector("#preview-replay-button"),
    replayItems: document.querySelector("#replay-items"),
    replayDialog: document.querySelector("#replay-dialog"),
    replayDialogList: document.querySelector("#replay-dialog-list"),
    replayDialogSummary: document.querySelector("#replay-dialog-summary"),
    replayDialogClose: document.querySelector("#replay-dialog-close"),
    replayDialogSelectAll: document.querySelector("#replay-dialog-select-all"),
    replayDialogConfirm: document.querySelector("#replay-dialog-confirm"),
    listenerList: document.querySelector("#listener-list"),
    listenerName: document.querySelector("#listener-name"),
    listenerAddButton: document.querySelector("#listener-add-button"),
    listenerFeedback: document.querySelector("#listener-feedback"),
    listenerChannelList: document.querySelector("#listener-channel-list"),
  };

  const stateLabels = {
    starting: "启动中",
    running: "运行中",
    stopping: "停止中",
    stopped: "已停止",
    degraded: "部分运行",
    failed: "启动失败",
  };

  const bindingLabels = {
    unbound: "未绑定",
    bound: "已绑定",
    binding: "绑定中",
    cancelling: "取消中",
  };

  const operationLabels = {
    idle: "尚未执行",
    running: "进行中",
    cancelling: "取消中",
    succeeded: "成功",
    failed: "失败",
    cancelled: "已取消",
  };

  const eventLabels = {
    control_plane_ready: "控制面",
    start_requested: "启动",
    process_started: "进程",
    prime_completed: "检查",
    prime_requested: "prime",
    prime_succeeded: "prime 完成",
    prime_failed: "prime 失败",
    prime_cancelled: "prime 取消",
    start_completed: "启动完成",
    start_failed: "启动失败",
    stop_requested: "停止",
    stop_completed: "停止完成",
    stop_failed: "停止失败",
    process_exited: "进程退出",
    check_requested: "只读检查",
    check_passed: "检查通过",
    check_failed: "检查未通过",
    notification_wakeup: "通知唤醒",
    bind_requested: "绑定",
    bind_succeeded: "绑定完成",
    bind_failed: "绑定失败",
    bind_cancel_requested: "取消绑定",
    bind_cancelled: "绑定取消",
    bind_skipped: "绑定状态",
    test_requested: "主动测试",
    test_succeeded: "测试成功",
    test_failed: "测试失败",
    test_cancelled: "测试取消",
    replay_requested: "指定补发",
    replay_succeeded: "补发完成",
    replay_failed: "补发失败",
    replay_cancel_requested: "取消补发",
    replay_cancelled: "补发取消",
    listener_added: "监听人员",
  };

  function setText(node, value) {
    if (node) node.textContent = value == null ? "" : String(value);
  }

  function showError(message) {
    setText(elements.globalError, message);
    elements.globalError.hidden = !message;
  }

  function showFeedback(message, isError) {
    setText(elements.feedback, message);
    elements.feedback.classList.toggle("is-error", Boolean(isError));
  }

  function stateClass(state) {
    if (state === "running" || state === "succeeded" || state === "bound") return "is-running";
    if (state === "starting" || state === "stopping" || state === "degraded" || state === "binding" || state === "cancelling") return "is-starting";
    if (state === "failed") return "is-danger";
    return "";
  }

  function formatTime(value) {
    if (!value) return "未记录";
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) return "未记录";
    return new Intl.DateTimeFormat("zh-CN", {
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
    }).format(date);
  }

  function formatPid(pid) {
    return typeof pid === "number" ? `PID ${pid}` : "尚未运行";
  }

  function makeElement(tag, className, text) {
    const node = document.createElement(tag);
    if (className) node.className = className;
    if (text !== undefined) node.textContent = text;
    return node;
  }

  function operationsFrom(data) {
    const operations = data.operations || {};
    return {
      binding: data.binding || operations.binding || {},
      test: data.test || operations.test || {},
      prime: data.prime || operations.prime || {},
      replay: data.replay || operations.replay || {},
    };
  }

  function renderOverall(data) {
    const overall = data.overall || {};
    const state = overall.state || "stopped";
    const label = overall.label || stateLabels[state] || "未知";
    const className = stateClass(state);
    elements.overallBadge.className = `status-badge ${className}`.trim();
    setText(elements.overallBadge, label);
    elements.statusOrb.className = `status-orb ${className}`.trim();
    setText(elements.overallLabel, label);

    let detail = "两个本地进程都未运行。";
    if (state === "running") detail = "通知监听、Perfecto 和四个频道转发都在运行。";
    if (state === "starting") detail = "正在准备游标并启动本地进程。";
    if (state === "stopping") detail = "正在停止由本控制面启动的进程。";
    if (state === "degraded") detail = overall.failure_message || "只有部分进程在运行，请检查子进程状态。";
    if (state === "failed") detail = overall.failure_message || "启动流程未完成，请运行只读检查后重试。";
    setText(elements.overallDetail, detail);

    elements.startButton.disabled = busy || state === "running" || state === "starting" || state === "stopping";
    elements.stopButton.disabled = busy || state === "stopped" || state === "stopping";
    elements.restartButton.disabled = busy || state === "starting" || state === "stopping";
  }

  function renderProcesses(processes) {
    elements.processList.replaceChildren();
    if (!Array.isArray(processes) || processes.length === 0) {
      elements.processList.appendChild(makeElement("div", "empty-state", "暂时没有子进程状态。"));
      return;
    }
    processes.forEach(function (process) {
      const row = makeElement("div", "process-row");
      const name = makeElement("div", "process-name");
      const icon = makeElement("span", "process-icon", process.role === "forwarder" ? "转" : "听");
      icon.setAttribute("aria-hidden", "true");
      const copy = makeElement("div");
      copy.appendChild(makeElement("strong", "", process.label || "本地进程"));
      copy.appendChild(makeElement("small", "", `${formatPid(process.pid)} · ${formatTime(process.started_at)}`));
      name.appendChild(icon);
      name.appendChild(copy);
      const state = process.state || "stopped";
      const stateNode = makeElement("span", `process-state ${stateClass(state)}`, process.label_for_state || stateLabels[state] || "未知");
      row.appendChild(name);
      row.appendChild(stateNode);
      elements.processList.appendChild(row);
    });
  }

  function makeRuntimeRow(label, value, good) {
    const row = makeElement("div", "runtime-row");
    row.appendChild(makeElement("span", "", label));
    row.appendChild(makeElement("strong", `runtime-value ${good ? "is-good" : "is-bad"}`, value));
    return row;
  }

  function yesNo(value, yes, no) {
    return value ? yes : no;
  }

  function renderRuntime(data) {
    const runtime = data.runtime || {};
    const check = data.check || {};
    const channelCount = Number(runtime.channel_forwarding_count) || 0;
    elements.runtimeList.replaceChildren();
    const qqCount = Number(runtime.qq_group_count) || (runtime.qq_group_bound ? 1 : 0);
    elements.runtimeList.appendChild(makeRuntimeRow("QQ 群绑定", qqCount ? `${qqCount} 个已绑定` : "未绑定", qqCount > 0));
    elements.runtimeList.appendChild(makeRuntimeRow("飞书会话游标", yesNo(runtime.message_cursor_initialized, "已就绪", "需先 prime"), Boolean(runtime.message_cursor_initialized)));
    elements.runtimeList.appendChild(makeRuntimeRow("频道自动转发", channelCount ? `${channelCount} 个已配置` : "未配置", Boolean(runtime.channel_forwarding_available)));
    elements.runtimeList.appendChild(makeRuntimeRow("本地通知记录", yesNo(runtime.notification_log_available, "可读取", "尚未生成"), Boolean(runtime.notification_log_available)));
    let checkLabel = "尚未执行";
    let checkGood = false;
    if (check.state === "running") checkLabel = "检查中";
    if (check.state === "passed") {
      checkLabel = `通过 · ${formatTime(check.completed_at)}`;
      checkGood = true;
    }
    if (check.state === "failed") checkLabel = "未通过";
    elements.runtimeList.appendChild(makeRuntimeRow("最近只读检查", checkLabel, checkGood));
    elements.checkButton.disabled = busy || check.state === "running";
  }

  function renderOperationBadge(node, operation, fallbackState, labels) {
    const state = operation.state || fallbackState;
    node.className = `status-badge ${stateClass(state)}`.trim();
    setText(node, operation.label || labels[state] || "未知");
    return state;
  }

  function serviceMustBeStopped(overallState) {
    return overallState === "running" || overallState === "degraded" || overallState === "starting" || overallState === "stopping";
  }

  function renderBinding(operation, runtime, overallState) {
    const fallbackState = runtime.qq_group_bound ? "bound" : "unbound";
    const state = renderOperationBadge(elements.bindingBadge, operation, fallbackState, bindingLabels);
    let title = state === "bound" ? "目标 QQ 群已绑定" : "尚未绑定 QQ 群";
    let detail = "开始绑定后，在目标 QQ 群发送 @qclaw 绑定测试。";
    const blocked = serviceMustBeStopped(overallState);
    const busyState = state === "binding" || state === "cancelling";
    if (state === "binding") detail = "请在目标 QQ 群发送 @qclaw 绑定测试，绑定完成后会自动更新。";
    if (state === "cancelling") detail = "正在取消绑定，当前已有绑定不会被替换。";
    if (state === "bound") detail = `已保存 ${runtime.qq_group_count || 1} 个 QQ 群绑定；重新绑定会替换当前群，新增绑定会保留现有群。`;
    if (blocked && !busyState) detail = "请先停止转发服务，再新增或重新绑定 QQ 群。";
    if (operation.failure_message) detail = operation.failure_message;
    setText(elements.bindingTitle, title);
    setText(elements.bindingDetail, detail);

    if (elements.bindingGroupList) {
      elements.bindingGroupList.replaceChildren();
      const groups = Array.isArray(runtime.qq_groups) ? runtime.qq_groups : [];
      if (!groups.length) {
        elements.bindingGroupList.appendChild(makeElement("span", "panel-footnote", "暂无已加入的 QQ 群"));
      } else {
        groups.forEach(function (group, index) {
          const row = makeElement("div", "binding-group-row");
          const name = group.label || `QQ 群 ${index + 1}`;
          const suffix = group.display_id ? ` · 尾号 ${group.display_id}` : "";
          const stateText = group.status === "active" ? "投递中" : (group.status === "disabled" ? "已停用" : "需检查");
          const nameWrap = makeElement("span", "binding-group-name");
          nameWrap.appendChild(makeElement("span", "binding-group-label", name));
          nameWrap.appendChild(makeElement("span", "binding-group-id", suffix || " · 未设置备注"));
          row.appendChild(nameWrap);
          const actions = makeElement("span", "binding-group-actions");
          actions.appendChild(makeElement("span", `binding-group-state ${group.status === "active" ? "is-active" : ""}`, stateText));
          const edit = makeElement("button", "text-button binding-group-edit", "改备注");
          edit.type = "button";
          edit.dataset.bindingId = group.binding_id || "";
          edit.dataset.groupLabel = name;
          edit.disabled = blocked || busy || busyState;
          actions.appendChild(edit);
          const remove = makeElement("button", "text-button binding-group-remove", "删除");
          remove.type = "button";
          remove.dataset.bindingId = group.binding_id || "";
          remove.disabled = blocked || busy || busyState;
          actions.appendChild(remove);
          row.appendChild(actions);
          elements.bindingGroupList.appendChild(row);
        });
      }
    }

    elements.bindButton.disabled = busy || busyState || blocked || state === "bound";
    elements.rebindButton.disabled = busy || busyState || blocked || state !== "bound";
    if (elements.addBindButton) elements.addBindButton.disabled = busy || busyState || blocked;
    elements.cancelBindButton.hidden = !busyState;
    elements.cancelBindButton.disabled = busy || state === "cancelling";
  }

  function renderTest(operation, binding, overallState) {
    const state = renderOperationBadge(elements.testBadge, operation, "idle", operationLabels);
    let title = "确认绑定后再测试";
    let detail = "测试会向真实 QQ 群发送一条测试消息。";
    if (binding.state === "bound") title = "可以发送主动消息测试";
    if (state === "running") {
      title = "正在发送测试消息";
      detail = "正在向已绑定 QQ 群发送测试消息，请稍候。";
    }
    if (state === "succeeded") {
      title = "主动消息测试成功";
      detail = operation.effect || "测试消息已发送到已绑定 QQ 群。";
    }
    if (state === "failed") {
      title = "主动消息测试失败";
      detail = operation.failure_message || "请检查绑定和群主动发言权限后重试。";
    }
    if (state === "cancelled") detail = "测试已取消，可以在确认绑定后重试。";
    setText(elements.testTitle, title);
    setText(elements.testDetail, detail);
    elements.testButton.disabled = busy || state === "running" || binding.state !== "bound" || overallState === "starting" || overallState === "stopping";
  }

  function renderPrime(operation, overallState) {
    const state = renderOperationBadge(elements.primeBadge, operation, "idle", operationLabels);
    const forceEnd = operation.mode === "force_end";
    let title = "默认 prime 不补发历史消息";
    let detail = "默认操作会沿用现有游标；首次初始化从最新位置开始。";
    if (state === "running") {
      title = forceEnd ? "正在从最新位置开始" : "正在执行默认 prime";
      detail = forceEnd ? "正在放弃当前未处理消息，请稍候。" : "正在读取飞书会话并设置游标，请稍候。";
    }
    if (state === "succeeded") {
      title = forceEnd ? "已从最新位置开始" : "默认 prime 已完成";
      detail = operation.effect || detail;
    }
    if (state === "failed") {
      title = "prime 未完成";
      detail = operation.failure_message || "请运行只读检查后重试。";
    }
    if (state === "cancelled") detail = "prime 已取消，本次操作未改动游标。";
    setText(elements.primeTitle, title);
    setText(elements.primeDetail, detail);
    const blocked = busy || state === "running" || serviceMustBeStopped(overallState);
    elements.primeButton.disabled = blocked;
    elements.forceEndButton.disabled = blocked;
  }

  function formatBaseline(value) {
    if (!value) return "指定基准";
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) return "指定基准";
    return new Intl.DateTimeFormat("zh-CN", {
      year: "numeric",
      month: "numeric",
      day: "numeric",
      hour: "2-digit",
      minute: "2-digit",
    }).format(date);
  }

  function renderReplay(operation, channelReplay, overallState) {
    const replay = channelReplay || {};
    const channels = Array.isArray(replay.channels) ? replay.channels : [];
    const readyChannels = channels.filter(function (channel) {
      return channel && channel.state === "ready" && typeof channel.name === "string";
    });
    const selectedBefore = elements.channelSelect.value;
    elements.channelSelect.replaceChildren();
    if (readyChannels.length === 0) {
      const emptyOption = makeElement("option", "", "暂无可补发频道");
      emptyOption.value = "";
      elements.channelSelect.appendChild(emptyOption);
    } else {
      readyChannels.forEach(function (channel) {
        const option = makeElement("option", "", channel.name);
        option.value = channel.name;
        elements.channelSelect.appendChild(option);
      });
      const selected = readyChannels.some(function (channel) {
        return channel.name === selectedBefore;
      }) ? selectedBefore : readyChannels[0].name;
      elements.channelSelect.value = selected;
    }

    const state = renderOperationBadge(elements.replayBadge, operation, "idle", operationLabels);
    let title = "选择一个已建立游标的频道";
    let detail = "自动转发会从各频道当前游标继续；这里可手动控制所选频道的历史积压。";
    const serviceBlocked = serviceMustBeStopped(overallState);
    if (serviceBlocked && state === "idle") {
      title = "四个频道正在自动转发";
      detail = "自动转发运行中。如需手动补发历史消息，请先停止转发服务。";
    }
    if (state === "running" || state === "cancelling") {
      title = `正在补发 ${operation.mode || "所选频道"}`;
      detail = state === "cancelling"
        ? "正在停止补发，已成功发送的消息会保留记录。"
        : "补发进行中，已成功发送的消息会立即推进该频道游标。";
    }
    if (state === "succeeded") {
      title = "指定频道补发已完成";
      detail = operation.effect || "所选频道的可补发消息已处理完成。";
    }
    if (state === "failed") {
      title = "指定频道补发失败";
      detail = operation.failure_message || "已成功发送的消息保持记录，请检查后重试。";
    }
    if (state === "cancelled") {
      title = "指定频道补发已取消";
      detail = "已成功发送的消息保持记录，剩余消息可以再次补发。";
    }
    setText(elements.replayTitle, title);
    setText(elements.replayDetail, detail);

    const operationBusy = state === "running" || state === "cancelling";
    const unavailable = readyChannels.length === 0;
    elements.channelSelect.disabled = busy || serviceBlocked || operationBusy || unavailable;
    elements.replayButton.disabled = busy || serviceBlocked || operationBusy || unavailable;
    if (elements.previewReplayButton) elements.previewReplayButton.disabled = busy || serviceBlocked || operationBusy || unavailable;
    elements.cancelReplayButton.hidden = !operationBusy;
    elements.cancelReplayButton.disabled = busy || state === "cancelling";

    const baseline = formatBaseline(replay.baseline_at);
    const progress = window.__lastState && window.__lastState.replay_progress;
    const progressText = progress && progress.channel === (elements.channelSelect.value || "")
      ? `当前进度：${progress.current || 0}/${progress.total || 0}，已发送 ${progress.forwarded || 0}，已跳过 ${progress.skipped || 0}`
      : "";
    if (elements.replayDialog && elements.replayDialog.open && progress && progress.channel === elements.channelSelect.value) {
      const processed = new Set(progress.processed_ids || []);
      elements.replayDialogList.querySelectorAll(".replay-item").forEach(function (row) {
        const done = processed.has(row.dataset.messageId);
        row.classList.toggle("is-done", done);
        const checkbox = row.querySelector("input");
        if (checkbox && done) checkbox.checked = false;
        if (checkbox) checkbox.disabled = done;
      });
      setText(elements.replayDialogSummary, `处理进度：${progress.current || 0}/${progress.total || 0}，已发送 ${progress.forwarded || 0}，已跳过 ${progress.skipped || 0}`);
    }
    setText(
      elements.replaySummary,
      replay.available && readyChannels.length
        ? `已配置 ${readyChannels.length} 个频道；初始基准为 ${baseline}。${progressText}`
        : "暂无可用的频道游标，请先建立频道游标。",
    );
  }

  function renderRecovery(data) {
    const recovery = data.recovery || {};
    const hints = Array.isArray(recovery.hints) ? recovery.hints : [];
    elements.recoveryList.replaceChildren();
    if (hints.length === 0) {
      elements.recoveryList.appendChild(makeElement("div", "empty-state", "暂时没有恢复建议。"));
    } else {
      const list = makeElement("ul", "recovery-items");
      hints.forEach(function (hint) {
        list.appendChild(makeElement("li", "", hint));
      });
      elements.recoveryList.appendChild(list);
    }
    const check = data.check || {};
    const overall = data.overall || {};
    elements.recoveryCheckButton.disabled = busy || check.state === "running";
    elements.recoveryRestartButton.disabled = busy || overall.state === "starting" || overall.state === "stopping";
  }

  function renderEvents(events) {
    elements.eventsList.replaceChildren();
    if (!Array.isArray(events) || events.length === 0) {
      elements.eventsList.appendChild(makeElement("div", "empty-state", "本次控制面会话还没有事件。"));
      return;
    }
    events.slice().reverse().slice(0, 24).forEach(function (event) {
      const row = makeElement("div", "event-row");
      row.appendChild(makeElement("time", "event-time", formatTime(event.occurred_at)));
      row.appendChild(makeElement("span", `event-dot ${event.level === "error" ? "is-error" : event.level === "warning" ? "is-warning" : ""}`));
      row.appendChild(makeElement("span", "event-message", event.message || "状态更新"));
      row.appendChild(makeElement("span", "event-type", eventLabels[event.type] || "状态"));
      elements.eventsList.appendChild(row);
    });
  }

  function render(data) {
    window.__lastState = data;
    const operations = operationsFrom(data);
    const runtime = data.runtime || {};
    const overallState = (data.overall || {}).state || "stopped";
    renderOverall(data);
    renderBinding(operations.binding, runtime, overallState);
    renderTest(operations.test, operations.binding, overallState);
    renderPrime(operations.prime, overallState);
    renderReplay(operations.replay, data.channel_replay, overallState);
    renderProcesses(data.processes);
    renderRuntime(data);
    renderRecovery(data);
    renderEvents(data.events);
    if (elements.listenerList) {
      elements.listenerList.replaceChildren();
      (Array.isArray(data.listeners) ? data.listeners : ["Perfecto"]).forEach(function (name) {
        elements.listenerList.appendChild(makeElement("span", "listener-chip", name));
      });
    }
    if (elements.listenerChannelList) {
      elements.listenerChannelList.replaceChildren();
      const channels = data.channel_replay && Array.isArray(data.channel_replay.channels)
        ? data.channel_replay.channels : [];
      if (!channels.length) {
        elements.listenerChannelList.appendChild(makeElement("span", "panel-footnote", "暂无已配置监听频道"));
      } else {
        channels.forEach(function (channel) {
          const label = channel.state === "ready" ? `${channel.name} · 监听中` : `${channel.name} · ${channel.label || "未就绪"}`;
          elements.listenerChannelList.appendChild(makeElement("span", `listener-chip ${channel.state === "ready" ? "is-channel-active" : "is-channel-disabled"}`, label));
        });
      }
    }
    setText(elements.sessionTime, `最后更新 ${formatTime(new Date().toISOString())}`);
    showError("");
  }

  async function fetchJson(url, options) {
    const response = await fetch(url, Object.assign({ cache: "no-store" }, options || {}));
    let payload = null;
    try {
      payload = await response.json();
    } catch (_error) {
      throw new Error("控制面返回了无效响应");
    }
    if (!response.ok || !payload.ok) {
      throw new Error((payload && payload.error) || "请求未完成");
    }
    return payload.data;
  }

  async function refresh() {
    try {
      const data = await fetchJson("/api/status");
      render(data);
    } catch (error) {
      showError(error.message || "无法读取本机状态");
      setText(elements.sessionTime, "连接失败");
    }
  }

  function renderActionBusy() {
    [
      elements.startButton,
      elements.stopButton,
      elements.restartButton,
      elements.checkButton,
      elements.recoveryCheckButton,
      elements.recoveryRestartButton,
      elements.bindButton,
      elements.rebindButton,
      elements.cancelBindButton,
      elements.testButton,
      elements.primeButton,
      elements.forceEndButton,
      elements.channelSelect,
      elements.replayButton,
      elements.cancelReplayButton,
    ].forEach(function (button) {
      button.disabled = true;
    });
  }

  async function runAction(path, label, body) {
    if (busy) return;
    busy = true;
    showFeedback(`${label}请求已发送。`, false);
    renderActionBusy();
    try {
      await fetchJson(path, {
        method: "POST",
        headers: {
          "X-Control-Token": controlToken,
          "Content-Type": "application/json",
        },
        body: JSON.stringify(body || {}),
      });
      showFeedback(`${label}已接受，状态会自动更新。`, false);
      await refresh();
    } catch (error) {
      showFeedback(error.message || `${label}未完成。`, true);
      await refresh();
    } finally {
      busy = false;
      await refresh();
    }
  }

  function bindActions() {
    elements.startButton.addEventListener("click", function () {
      if (window.confirm("启动后会自动转发 Perfecto 和四个频道；各频道会从当前游标继续，尚未补发的历史消息也可能被发送。确定启动吗？")) {
        runAction("/api/actions/start", "启动");
      }
    });
    elements.stopButton.addEventListener("click", function () { runAction("/api/actions/stop", "停止"); });
    elements.restartButton.addEventListener("click", function () { runAction("/api/actions/restart", "重启"); });
    elements.checkButton.addEventListener("click", function () { runAction("/api/actions/check", "只读检查"); });
    elements.recoveryCheckButton.addEventListener("click", function () { runAction("/api/actions/check", "检查权限与连接"); });
    elements.recoveryRestartButton.addEventListener("click", function () { runAction("/api/actions/restart", "重启"); });
    elements.bindButton.addEventListener("click", function () { runAction("/api/actions/bind", "绑定"); });
    if (elements.addBindButton) elements.addBindButton.addEventListener("click", function () {
      runAction("/api/actions/bind", "新增群绑定", { add: true });
    });
    elements.cancelBindButton.addEventListener("click", function () { runAction("/api/actions/bind/cancel", "取消绑定"); });
    if (elements.listenerAddButton) elements.listenerAddButton.addEventListener("click", async function () {
      const name = (elements.listenerName.value || "").trim();
      if (!name) { setText(elements.listenerFeedback, "请输入飞书联系人名称。"); return; }
      elements.listenerAddButton.disabled = true;
      try {
        const result = await fetchJson("/api/listeners", { method: "POST", headers: { "X-Control-Token": controlToken, "Content-Type": "application/json" }, body: JSON.stringify({ name }) });
        elements.listenerName.value = "";
        setText(elements.listenerFeedback, `已新增“${name}”，正在自动热更新转发任务…`);
        if (elements.listenerList) {
          elements.listenerList.replaceChildren();
          result.listeners.forEach(value => elements.listenerList.appendChild(makeElement("span", "listener-chip", value)));
        }
        try {
          await fetchJson("/api/actions/restart", {
            method: "POST",
            headers: { "X-Control-Token": controlToken, "Content-Type": "application/json" },
            body: JSON.stringify({}),
          });
          setText(elements.listenerFeedback, `已新增“${name}”，转发任务已自动热更新。`);
        } catch (reloadError) {
          setText(elements.listenerFeedback, `已新增“${name}”，但自动热更新失败：${reloadError.message || "请稍后重试"}`);
        }
      } catch (error) { setText(elements.listenerFeedback, error.message || "新增监听失败。"); }
      finally { elements.listenerAddButton.disabled = false; }
    });
    if (elements.bindingGroupList) elements.bindingGroupList.addEventListener("click", function (event) {
      const button = event.target.closest("button[data-binding-id]");
      if (!button || !button.dataset.bindingId) return;
      if (button.classList.contains("binding-group-edit")) {
        const label = window.prompt("请输入 QQ 群备注（例如：主群、测试群）", button.dataset.groupLabel || "");
        if (label !== null && label.trim()) {
          runAction("/api/actions/groups/label", "更新 QQ 群备注", { binding_id: button.dataset.bindingId, label: label.trim() });
        }
        return;
      }
      if (window.confirm("删除后将停止向这个 QQ 群投递，但不会删除历史投递记录。确定删除吗？")) {
        runAction("/api/actions/groups/remove", "删除 QQ 群", { binding_id: button.dataset.bindingId, confirm: true });
      }
    });
    elements.replayButton.addEventListener("click", function () {
      const channel = elements.channelSelect.value;
      if (!channel) return;
      const rawIds = elements.messageIds ? elements.messageIds.value : "";
      const messageIds = rawIds.split(/[，,\s]+/).map(value => value.trim()).filter(Boolean);
      if (window.confirm(`将把“${channel}”${messageIds.length ? `选中的 ${messageIds.length} 条消息` : "全部待补发消息"}补发到当前 QQ 群，消息会真实发送。确定继续吗？`)) {
        runAction("/api/actions/replay", "频道补发", { channel: channel, message_ids: messageIds });
      }
    });
    if (elements.previewReplayButton) elements.previewReplayButton.addEventListener("click", async function () {
      const channel = elements.channelSelect.value;
      if (!channel) return;
      if (elements.replayDialog.showModal && !elements.replayDialog.open) elements.replayDialog.showModal();
      elements.replayDialogList.replaceChildren(makeElement("div", "empty-state", "正在读取待补发消息…"));
      setText(elements.replayDialogSummary, `正在读取“${channel}”的待补发清单，请稍候。`);
      elements.replayDialogSelectAll.disabled = true;
      elements.replayDialogConfirm.disabled = true;
      setText(elements.replayDialogSelectAll, "全选");
      try {
        const result = await fetchJson(`/api/replay/preview?channel=${encodeURIComponent(channel)}`);
        const items = result.items || [];
        window.__replayPreviewItems = items;
        elements.replayDialogList.replaceChildren();
        items.forEach(function (item) {
          const row = makeElement("label", "replay-item");
          row.dataset.messageId = item.message_id;
          const checkbox = document.createElement("input");
          checkbox.type = "checkbox";
          checkbox.value = item.message_id;
          checkbox.checked = true;
          row.appendChild(checkbox);
          const main = makeElement("div", "replay-item-main");
          main.appendChild(makeElement("div", "replay-item-meta", `${item.position} · ${item.type === "image" ? "图片" : "文本"} · ${item.message_id}`));
          if (item.type === "image") {
            main.appendChild(makeElement("span", "replay-item-image", item.image_key ? `图片资源：${item.image_key}` : "图片消息（缺少资源键）"));
          } else {
            main.appendChild(makeElement("div", "replay-item-content", item.content || item.preview || "（空文本）"));
          }
          row.appendChild(main);
          elements.replayDialogList.appendChild(row);
        });
        setText(elements.replayDialogSummary, items.length ? `共 ${items.length} 条待补发，勾选后点击“使用选中消息”。` : "当前没有待补发消息。");
        elements.replayDialogSelectAll.disabled = items.length === 0;
        elements.replayDialogConfirm.disabled = items.length === 0;
        if (elements.replayDialogSelectAll) setText(elements.replayDialogSelectAll, "全不选");
      } catch (error) {
        elements.replayDialogList.replaceChildren(makeElement("div", "empty-state", error.message || "无法读取待补发消息。"));
        setText(elements.replayDialogSummary, "读取失败，请关闭弹窗后重试。");
      }
    });
    if (elements.replayDialogClose) elements.replayDialogClose.addEventListener("click", function () { elements.replayDialog.close(); });
    if (elements.replayDialogSelectAll) elements.replayDialogSelectAll.addEventListener("click", function () {
      const inputs = Array.from(elements.replayDialogList.querySelectorAll("input:not(:disabled)"));
      const shouldClear = inputs.length > 0 && inputs.every(input => input.checked);
      inputs.forEach(input => { input.checked = !shouldClear; });
      setText(elements.replayDialogSelectAll, shouldClear ? "全选" : "全不选");
    });
    if (elements.replayDialogList) elements.replayDialogList.addEventListener("change", function () {
      const inputs = Array.from(elements.replayDialogList.querySelectorAll("input:not(:disabled)"));
      setText(elements.replayDialogSelectAll, inputs.length && inputs.every(input => input.checked) ? "全不选" : "全选");
    });
    if (elements.replayDialogConfirm) elements.replayDialogConfirm.addEventListener("click", function () {
      const ids = Array.from(elements.replayDialogList.querySelectorAll("input:checked")).map(input => input.value);
      if (!ids.length) {
        setText(elements.replayDialogSummary, "请至少选择一条消息；如需全部补发，请关闭弹窗后直接点击补发。" );
        return;
      }
      elements.messageIds.value = ids.join("\n");
      setText(elements.replayItems, ids.length ? `已选择 ${ids.length} 条消息，点击“补发选中频道”开始。` : "未选择消息，将不会补发。");
      elements.replayDialog.close();
    });
    elements.cancelReplayButton.addEventListener("click", function () {
      runAction("/api/actions/replay/cancel", "取消补发");
    });
    elements.rebindButton.addEventListener("click", function () {
      if (window.confirm("重新绑定会等待新的 QQ 群确认，并替换现有绑定。请确认你要继续。")) {
        runAction("/api/actions/bind", "重新绑定", { rebind: true, confirm: true });
      }
    });
    elements.testButton.addEventListener("click", function () {
      if (window.confirm("这会向真实 QQ 群发送一条测试消息，不是模拟操作。确定继续吗？")) {
        runAction("/api/actions/test", "主动消息测试");
      }
    });
    elements.primeButton.addEventListener("click", function () { runAction("/api/actions/prime", "默认 prime"); });
    elements.forceEndButton.addEventListener("click", function () {
      if (window.confirm("force-end 会放弃当前未处理消息，并从最新位置开始；历史消息不会补发。确定继续吗？")) {
        runAction("/api/actions/prime/force-end", "从最新位置开始", { force_end: true, confirm: true });
      }
    });
  }

  async function boot() {
    bindActions();
    try {
      const session = await fetchJson("/api/session");
      controlToken = session.control_token || "";
      if (!controlToken) throw new Error("本机操作令牌不可用");
      await refresh();
      pollTimer = window.setInterval(refresh, 2500);
    } catch (error) {
      showError(error.message || "无法连接本机控制面");
      setText(elements.sessionTime, "连接失败");
    }
  }

  window.addEventListener("beforeunload", function () {
    if (pollTimer) window.clearInterval(pollTimer);
  });

  boot();
}());
