const $ = (selector) => document.querySelector(selector);
const state = { data: null };
const googleLabels = {
  "google.gmail": "Gmail",
  "google.calendar": "Calendar",
  "google.contacts": "Contacts",
  "google.drive": "Drive",
};

function cookie(name) {
  const row = document.cookie.split("; ").find((value) => value.startsWith(`${name}=`));
  return row ? decodeURIComponent(row.split("=").slice(1).join("=")) : "";
}

async function api(path, options = {}) {
  const headers = { "Content-Type": "application/json", ...(options.headers || {}) };
  if (options.method && options.method !== "GET") headers["X-Recall-CSRF"] = cookie("recall_admin_csrf");
  const response = await fetch(path, { credentials: "same-origin", ...options, headers });
  const payload = await response.json();
  if (!response.ok) {
    const error = new Error(payload.error || "request_failed");
    error.status = response.status;
    throw error;
  }
  return payload;
}

function toast(message) {
  const node = $("#toast");
  node.textContent = message;
  node.classList.add("show");
  window.setTimeout(() => node.classList.remove("show"), 2600);
}

function renderBrains() {
  const target = $("#brain-list");
  target.replaceChildren();
  state.data.brains.forEach((brain, index) => {
    const card = document.createElement("article");
    card.className = "brain-card";
    card.dataset.index = String(index + 1).padStart(2, "0");
    card.innerHTML = `
      <span class="brain-kind">${brain.brain_kind}</span>
      <h3>${escapeText(brain.display_name)}</h3>
      <p>${escapeText(brain.slug)} · ${escapeText(brain.permission)} access</p>`;
    target.append(card);
  });
}

function renderAccess() {
  const brains = state.data.brains.filter((brain) =>
    brain.brain_kind === "company" && ["owner", "admin"].includes(brain.permission)
  );
  const select = $("#invite-brain");
  select.replaceChildren(...brains.map((brain) => {
    const option = document.createElement("option");
    option.value = brain.tenant_id;
    option.textContent = brain.display_name;
    return option;
  }));
  const form = $("#invite-form");
  [...form.elements].forEach((element) => { element.disabled = !brains.length; });
  renderInviteEndpoint();

  const target = $("#invitation-list");
  target.replaceChildren();
  const invitations = state.data.invitations || [];
  if (!invitations.length) {
    target.innerHTML = '<p class="access-empty">No invitations yet. The first teammate can be here in under a minute.</p>';
    return;
  }
  invitations.forEach((item) => {
    const row = document.createElement("article");
    row.className = `access-row state-${item.status}`;
    const canRevoke = ["pending", "active"].includes(item.status);
    row.innerHTML = `
      <strong>${escapeText(item.display_name || item.email)}</strong>
      <span>${escapeText(item.role)}</span>
      <span class="access-state"><i></i>${escapeText(item.status)}</span>
      <button type="button" data-invitation-id="${escapeText(item.id)}" ${canRevoke ? "" : "disabled"}>
        ${item.status === "active" ? "remove" : "revoke"}
      </button>`;
    target.append(row);
  });
}

function renderInviteEndpoint() {
  const tenantId = $("#invite-brain").value;
  $("#invite-endpoint").value = tenantId
    ? `${window.location.origin}/mcp/brains/${tenantId}`
    : "";
}

function escapeText(value) {
  const node = document.createElement("span");
  node.textContent = String(value);
  return node.innerHTML;
}

function brainOptions() {
  const brains = [...state.data.brains].sort((left, right) => {
    const leftRank = left.brain_kind === "personal" ? 0 : 1;
    const rightRank = right.brain_kind === "personal" ? 0 : 1;
    return leftRank - rightRank || left.display_name.localeCompare(right.display_name);
  });
  return brains.map((brain) =>
    `<option value="${escapeText(brain.tenant_id)}">${escapeText(brain.display_name)} · ${escapeText(brain.brain_kind)}</option>`
  ).join("");
}

function renderGoogle() {
  const target = $("#google-routes");
  target.replaceChildren();
  const providerIds = new Set(state.data.providers.map((item) => item.id));
  const supported = [
    ["composio", "Hosted connection (Composio)"],
    ["google", "Direct Google OAuth"],
  ].filter(([id]) => providerIds.has(id));
  const available = supported.length > 0;
  const provider = $("#google-auth-provider");
  const previous = provider.value;
  provider.replaceChildren(...supported.map(([id, label]) => {
    const option = document.createElement("option");
    option.value = id;
    option.textContent = label;
    return option;
  }));
  if (supported.some(([id]) => id === previous)) provider.value = previous;
  provider.disabled = !available;
  const renderProviderNote = () => {
    $("#google-auth-note").textContent = provider.value === "composio"
      ? "Authorize one source per trip; Recall binds the exact hosted account."
      : "Authorize several selected sources in one direct Google trip.";
  };
  renderProviderNote();
  Object.entries(googleLabels).forEach(([connectorId, label]) => {
    const row = document.createElement("div");
    row.className = "route-row";
    row.dataset.connector = connectorId;
    row.innerHTML = `
      <strong>${label}</strong>
      <label><span class="sr-only">Destination for ${label}</span><select ${available ? "" : "disabled"}>${brainOptions()}</select></label>
      <label class="switch"><input type="checkbox" aria-label="Enable ${label}" ${available ? "" : "disabled"}><span></span></label>`;
    target.append(row);
  });
  const connected = state.data.connections.filter((item) =>
    item.provider === "google" || item.provider === "composio"
  );
  const active = connected.filter((item) => item.status === "connected");
  const degraded = connected.filter((item) => item.status === "degraded");
  const connection = $("#google-connection");
  connection.replaceChildren();
  connection.append(active.length
    ? `${active.length} connection${active.length === 1 ? "" : "s"} · authority bound server-side`
    : degraded.length
      ? "Connection expired · authorize the source again"
    : available ? "Not connected" : "No connection provider configured");
  connected.forEach((item) => {
    const disconnect = document.createElement("button");
    disconnect.type = "button";
    disconnect.dataset.connectionId = item.id;
    disconnect.textContent = item.status === "degraded"
      ? `Remove expired ${item.provider}`
      : `Disconnect ${item.provider}`;
    connection.append(disconnect);
  });
  connection.classList.toggle("connected", active.length > 0);
  $("#google-form button[type=submit]").disabled = !available;
}

function renderSlack() {
  const available = state.data.providers.some((item) => item.id === "slack");
  const brain = $("#slack-brain");
  brain.innerHTML = brainOptions();
  brain.disabled = !available;
  $("#slack-enabled").disabled = !available;
  $("#slack-form button[type=submit]").disabled = !available;
  const connections = state.data.connections.filter((item) => item.provider === "slack");
  const active = connections.filter((item) => item.status === "connected");
  const target = $("#slack-connection");
  target.replaceChildren();
  target.append(active.length ? "Connected" : available ? "Not connected" : "Slack OAuth is not configured");
  connections.forEach((item) => {
    const button = document.createElement("button");
    button.type = "button";
    button.dataset.connectionId = item.id;
    button.textContent = "Disconnect Slack";
    target.append(button);
  });
  target.classList.toggle("connected", active.length > 0);
}

function renderCatalog() {
  const target = $("#integration-grid");
  target.replaceChildren();
  state.data.catalog
    .filter((item) => !item.connector_id.startsWith("google."))
    .slice(0, 6)
    .forEach((item) => {
      const card = document.createElement("article");
      card.className = "catalog-card";
      card.innerHTML = `
        <span class="provider-kicker">${escapeText(item.placement.execution)} · ${escapeText(item.auth.kind)}</span>
        <h3>${escapeText(item.connector_id.replaceAll(".", " / "))}</h3>
        <p>${escapeText(item.source_family)} · shared control contract ready</p>`;
      target.append(card);
    });
}

function formatBytes(value) {
  const bytes = Number(value || 0);
  if (bytes < 1024) return `${bytes} B`;
  const units = ["KB", "MB", "GB", "TB"];
  let amount = bytes;
  let unit = -1;
  do {
    amount /= 1024;
    unit += 1;
  } while (amount >= 1024 && unit < units.length - 1);
  return `${amount >= 10 ? amount.toFixed(0) : amount.toFixed(1)} ${units[unit]}`;
}

function relativeTime(value) {
  if (!value) return "never";
  const seconds = Math.max(0, Math.floor((Date.now() - new Date(value).getTime()) / 1000));
  if (seconds < 60) return `${seconds}s ago`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ago`;
  if (seconds < 86400) return `${Math.floor(seconds / 3600)}h ago`;
  return `${Math.floor(seconds / 86400)}d ago`;
}

function sourceDevice(item) {
  if (item.device_id) return item.device_id;
  const parts = String(item.source_id).split(":");
  if (["claude", "codex"].includes(parts[0]) && parts.length > 2) {
    return parts.slice(2).join(":");
  }
  return item.execution === "remote_worker" ? "Hosted connector" : item.source_id;
}

function fleetRank(value) {
  return ({ degraded: 5, stale: 4, unknown: 3, backfilling: 2, running: 1, ready: 0 })[value] ?? 3;
}

function renderFleet() {
  const rows = state.data.fleet || [];
  const summary = $("#fleet-summary");
  const target = $("#fleet-list");
  summary.replaceChildren();
  target.replaceChildren();
  if (!rows.length) {
    summary.innerHTML = '<p class="empty">No administered sources yet.</p>';
    return;
  }
  const brains = new Map(state.data.brains.map((brain) => [brain.tenant_id, brain.display_name]));
  const groups = new Map();
  rows.forEach((item) => {
    const device = sourceDevice(item);
    const key = `${item.tenant_id}\u0000${item.owner_principal_id}\u0000${device}`;
    const group = groups.get(key) || {
      tenantId: item.tenant_id,
      owner: item.owner_display_name,
      device,
      sources: [],
      records: 0,
      bytes: 0,
      health: "ready",
      lastTransfer: null,
      lastReport: null,
    };
    group.sources.push(item);
    group.records += Number(item.records_24h || 0);
    group.bytes += Number(item.bytes_24h || 0);
    if (fleetRank(item.health) > fleetRank(group.health)) group.health = item.health;
    if (item.last_transfer_at && (!group.lastTransfer || item.last_transfer_at > group.lastTransfer)) {
      group.lastTransfer = item.last_transfer_at;
    }
    if (item.reported_at && (!group.lastReport || item.reported_at > group.lastReport)) {
      group.lastReport = item.reported_at;
    }
    groups.set(key, group);
  });
  const machines = [...groups.values()].sort((left, right) =>
    left.owner.localeCompare(right.owner) || left.device.localeCompare(right.device)
  );
  const records24h = machines.reduce((sum, item) => sum + item.records, 0);
  const bytes24h = machines.reduce((sum, item) => sum + item.bytes, 0);
  const attention = machines.filter((item) => fleetRank(item.health) >= fleetRank("unknown")).length;
  [
    ["Machines", machines.length],
    ["Records / 24h", records24h.toLocaleString()],
    ["Raw / 24h", formatBytes(bytes24h)],
    ["Needs attention", attention],
  ].forEach(([label, value]) => {
    const card = document.createElement("article");
    card.innerHTML = `<span>${escapeText(label)}</span><strong>${escapeText(value)}</strong>`;
    summary.append(card);
  });
  machines.forEach((machine) => {
    const row = document.createElement("article");
    row.className = `fleet-machine health-${machine.health}`;
    const sourceRows = machine.sources.map((item) => {
      const coverage = item.coverage_percent == null
        ? ""
        : ` · ${Number(item.coverage_percent).toFixed(0)}% covered`;
      const queue = Number(item.pending_records || 0) || Number(item.dead_records || 0)
        ? ` · ${Number(item.pending_records || 0)} pending / ${Number(item.dead_records || 0)} dead`
        : "";
      const label = item.collector_kind || item.connector_id || item.source_family || item.source_id;
      return `<li><span class="source-health health-${escapeText(item.health)}"></span><strong>${escapeText(label)}</strong><span>${escapeText(item.health)}${escapeText(coverage)}${escapeText(queue)}</span></li>`;
    }).join("");
    row.innerHTML = `
      <div class="fleet-machine-head">
        <div><span class="fleet-owner">${escapeText(machine.owner)}</span><h3>${escapeText(machine.device)}</h3></div>
        <span class="fleet-health">${escapeText(machine.health)}</span>
      </div>
      <div class="fleet-machine-metrics">
        <div><span>Brain</span><strong>${escapeText(brains.get(machine.tenantId) || machine.tenantId)}</strong></div>
        <div><span>Transferred / 24h</span><strong>${machine.records.toLocaleString()} records · ${formatBytes(machine.bytes)}</strong></div>
        <div><span>Last transfer</span><strong>${relativeTime(machine.lastTransfer)}</strong></div>
        <div><span>Last heartbeat</span><strong>${relativeTime(machine.lastReport)}</strong></div>
      </div>
      <ul class="fleet-sources">${sourceRows}</ul>`;
    target.append(row);
  });
}

function renderInstallations() {
  const target = $("#installation-list");
  target.replaceChildren();
  if (!state.data.installations.length) {
    target.innerHTML = '<p class="empty">No live routes yet. Switch on a source above.</p>';
    return;
  }
  const brains = new Map(state.data.brains.map((brain) => [brain.tenant_id, brain.display_name]));
  state.data.installations.forEach((item) => {
    const row = document.createElement("article");
    row.className = "installation";
    const reconnectRequired = [
      "connector_authority_revoked",
      "connector_authority_forbidden",
    ].includes(item.last_error_code);
    const action = reconnectRequired
      ? null
      : item.state === "enabled"
      ? "pause"
      : item.state === "paused"
        ? "resume"
        : item.state === "revoked"
          ? "uninstall"
          : "enable";
    const revoke = item.state === "revoked"
      ? ""
      : `<button data-action="revoke" data-id="${item.id}">revoke</button>`;
    const transition = action
      ? `<button data-action="${action}" data-id="${item.id}">${action}</button>`
      : "";
    const runtime = reconnectRequired
      ? "reconnect Google to resume"
      : item.last_error_code
        ? `attention · ${item.last_error_code}`
      : item.last_success_at
        ? "synced"
        : item.execution === "remote_worker" && item.state === "enabled"
          ? "waiting for first sync"
          : item.state;
    row.innerHTML = `
      <strong>${escapeText(item.connector_id)}</strong>
      <span>${escapeText(brains.get(item.tenant_id) || item.tenant_id)}</span>
      <span class="state">${escapeText(runtime)}</span>
      <div class="installation-actions">
        ${transition}
        ${revoke}
      </div>`;
    target.append(row);
  });
}

function render() {
  renderBrains();
  renderAccess();
  renderGoogle();
  renderSlack();
  renderCatalog();
  renderFleet();
  renderInstallations();
  $(".pulse").classList.add("ready");
  $("#system-label").textContent = "CONTROL PLANE / READY";
}

$("#invite-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  try {
    const invitation = await api("/admin/api/v1/invitations", {
      method: "POST",
      body: JSON.stringify({
        tenant_id: $("#invite-brain").value,
        display_name: $("#invite-name").value,
        email: $("#invite-email").value,
        role: $("#invite-role").value,
      }),
    });
    $("#invite-email").value = "";
    $("#invite-name").value = "";
    if (invitation.delivery?.status === "sent") {
      toast("Invitation emailed. OAuth activates access automatically.");
    } else if (invitation.delivery?.status === "failed") {
      toast("Invitation created, but email delivery failed. Re-invite to retry.");
    } else {
      toast("Invitation ready. Email delivery is not configured; copy the endpoint.");
    }
    await load();
  } catch (error) {
    toast(`Invitation unchanged: ${error.message}`);
  }
});

$("#invite-brain").addEventListener("change", renderInviteEndpoint);

$("#copy-invite-endpoint").addEventListener("click", async () => {
  const value = $("#invite-endpoint").value;
  if (!value) return;
  try {
    await navigator.clipboard.writeText(value);
    toast("Company-brain MCP endpoint copied.");
  } catch (_error) {
    $("#invite-endpoint").select();
    toast("Endpoint selected. Copy it from the field.");
  }
});

$("#invitation-list").addEventListener("click", async (event) => {
  const button = event.target.closest("button[data-invitation-id]");
  if (!button || button.disabled) return;
  if (!window.confirm("Remove this company-brain access immediately?")) return;
  try {
    await api(`/admin/api/v1/invitations/${button.dataset.invitationId}/revoke`, {
      method: "POST",
      body: JSON.stringify({}),
    });
    toast("Access revoked. The next MCP request will be denied.");
    await load();
  } catch (error) {
    toast(`Access unchanged: ${error.message}`);
  }
});

async function loadAuthMethods() {
  try {
    const methods = await api("/admin/api/v1/auth-methods");
    $("#oauth-login").hidden = !methods.oauth;
    $("#oauth-copy").hidden = !methods.oauth;
    $("#legacy-login").open = !methods.oauth;
  } catch (_error) {
    $("#legacy-login").open = true;
  }
}

async function load() {
  try {
    state.data = await api("/admin/api/v1/state");
    render();
  } catch (error) {
    if (error.status === 401) $("#auth-dialog").showModal();
    else toast(`Could not load: ${error.message}`);
  }
}

$("#auth-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  if (!$("#admin-token").value) return;
  $("#auth-error").textContent = "";
  try {
    await api("/admin/api/v1/session", {
      method: "POST",
      body: JSON.stringify({ token: $("#admin-token").value }),
    });
    $("#admin-token").value = "";
    $("#auth-dialog").close();
    await load();
  } catch (error) {
    $("#auth-error").textContent = "That key was not accepted.";
  }
});

$("#google-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  const routes = [...document.querySelectorAll(".route-row")]
    .filter((row) => row.querySelector("input").checked)
    .map((row) => ({
      connector_id: row.dataset.connector,
      tenant_id: row.querySelector("select").value,
      privacy_mode: "scrub",
      selectors: {},
  }));
  if (!routes.length) return toast("Switch on at least one Google source.");
  const brainKinds = new Map(
    state.data.brains.map((brain) => [brain.tenant_id, brain.brain_kind])
  );
  if (
    routes.some((route) => brainKinds.get(route.tenant_id) === "company")
    && !window.confirm(
      "Company brains are shared. Selected Google content will be available to authorized company members. Continue?"
    )
  ) return;
  const provider = $("#google-auth-provider").value;
  if (provider === "composio" && routes.length !== 1) {
    return toast("Hosted connections authorize one Google source at a time.");
  }
  try {
    const result = await api("/admin/api/v1/oauth/start", {
      method: "POST",
      body: JSON.stringify({ provider, routes }),
    });
    window.location.assign(result.authorization_url);
  } catch (error) {
    toast(`Authorization did not start: ${error.message}`);
  }
});

$("#slack-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  if (!$("#slack-enabled").checked) return toast("Switch Slack on first.");
  const tenantId = $("#slack-brain").value;
  const brain = state.data.brains.find((item) => item.tenant_id === tenantId);
  if (brain?.brain_kind === "company" && !window.confirm(
    "Slack content will be available to authorized company members. Continue?"
  )) return;
  try {
    const result = await api("/admin/api/v1/oauth/start", {
      method: "POST",
      body: JSON.stringify({
        provider: "slack",
        routes: [{
          connector_id: "slack.messages",
          tenant_id: tenantId,
          privacy_mode: "scrub",
          selectors: { channel_ids: [], owner_user_ids: [] },
        }],
      }),
    });
    window.location.assign(result.authorization_url);
  } catch (error) {
    toast(`Slack authorization did not start: ${error.message}`);
  }
});

$("#installation-list").addEventListener("click", async (event) => {
  const button = event.target.closest("button[data-action]");
  if (!button) return;
  if (
    button.dataset.action === "revoke"
    && !window.confirm("Revoke this routed source? Its checkpoint is retained.")
  ) return;
  try {
    await api(`/admin/api/v1/installations/${button.dataset.id}/actions`, {
      method: "POST",
      body: JSON.stringify({ action: button.dataset.action }),
    });
    toast(`Route ${button.dataset.action}d.`);
    await load();
  } catch (error) {
    toast(`Route unchanged: ${error.message}`);
  }
});

$("#google-connection").addEventListener("click", async (event) => {
  const button = event.target.closest("button[data-connection-id]");
  if (!button) return;
  if (!window.confirm("Disconnect Google and revoke every dependent route?")) return;
  try {
    await api(`/admin/api/v1/connections/${button.dataset.connectionId}/revoke`, {
      method: "POST",
      body: JSON.stringify({}),
    });
    toast("Provider authority revoked and encrypted references wiped.");
    await load();
  } catch (error) {
    toast(`Provider remains connected: ${error.message}`);
  }
});

$("#slack-connection").addEventListener("click", async (event) => {
  const button = event.target.closest("button[data-connection-id]");
  if (!button || !window.confirm("Disconnect Slack and revoke its route?")) return;
  try {
    await api(`/admin/api/v1/connections/${button.dataset.connectionId}/revoke`, {
      method: "POST", body: JSON.stringify({}),
    });
    toast("Slack disconnected.");
    await load();
  } catch (error) {
    toast(`Slack remains connected: ${error.message}`);
  }
});

$("#google-auth-provider").addEventListener("change", () => {
  $("#google-auth-note").textContent = $("#google-auth-provider").value === "composio"
    ? "Authorize one source per trip; Recall binds the exact hosted account."
    : "Authorize several selected sources in one direct Google trip.";
});

const oauth = new URLSearchParams(window.location.search).get("oauth");
if (oauth === "connected") history.replaceState({}, "", "/admin");
loadAuthMethods().then(load);
