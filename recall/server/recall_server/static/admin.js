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
      <strong>${escapeText(item.email)}</strong>
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
  const connection = $("#google-connection");
  connection.replaceChildren();
  connection.append(connected.length
    ? `${connected.length} connection${connected.length === 1 ? "" : "s"} · authority bound server-side`
    : available ? "Not connected" : "No connection provider configured");
  connected.forEach((item) => {
    const disconnect = document.createElement("button");
    disconnect.type = "button";
    disconnect.dataset.connectionId = item.id;
    disconnect.textContent = `Disconnect ${item.provider}`;
    connection.append(disconnect);
  });
  connection.classList.toggle("connected", connected.length > 0);
  $("#google-form button[type=submit]").disabled = !available;
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
    const action = item.state === "enabled"
      ? "pause"
      : item.state === "paused"
        ? "resume"
        : item.state === "revoked"
          ? "uninstall"
          : "enable";
    const revoke = item.state === "revoked"
      ? ""
      : `<button data-action="revoke" data-id="${item.id}">revoke</button>`;
    const runtime = item.last_error_code
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
        <button data-action="${action}" data-id="${item.id}">${action}</button>
        ${revoke}
      </div>`;
    target.append(row);
  });
}

function render() {
  renderBrains();
  renderAccess();
  renderGoogle();
  renderCatalog();
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
        email: $("#invite-email").value,
        role: $("#invite-role").value,
      }),
    });
    $("#invite-email").value = "";
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

$("#google-auth-provider").addEventListener("change", () => {
  $("#google-auth-note").textContent = $("#google-auth-provider").value === "composio"
    ? "Authorize one source per trip; Recall binds the exact hosted account."
    : "Authorize several selected sources in one direct Google trip.";
});

const oauth = new URLSearchParams(window.location.search).get("oauth");
if (oauth === "connected") history.replaceState({}, "", "/admin");
load();
