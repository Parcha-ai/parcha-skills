#!/usr/bin/env node

/**
 * Apply Recall's hosted Descope experience without changing flow behavior.
 *
 * Required environment variables:
 *   RECALL_DESCOPE_PROJECT_ID
 *   RECALL_DESCOPE_MGMT_KEY
 *
 * Existing component IDs and interactions are deliberately preserved.
 */

"use strict";

const { isDeepStrictEqual } = require("node:util");

const FLOW_ID = "recall-mcp-user-consent";
const STYLE_ID = "recall";
const JETBRAINS_MONO = {
  family: ["JetBrains Mono", "monospace"],
  label: "JetBrains Mono",
  url: "https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500;600;700;800&display=swap",
};

function descopeBaseUrl(projectId) {
  if (process.env.DESCOPE_BASE_URL) return process.env.DESCOPE_BASE_URL.replace(/\/$/, "");
  const region = projectId.slice(1, -27);
  return `https://api.${region ? `${region}.` : ""}descope.com`;
}

function descopeClient(projectId, managementKey) {
  const baseUrl = descopeBaseUrl(projectId);
  const headers = {
    Authorization: `Bearer ${projectId}:${managementKey}`,
    "Content-Type": "application/json",
    "x-descope-project-id": projectId,
  };

  return async function request(path, { method = "POST", body } = {}) {
    const response = await fetch(`${baseUrl}${path}`, {
      method,
      headers,
      body: body === undefined ? undefined : JSON.stringify(body),
    });
    const text = await response.text();
    const payload = text ? JSON.parse(text) : {};
    if (!response.ok) {
      const detail = payload?.errorDescription || payload?.errorMessage || `HTTP ${response.status}`;
      throw new Error(`Descope request failed: ${detail}`);
    }
    return payload;
  };
}

function brandTheme(theme) {
  const palette = {
    contrast: "#071008FF",
    dark: "#7FA91BFF",
    highlight: "#E4FF94FF",
    light: "#D6FF68FF",
    main: "#C8FF3DFF",
  };
  const secondary = {
    contrast: "#FFFFFFFF",
    dark: "#1546A0FF",
    highlight: "#B8D2FFFF",
    light: "#4D8DFFFF",
    main: "#2A6DFFFF",
  };
  const result = structuredClone(theme);
  const squareComponents = {
    alert: { "--descope-alert-border-radius": "0px" },
    button: { "--descope-button-border-radius": "0px" },
    code: { "--descope-code-input-border-radius": "0px" },
    emailField: { "--descope-email-field-input-border-radius": "0px" },
    inputWrapper: { "--descope-input-wrapper-border-radius": "0px" },
    tooltip: { "--descope-tooltip-border-radius": "0px" },
  };
  const squareRadii = {
    "2xl": "0px",
    "3xl": "0px",
    lg: "0px",
    md: "0px",
    sm: "0px",
    xl: "0px",
    xs: "0px",
  };
  result.codeMode = true;
  result.cssTemplate ||= {};
  for (const mode of ["light", "dark"]) {
    result.cssTemplate[mode] ||= {};
    result.cssTemplate[mode].globals ||= {};
    result.cssTemplate[mode].globals.colors ||= {};
    result.cssTemplate[mode].globals.colors.primary = palette;
    result.cssTemplate[mode].globals.colors.secondary = secondary;
    result.cssTemplate[mode].globals.radius = squareRadii;
    result.cssTemplate[mode].fonts = {
      font1: JETBRAINS_MONO,
      font2: JETBRAINS_MONO,
    };
    result.cssTemplate[mode].components = {
      ...(result.cssTemplate[mode].components || {}),
      ...squareComponents,
    };
  }
  return result;
}

function brandSnapshot(files, theme) {
  const stylesIndex = files["styles/styles.json"];
  if (!stylesIndex?.styles) throw new Error("Descope snapshot is missing its styles index");

  let changed = false;
  for (const mode of ["light", "dark"]) {
    const source = structuredClone(theme.cssTemplate[mode]);
    const fonts = source.fonts;
    delete source.fonts;
    source.globals ||= {};
    source.globals.fonts = fonts;
    const style = { ...source, name: "Recall", type: "flows" };
    const path = `styles/${STYLE_ID}-${mode}.json`;
    if (!isDeepStrictEqual(files[path], style)) {
      files[path] = style;
      changed = true;
    }

    const name = `${STYLE_ID}-${mode}`;
    if (!stylesIndex.styles.includes(name)) {
      stylesIndex.styles.push(name);
      changed = true;
    }
  }
  return changed;
}

function parseTemplate(screen) {
  return typeof screen.htmlTemplate === "string"
    ? JSON.parse(screen.htmlTemplate)
    : structuredClone(screen.htmlTemplate);
}

function saveTemplate(screen, template) {
  screen.htmlTemplate = typeof screen.htmlTemplate === "string"
    ? JSON.stringify(template)
    : template;
}

function textNode(id, parent, children, variant = "body2", align = "left") {
  return {
    custom: {},
    displayName: "Text",
    hidden: false,
    isCanvas: false,
    linkedNodes: {},
    nodes: [],
    parent,
    props: {
      children,
      "full-width": true,
      id,
      isEnriched: true,
      italic: false,
      "link-target-blank": true,
      mode: "primary",
      "text-align": align,
      variant,
    },
    type: { resolvedName: "Text" },
  };
}

function styleRoot(root, paddingY = "8") {
  Object.assign(root.props, {
    align: "stretch",
    background: "#0B100DFF",
    direction: "column",
    justify: "center",
    paddingX: "6",
    paddingY,
    spaceBetween: "5",
    width: "100%",
  });
}

function styleText(node, children, variant, align = "left") {
  Object.assign(node.props, {
    children,
    "full-width": true,
    "text-align": align,
    variant,
  });
}

function styleContainer(node, overrides = {}) {
  Object.assign(node.props, {
    background: "#ffffff00",
    paddingX: "0",
    paddingY: "0",
    ...overrides,
  });
}

function replaceLogoWithWordmark(node, parent = "ROOT") {
  Object.assign(node, textNode(node.props.id, parent, "RECALL //", "subtitle2", "left"));
}

function findEntry(template, predicate, label) {
  const match = Object.entries(template).find(([id, value]) => predicate(value, id));
  if (!match) throw new Error(`expected ${label} component is missing`);
  return match;
}

function hasType(template, resolvedName) {
  return Object.values(template).some((node) => node.type?.resolvedName === resolvedName);
}

function byType(resolvedName) {
  return (node) => node.type?.resolvedName === resolvedName;
}

function textContaining(fragment) {
  return (node) => node.type?.resolvedName === "Text" && node.props?.children?.includes(fragment);
}

function textContainingAny(...fragments) {
  return (node) => node.type?.resolvedName === "Text"
    && fragments.some((fragment) => node.props?.children?.includes(fragment));
}

function buttonLabeled(label) {
  return (node) => node.type?.resolvedName?.includes("Button") && node.props?.children === label;
}

function buttonLabeledAny(...labels) {
  return (node) => node.type?.resolvedName?.includes("Button") && labels.includes(node.props?.children);
}

function styleWelcome(screen) {
  const t = parseTemplate(screen);
  const [logoId, logo] = findEntry(
    t,
    (node) => byType("Logo")(node) || textContaining("RECALL //")(node),
    "logo",
  );
  const headlineEntry = Object.entries(t).find(([, node]) => byType("Text")(node)
    && (["h1", "h2", "h3"].includes(node.props?.variant) || textContaining("Welcome")(node)));
  const headlineId = headlineEntry?.[0];
  const [bodyId, body] = findEntry(t, textContainingAny("Privacy Statement", "shared engineering memory"), "welcome body");
  const [emailId, email] = findEntry(t, byType("EmailInput"), "email input");
  const [, continueButton] = findEntry(t, buttonLabeledAny("Continue", "Continue securely  →"), "continue button");
  const [dividerId, divider] = findEntry(t, byType("Divider"), "divider");
  const [googleId, google] = findEntry(t, byType("GoogleButton"), "Google button");
  const [microsoftId, microsoft] = findEntry(t, byType("MicrosoftButton"), "Microsoft button");
  const headerId = body.parent;
  const formId = email.parent;
  const socialId = google.parent;
  const header = t[headerId];
  const form = t[formId];
  const social = t[socialId];

  styleRoot(t.ROOT);
  t.ROOT.nodes = [logoId, headerId, formId];

  replaceLogoWithWordmark(logo);
  styleContainer(header, { align: "start", spaceBetween: "2" });
  header.nodes = ["recallEyebrow", bodyId];
  t.recallEyebrow = textNode("recallEyebrow", headerId, "PARCHA // COMPANY BRAIN", "subtitle2");
  styleText(body, "Connect your coding agent to Parcha's shared engineering memory.", "body1");
  if (headlineId) delete t[headlineId];
  delete t.recallTrust;

  styleContainer(form, { align: "stretch", direction: "column", spaceBetween: "3" });
  email.props.placeholder = "Work email";
  continueButton.props.children = "Continue securely  →";
  divider.props.children = "OR CONNECT WITH";
  styleContainer(social, { align: "stretch", direction: "column", spaceBetween: "2" });
  google.props.children = "Continue with Google";
  microsoft.props.children = "Continue with Microsoft";

  saveTemplate(screen, t);
}

function styleConsent(screen, { verified }) {
  const t = parseTemplate(screen);
  const appLogoEntry = Object.entries(t).find(([, node]) => [
    "InboundAppLogo",
    "ThirdPartyAppLogo",
  ].includes(node.type?.resolvedName));
  const headlineEntry = Object.entries(t).find(([, node]) => textContaining("{{mcpClient.name}}")(node));
  const requestedEntry = Object.entries(t).find(([, node]) => textContainingAny(
    "will be able",
    "Requested access",
  )(node));
  const scopesEntry = Object.entries(t).find(([, node]) => [
    "Scopes",
    "InboundAppScopes",
    "ScopesList",
  ].includes(node.type?.resolvedName));
  const [, authorize] = findEntry(t, buttonLabeledAny("Authorize", "Connect company brain  →"), "authorize button");
  const [, cancel] = findEntry(t, buttonLabeledAny("Cancel", "Not now"), "cancel button");
  const actionsContainerId = authorize.parent;
  const actionsContainer = t[actionsContainerId];
  let warningContainerId;
  let warningContainer;
  let warningHeadline;
  let warningBody;
  if (!verified) {
    [, warningHeadline] = findEntry(t, textContainingAny("Unverified Application", "NEW CLIENT CONNECTION"), "unverified warning");
    [, warningBody] = findEntry(
      t,
      textContainingAny("put your data at risk", "read-only access", "not been verified by Descope"),
      "unverified detail",
    );
    warningContainerId = warningHeadline.parent;
    warningContainer = t[warningContainerId];
  }

  styleRoot(t.ROOT, "7");
  t.ROOT.nodes = [
    "recallConsentBrand",
    "recallConsentEyebrow",
    "recallConsentBody",
    ...(verified ? [] : [warningContainerId]),
    actionsContainerId,
  ];
  t.recallConsentBrand = textNode("recallConsentBrand", "ROOT", "RECALL //", "subtitle2");
  t.recallConsentEyebrow = textNode(
    "recallConsentEyebrow",
    "ROOT",
    "ACCESS REVIEW // COMPANY BRAIN",
    "subtitle2",
  );
  for (const entry of [appLogoEntry, headlineEntry, requestedEntry, scopesEntry]) {
    if (!entry) continue;
    const [nodeId, node] = entry;
    const parentId = node.parent;
    delete t[nodeId];
    if (parentId && parentId !== "ROOT") delete t[parentId];
  }
  t.recallConsentBody = textNode(
    "recallConsentBody",
    "ROOT",
    "Connect your coding agent to Parcha's shared engineering history.",
    "body1",
    "center",
  );
  delete t.recallConsentTrust;
  styleContainer(actionsContainer, { align: "stretch", background: "#ffffff00", spaceBetween: "2" });
  authorize.props.children = "Connect company brain  →";
  cancel.props.children = "Not now";

  if (!verified) {
    styleContainer(warningContainer, {
      align: "start",
      background: "#312B10FF",
      paddingX: "3",
      paddingY: "3",
      spaceBetween: "1",
    });
    styleText(warningHeadline, "NEW CLIENT CONNECTION", "subtitle2");
    styleText(
      warningBody,
      "This client has not been verified by Descope.",
      "body2",
    );
  }

  saveTemplate(screen, t);
}

function styleOtp(screen) {
  const t = parseTemplate(screen);
  const [, code] = findEntry(
    t,
    (node) => ["OneTimeCode", "Code"].includes(node.type?.resolvedName),
    "one-time code input",
  );
  const [headlineId, headline] = findEntry(t, textContainingAny("Enter Code", "Check your inbox"), "OTP headline");
  const [messageId, message] = findEntry(t, textContaining("{{sentTo.maskedEmail}}"), "OTP message");
  const [, resend] = findEntry(t, buttonLabeledAny("Send again", "Send a new code"), "resend button");
  const [, anotherMethod] = findEntry(
    t,
    buttonLabeledAny("Choose another authentication method", "Use another sign-in method"),
    "alternate method button",
  );
  const formId = code.parent;
  const linksId = resend.parent;
  const form = t[formId];
  const links = t[linksId];

  styleRoot(t.ROOT, "8");
  t.ROOT.nodes = ["recallOtpBrand", "recallOtpEyebrow", headlineId, messageId, formId];
  t.recallOtpBrand = textNode("recallOtpBrand", "ROOT", "RECALL //", "subtitle2");
  t.recallOtpEyebrow = textNode("recallOtpEyebrow", "ROOT", "AUTH // VERIFY", "subtitle2");
  styleText(headline, "Check your inbox.", "h2");
  styleText(message, "Enter the six-digit code sent to {{sentTo.maskedEmail}}.", "body1");
  styleContainer(form, { align: "stretch", spaceBetween: "3" });
  styleContainer(links, { align: "start", direction: "column", spaceBetween: "1" });
  resend.props.children = "Send a new code";
  anotherMethod.props.children = "Use another sign-in method";
  saveTemplate(screen, t);
}

function brandScreens(screens) {
  const classified = { welcome: [], verified: [], unverified: [], otp: [] };
  for (const screen of screens) {
    const template = parseTemplate(screen);
    if (hasType(template, "EmailInput")) classified.welcome.push(screen);
    else if (hasType(template, "OneTimeCode") || hasType(template, "Code")) classified.otp.push(screen);
    else if (
      ["InboundAppScopes", "Scopes", "ScopesList"].some((name) => hasType(template, name))
      || Object.values(template).some(buttonLabeledAny("Authorize", "Connect company brain  →"))
    ) {
      const isUnverified = Object.values(template).some(
        textContainingAny("Unverified Application", "NEW CLIENT CONNECTION"),
      );
      classified[isUnverified ? "unverified" : "verified"].push(screen);
    }
  }
  for (const [kind, matches] of Object.entries(classified)) {
    if (matches.length !== 1) throw new Error(`expected exactly one ${kind} screen; found ${matches.length}`);
  }
  styleWelcome(classified.welcome[0]);
  styleConsent(classified.verified[0], { verified: true });
  styleConsent(classified.unverified[0], { verified: false });
  styleOtp(classified.otp[0]);
}

function hostedLoginUrl(value, styleId) {
  const url = new URL(value);
  url.searchParams.set("flow", FLOW_ID);
  if (styleId) url.searchParams.set("style", styleId);
  url.searchParams.set("theme", "dark");
  url.searchParams.set("bg", "#070A08");
  url.searchParams.set("width", "580px");
  url.searchParams.set("shadow", "false");
  return url.toString();
}

async function main() {
  const projectId = process.env.RECALL_DESCOPE_PROJECT_ID;
  const managementKey = process.env.RECALL_DESCOPE_MGMT_KEY;
  if (!projectId || !managementKey) throw new Error("missing Descope management environment");

  const request = descopeClient(projectId, managementKey);
  const exportedTheme = await request("/v1/mgmt/theme/export", { body: {} });
  const brandedTheme = brandTheme(exportedTheme.theme);
  const snapshot = await request("/v1/mgmt/project/snapshot/export", { body: {} });
  const snapshotChanged = brandSnapshot(snapshot.files, brandedTheme);
  if (snapshotChanged) {
    const validation = await request("/v1/mgmt/project/snapshot/validate", {
      body: { files: snapshot.files },
    });
    const missingSecrets = Object.values(validation.missingSecrets || {}).some(
      (secrets) => secrets?.length,
    );
    if (!validation.ok || validation.failures?.length || missingSecrets) {
      throw new Error("Descope rejected the branded style snapshot");
    }
    await request("/v1/mgmt/project/snapshot/import", { body: { files: snapshot.files } });
  }
  const exported = await request("/v1/mgmt/flow/export", { body: { flowId: FLOW_ID } });
  const screens = structuredClone(exported.screens);
  brandScreens(screens);
  const imported = await request("/v1/mgmt/flow/import", {
    body: { flowId: FLOW_ID, flow: exported.flow, screens },
  });

  const appData = await request("/v1/mgmt/thirdparty/apps/load", { method: "GET" });
  const apps = appData.apps || [];
  let patchedApps = 0;
  for (const app of apps) {
    if (!app.loginPageUrl?.includes(`flow=${FLOW_ID}`)) continue;
    const loginPageUrl = hostedLoginUrl(app.loginPageUrl, STYLE_ID);
    if (loginPageUrl === app.loginPageUrl) continue;
    await request("/v1/mgmt/thirdparty/app/patch", { body: { id: app.id, loginPageUrl } });
    patchedApps += 1;
  }

  console.log(JSON.stringify({
    flowId: FLOW_ID,
    flowVersion: imported.flow?.version,
    screens: imported.screens?.length,
    patchedApps,
    snapshotChanged,
  }));
}

if (require.main === module) {
  main().catch((error) => {
    console.error(error.message);
    process.exit(1);
  });
}

module.exports = {
  FLOW_ID,
  STYLE_ID,
  brandSnapshot,
  brandScreens,
  brandTheme,
  hostedLoginUrl,
};
