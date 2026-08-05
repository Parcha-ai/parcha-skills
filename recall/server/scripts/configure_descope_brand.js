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

const FLOW_ID = "recall-mcp-user-consent";

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
    const payload = await response.json();
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
    result.cssTemplate[mode].components = {
      ...(result.cssTemplate[mode].components || {}),
      ...squareComponents,
    };
  }
  return result;
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
  const [headlineId, headline] = findEntry(t, textContainingAny("Welcome", "Find the work"), "welcome headline");
  const [bodyId, body] = findEntry(t, textContainingAny("Privacy Statement", "shared engineering memory"), "welcome body");
  const [emailId, email] = findEntry(t, byType("EmailInput"), "email input");
  const [, continueButton] = findEntry(t, buttonLabeledAny("Continue", "Continue securely  →"), "continue button");
  const [dividerId, divider] = findEntry(t, byType("Divider"), "divider");
  const [googleId, google] = findEntry(t, byType("GoogleButton"), "Google button");
  const [microsoftId, microsoft] = findEntry(t, byType("MicrosoftButton"), "Microsoft button");
  const headerId = headline.parent;
  const formId = email.parent;
  const socialId = google.parent;
  const header = t[headerId];
  const form = t[formId];
  const social = t[socialId];

  styleRoot(t.ROOT);
  t.ROOT.nodes = [logoId, headerId, formId];

  replaceLogoWithWordmark(logo);
  styleContainer(header, { align: "start", spaceBetween: "2" });
  header.nodes = ["recallEyebrow", headlineId, bodyId, "recallTrust"];
  t.recallEyebrow = textNode("recallEyebrow", headerId, "PARCHA // COMPANY BRAIN", "subtitle2");
  styleText(headline, "Find the work. Keep the context.", "h2");
  styleText(body, "Connect your coding agent to Parcha's shared engineering memory.", "body1");
  t.recallTrust = textNode(
    "recallTrust",
    headerId,
    "READ ONLY  ·  COMPANY SCOPE  ·  REVOCABLE",
    "body2",
  );

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
  const [, appLogo] = findEntry(
    t,
    (node) => ["InboundAppLogo", "ThirdPartyAppLogo"].includes(node.type?.resolvedName),
    "inbound app logo",
  );
  const [headlineId, headline] = findEntry(t, textContaining("{{mcpClient.name}}"), "consent headline");
  const [, requested] = findEntry(t, textContainingAny("will be able", "Requested access"), "scope heading");
  const [scopesId, scopes] = findEntry(
    t,
    (node) => ["Scopes", "InboundAppScopes", "ScopesList"].includes(node.type?.resolvedName),
    "scopes",
  );
  const [, authorize] = findEntry(t, buttonLabeledAny("Authorize", "Connect company brain  →"), "authorize button");
  const [, cancel] = findEntry(t, buttonLabeledAny("Cancel", "Not now"), "cancel button");
  const logoContainerId = appLogo.parent;
  const headlineContainerId = headline.parent;
  const requestedContainerId = requested.parent;
  const scopesContainerId = scopes.parent;
  const actionsContainerId = authorize.parent;
  const logoContainer = t[logoContainerId];
  const headlineContainer = t[headlineContainerId];
  const requestedContainer = t[requestedContainerId];
  const scopesContainer = t[scopesContainerId];
  const actionsContainer = t[actionsContainerId];
  let warningContainerId;
  let warningContainer;
  let warningHeadline;
  let warningBody;
  if (!verified) {
    [, warningHeadline] = findEntry(t, textContainingAny("Unverified Application", "NEW CLIENT CONNECTION"), "unverified warning");
    [, warningBody] = findEntry(t, textContainingAny("put your data at risk", "read-only access"), "unverified detail");
    warningContainerId = warningHeadline.parent;
    warningContainer = t[warningContainerId];
  }

  styleRoot(t.ROOT, "7");
  t.ROOT.nodes = [
    "recallConsentBrand",
    "recallConsentEyebrow",
    logoContainerId,
    headlineContainerId,
    "recallConsentBody",
    "recallConsentTrust",
    requestedContainerId,
    scopesContainerId,
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
  styleContainer(logoContainer, { align: "center", justify: "center" });
  styleContainer(headlineContainer, { align: "center", justify: "center" });
  styleText(headline, "Connect {{mcpClient.name}}", "h3", "center");
  t.recallConsentBody = textNode(
    "recallConsentBody",
    "ROOT",
    "Search Parcha's shared engineering history from your coding agent.",
    "body1",
    "center",
  );
  t.recallConsentTrust = textNode(
    "recallConsentTrust",
    "ROOT",
    "READ ONLY  ·  COMPANY BRAIN  ·  REVOKE ANYTIME",
    "body2",
    "center",
  );
  styleContainer(requestedContainer, { align: "start" });
  styleText(requested, "Requested access", "subtitle2");
  styleContainer(scopesContainer, {
    align: "stretch",
    background: "#151C17FF",
    paddingX: "3",
    paddingY: "3",
  });
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
      "Descope has not verified this client. Recall still enforces read-only access to the Parcha company brain.",
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
    else if (["InboundAppScopes", "Scopes", "ScopesList"].some((name) => hasType(template, name))) {
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

function hostedLoginUrl(value) {
  const url = new URL(value);
  url.searchParams.set("flow", FLOW_ID);
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
  const importedTheme = await request("/v1/mgmt/theme/import", {
    body: { theme: brandTheme(exportedTheme.theme) },
  });
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
    const loginPageUrl = hostedLoginUrl(app.loginPageUrl);
    if (loginPageUrl === app.loginPageUrl) continue;
    await request("/v1/mgmt/thirdparty/app/patch", { body: { id: app.id, loginPageUrl } });
    patchedApps += 1;
  }

  console.log(JSON.stringify({
    flowId: FLOW_ID,
    flowVersion: imported.flow?.version,
    screens: imported.screens?.length,
    patchedApps,
    themeId: importedTheme.theme?.id,
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
  brandScreens,
  brandTheme,
  hostedLoginUrl,
};
