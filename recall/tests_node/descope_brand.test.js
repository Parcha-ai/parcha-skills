"use strict";

const assert = require("node:assert/strict");
const test = require("node:test");

const {
  brandScreens,
  brandTheme,
  hostedLoginUrl,
} = require("../server/scripts/configure_descope_brand.js");

function node(id, parent = "ROOT", props = {}) {
  return {
    custom: {},
    displayName: "Text",
    hidden: false,
    isCanvas: false,
    linkedNodes: {},
    nodes: [],
    parent,
    props: { id, ...props },
    type: { resolvedName: "Text" },
  };
}

function container(id, parent = "ROOT") {
  const result = node(id, parent, {
    align: "center",
    background: "",
    direction: "row",
    justify: "center",
    paddingX: "1",
    paddingY: "1",
    spaceBetween: "1",
    width: "100%",
  });
  result.displayName = "Container";
  result.type.resolvedName = "Container";
  return result;
}

function setType(item, resolvedName) {
  item.displayName = resolvedName;
  item.type.resolvedName = resolvedName;
  return item;
}

function welcomeTemplate() {
  const result = {
    ROOT: container("ROOT", undefined),
    U9D2pHfkJp: setType(node("U9D2pHfkJp"), "Logo"),
    FTAR1uG31j: container("FTAR1uG31j"),
    UTdxgyTBPh: node("UTdxgyTBPh", "FTAR1uG31j", { children: "Welcome!" }),
    GyGw_AJiyl: node("GyGw_AJiyl", "FTAR1uG31j", { children: "Privacy Statement" }),
    RP8Dj8wg6E: container("RP8Dj8wg6E"),
    Ek1mWE2Mag: setType(node("Ek1mWE2Mag", "RP8Dj8wg6E"), "EmailInput"),
    "3TVRPM-NqM": setType(node("3TVRPM-NqM", "RP8Dj8wg6E", { children: "Continue" }), "Button"),
    "2X1y3vdDA-": setType(node("2X1y3vdDA-", "RP8Dj8wg6E"), "Divider"),
    "jmGk9e-cOx": container("jmGk9e-cOx", "RP8Dj8wg6E"),
    FYf4hmIZgm: setType(node("FYf4hmIZgm", "jmGk9e-cOx"), "GoogleButton"),
    "0L1nCJrTJX": setType(node("0L1nCJrTJX", "jmGk9e-cOx"), "MicrosoftButton"),
  };
  result.ROOT.nodes = ["U9D2pHfkJp", "FTAR1uG31j", "RP8Dj8wg6E"];
  return result;
}

function consentTemplate({ verified }) {
  const result = {
    ROOT: container("ROOT", undefined),
    qIfDVKm2T2: container("qIfDVKm2T2"),
    ojc6ht: setType(node("ojc6ht", "qIfDVKm2T2"), "InboundAppLogo"),
    OHS3uAG0xM: container("OHS3uAG0xM"),
    LmrSIGOWZb: node("LmrSIGOWZb", "OHS3uAG0xM", { children: "Connect {{project.name}} with {{mcpClient.name}}" }),
    Zgp3CGaHAw: container("Zgp3CGaHAw"),
    _FBvzhJtOx: node("_FBvzhJtOx", "Zgp3CGaHAw", { children: "{{mcpClient.name}} will be able to:" }),
    "EQIG9X08-e": container("EQIG9X08-e"),
    scopeList: setType(node("scopeList", "EQIG9X08-e"), verified ? "Scopes" : "InboundAppScopes"),
    woAD652rBY: container("woAD652rBY"),
    _Z6xPaS9jy: setType(node("_Z6xPaS9jy", "woAD652rBY", { children: "Authorize" }), "Button"),
    "6N3cb_5t3T": setType(node("6N3cb_5t3T", "woAD652rBY", { children: "Cancel" }), "Button"),
  };
  if (!verified) {
    result.VqlMUvC3Yz = container("VqlMUvC3Yz");
    result.XGFJtHqtkn = node("XGFJtHqtkn", "VqlMUvC3Yz", { children: "Warning: Unverified Application" });
    result.NpV0qhppf_ = node("NpV0qhppf_", "VqlMUvC3Yz", { children: "Using this application may put your data at risk" });
  }
  return result;
}

function otpTemplate() {
  return {
    ROOT: container("ROOT", undefined),
    eBGiu9cYSb: node("eBGiu9cYSb", "ROOT", { children: "Enter Code" }),
    auED4dEJkJ: node("auED4dEJkJ", "ROOT", { children: "Code sent to {{sentTo.maskedEmail}}" }),
    yZNZ2ZVVu_: container("yZNZ2ZVVu_"),
    code: setType(node("code", "yZNZ2ZVVu_"), "OneTimeCode"),
    oOOUcyEOVr: container("oOOUcyEOVr", "yZNZ2ZVVu_"),
    resend: setType(node("resend", "oOOUcyEOVr", { children: "Send again" }), "Button"),
    pnOqEGpL1b: setType(node("pnOqEGpL1b", "oOOUcyEOVr", { children: "Choose another authentication method" }), "Button"),
  };
}

test("brandTheme applies the Recall palette without discarding theme identity", () => {
  const branded = brandTheme({ id: "theme-1", cssTemplate: { light: {}, dark: {} } });
  assert.equal(branded.id, "theme-1");
  assert.equal(branded.codeMode, true);
  assert.equal(branded.cssTemplate.dark.globals.colors.primary.main, "#C8FF3DFF");
  assert.equal(branded.cssTemplate.light.globals.colors.primary.contrast, "#071008FF");
});

test("hostedLoginUrl keeps the app URL and selects Recall's managed presentation", () => {
  const url = new URL(hostedLoginUrl("https://api.descope.com/login/P123?flow=old&tenant=parcha"));
  assert.equal(url.origin, "https://api.descope.com");
  assert.equal(url.searchParams.get("flow"), "recall-mcp-user-consent");
  assert.equal(url.searchParams.get("tenant"), "parcha");
  assert.equal(url.searchParams.get("theme"), "dark");
  assert.equal(url.searchParams.get("bg"), "#070A08");
  assert.equal(url.searchParams.get("width"), "580px");
  assert.equal(url.searchParams.get("shadow"), "false");
});

test("brandScreens changes presentation while preserving flow interactions", () => {
  const interactions = [{ id: "submit", componentId: "3TVRPM-NqM" }];
  const screens = [
    { id: "welcome-random-id", htmlTemplate: JSON.stringify(welcomeTemplate()), interactions },
    { id: "verified-random-id", htmlTemplate: consentTemplate({ verified: true }), interactions },
    { id: "unverified-random-id", htmlTemplate: consentTemplate({ verified: false }), interactions },
    { id: "otp-random-id", htmlTemplate: otpTemplate(), interactions },
  ];

  brandScreens(screens);

  const welcome = JSON.parse(screens[0].htmlTemplate);
  assert.equal(welcome.UTdxgyTBPh.props.children, "Find the work. Keep the context.");
  assert.equal(welcome["3TVRPM-NqM"].props.children, "Continue securely  →");
  assert.equal(welcome.U9D2pHfkJp.type.resolvedName, "Text");
  assert.deepEqual(screens[0].interactions, interactions);

  const consent = screens[2].htmlTemplate;
  assert.equal(consent._Z6xPaS9jy.props.id, "_Z6xPaS9jy");
  assert.match(consent.NpV0qhppf_.props.children, /read-only access/);
  assert.deepEqual(screens[2].interactions, interactions);

  const otp = screens[3].htmlTemplate;
  assert.equal(otp.eBGiu9cYSb.props.children, "Check your inbox.");
  assert.equal(otp.resend.props.children, "Send a new code");
});
