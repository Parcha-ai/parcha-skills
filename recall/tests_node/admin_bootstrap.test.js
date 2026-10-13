'use strict';
const assert = require('node:assert/strict');
const test = require('node:test');
const fs = require('node:fs');
const vm = require('node:vm');
const path = require('node:path');
const root = path.join(__dirname, '../server/recall_server/static');
const html = fs.readFileSync(path.join(root, 'admin.html'), 'utf8');
const script = fs.readFileSync(path.join(root, 'admin.js'), 'utf8');
const flush = () => new Promise(resolve => setImmediate(resolve));
const response = (body, status = 200) => ({ ok: status < 400, status, json: async () => body });
const valid = () => ({ brains: [{ tenant_id: 'tenant:company:example', display_name: 'Example', brain_kind: 'company', permission: 'owner', slug: 'example' }], providers: [{ id: 'slack' }], connections: [], catalog: [], installations: [], invitations: [] });

// Thin DOM seam: execute the unmodified browser script and its actual handlers.
// The guarded real-browser companion verifies native fieldset/dialog behavior.
function harness(fetcher) {
  class Element {
    constructor() { this.listeners = {}; this.children = []; this.elements = []; this.dataset = {}; this.hidden = false; this.disabled = false; this.open = false; this.value = ''; this.checked = true; this.textContent = ''; this.classes = new Set(); this.classList = { add: x => this.classes.add(x), remove: x => this.classes.delete(x), toggle: (x, on) => on ? this.classes.add(x) : this.classes.delete(x) }; }
    set innerHTML(value) { this.html = value; const option = value.match(/<option value="([^"]*)"/); if (option) this.value = option[1]; }
    get innerHTML() { return this.html || this.textContent; }
    addEventListener(name, handler) { this.listeners[name] = handler; }
    replaceChildren(...children) { this.children = children; if (children[0]?.value) this.value = children[0].value; }
    append(...children) { this.children.push(...children); }
    setAttribute(name, value) { this[name] = String(value); }
    removeAttribute(name) { delete this[name]; }
    showModal() { if (this.open) throw new Error('dialog already open'); this.open = true; }
    close() { this.open = false; }
    focus() {}
    querySelector() { return new Element(); }
  }
  const elements = new Map();
  for (const match of html.matchAll(/<[^>]+\bid="([^"]+)"[^>]*>/g)) { const element = new Element(); element.disabled = /\bdisabled\b/.test(match[0]); elements.set('#' + match[1], element); }
  elements.set('.pulse', new Element());
  for (const selector of ['#google-form button[type=submit]', '#slack-form button[type=submit]']) elements.set(selector, new Element());
  const calls = [], timers = new Map(); let nextTimer = 0;
  const document = { cookie: 'recall_admin_csrf=synthetic', querySelector: selector => elements.get(selector), querySelectorAll: () => [], createElement: () => new Element() };
  const window = { location: { origin: 'https://recall.example', search: '', assign: value => { window.redirect = value; } }, confirm: () => true,
    setTimeout: handler => { timers.set(++nextTimer, handler); return nextTimer; }, clearTimeout: id => timers.delete(id) };
  const context = vm.createContext({ document, window, navigator: {}, history: { replaceState() {} }, URLSearchParams, AbortController,
    setTimeout: window.setTimeout, clearTimeout: window.clearTimeout,
    fetch: async (url, options) => { calls.push({ url, options }); return fetcher(url, options); }, console });
  vm.runInContext(script, context);
  return { elements, calls, timers, window, context, async fire(selector, name = 'click') { const handler = elements.get(selector)?.listeners[name]; assert.ok(handler, `missing ${selector} ${name}`); await handler({ preventDefault() {}, target: elements.get(selector) }); await flush(); } };
}

test('slow state disables controls and blocks the actual Slack submit handler', async () => {
  const app = harness(url => url.endsWith('auth-methods') ? response({ oauth: true }) : new Promise(() => {}));
  await flush();
  assert.equal(app.elements.get('#admin-controls')?.disabled, true);
  await app.fire('#slack-form', 'submit');
  assert.equal(app.calls.filter(x => x.options.method === 'POST').length, 0);
  assert.match(app.elements.get('#bootstrap-message')?.textContent || '', /loading/i);
});

test('Slack submit cannot throw or write before bootstrap completes', async () => {
  const app = harness(url => url.endsWith('auth-methods') ? response({ oauth: true }) : new Promise(() => {}));
  await flush();
  await assert.doesNotReject(() => app.fire('#slack-form', 'submit'));
  assert.equal(app.calls.filter(x => x.options.method === 'POST').length, 0);
});

test('failed state persists, retries and preserves the exact authorized Slack request', async () => {
  let fail = true;
  const app = harness(url => url.endsWith('auth-methods') ? response({ oauth: true }) : url.endsWith('oauth/start') ? response({ authorization_url: 'https://slack.example/authorize' }) : fail ? response({ error: 'state_unavailable' }, 500) : response(valid()));
  await flush();
  assert.match(app.elements.get('#bootstrap-message')?.textContent || '', /state_unavailable/);
  assert.equal(app.elements.get('#admin-controls')?.disabled, true);
  fail = false; await app.fire('#bootstrap-retry');
  assert.equal(app.elements.get('#admin-controls').disabled, false);
  assert.equal(app.elements.get('#bootstrap-status').hidden, true);
  await app.fire('#slack-form', 'submit');
  const sent = app.calls.find(x => x.url.endsWith('oauth/start'));
  assert.deepEqual(JSON.parse(sent.options.body), { provider: 'slack', routes: [{ connector_id: 'slack.messages', tenant_id: 'tenant:company:example', privacy_mode: 'scrub', selectors: { channel_ids: [], owner_user_ids: [] } }] });
  assert.equal(sent.options.headers['X-Recall-CSRF'], 'synthetic');
  assert.equal(app.window.redirect, 'https://slack.example/authorize');
});

test('stalled auth discovery does not hold a valid state load', async () => {
  const app = harness(url => url.endsWith('auth-methods') ? new Promise(() => {}) : response(valid()));
  await flush();
  assert.equal(app.elements.get('#admin-controls')?.disabled, false);
  assert.ok(app.calls.some(x => x.url.endsWith('/state')));
});

test('401 persists after modal cancel and sign-in can reopen it', async () => {
  const app = harness(url => url.endsWith('auth-methods') ? response({ oauth: true }) : response({ error: 'authentication_required' }, 401));
  await flush();
  assert.equal(app.elements.get('#auth-dialog').open, true);
  app.elements.get('#auth-dialog').close();
  assert.match(app.elements.get('#bootstrap-message')?.textContent || '', /sign in/i);
  await app.fire('#bootstrap-retry');
  assert.equal(app.elements.get('#auth-dialog').open, true);
  assert.equal(app.elements.get('#admin-controls').disabled, true);
});

for (const [name, fetchState] of [
  ['null', () => response(null)], ['malformed', () => response({ brains: [] })],
  ['non-JSON', () => ({ ok: false, status: 502, json: async () => { throw new SyntaxError('unexpected HTML'); } })],
  ['network', () => Promise.reject(new TypeError('Failed to fetch'))],
]) test(`${name} bootstrap never enables actions and leaves a persistent retry`, async () => {
  const app = harness(url => url.endsWith('auth-methods') ? response({ oauth: true }) : fetchState());
  await flush();
  assert.equal(app.elements.get('#admin-controls')?.disabled, true);
  assert.ok(app.elements.get('#bootstrap-message')?.textContent);
  assert.equal(app.elements.get('#bootstrap-retry')?.disabled, false);
  await app.fire('#slack-form', 'submit');
  assert.equal(app.calls.filter(x => x.options.method === 'POST').length, 0);
});

test('failed refresh invalidates previously ready controls', async () => {
  let fail = false;
  const app = harness(url => url.endsWith('auth-methods') ? response({ oauth: true }) : fail ? response({ error: 'refresh_failed' }, 503) : response(valid()));
  await flush(); fail = true;
  await vm.runInContext('load()', app.context);
  assert.equal(app.elements.get('#admin-controls')?.disabled, true);
  assert.equal(app.elements.get('.pulse').classes.has('ready'), false);
  await app.fire('#slack-form', 'submit');
  assert.equal(app.calls.filter(x => x.options.method === 'POST').length, 0);
});

test('hung bootstrap times out and exposes a usable retry', async () => {
  const app = harness((url, options) => url.endsWith('auth-methods') ? response({ oauth: true }) : new Promise((resolve, reject) => options.signal?.addEventListener('abort', () => reject(new Error('aborted')))));
  await flush();
  for (const timer of [...app.timers.values()]) timer();
  await flush();
  assert.match(app.elements.get('#bootstrap-message')?.textContent || '', /timed out/i);
  assert.equal(app.elements.get('#bootstrap-retry')?.disabled, false);
});

test('fleet503 keeps Slack OAuth usable and is independently retryable', async () => {
  let failFleet = true;
  const app = harness(url => url.endsWith('auth-methods') ? response({ oauth: true }) : url.endsWith('oauth/start') ? response({ authorization_url: 'https://slack.example/authorize' }) : url.endsWith('/fleet') ? failFleet ? response({ error: 'fleet_unavailable' }, 503) : response({ fleet: [] }) : response(valid()));
  await flush();
  assert.equal(app.elements.get('#admin-controls').disabled, false);
  assert.match(app.elements.get('#fleet-message').textContent, /fleet_unavailable/);
  assert.equal(app.elements.get('#fleet-status').hidden, false);
  await app.fire('#slack-form', 'submit');
  assert.equal(app.calls.filter(x => x.url.endsWith('oauth/start')).length, 1);
  assert.equal(app.window.redirect, 'https://slack.example/authorize');
  const stateCalls = app.calls.filter(x => x.url.endsWith('/state')).length;
  failFleet = false; await app.fire('#fleet-retry');
  assert.equal(app.elements.get('#fleet-status').hidden, true);
  assert.equal(app.calls.filter(x => x.url.endsWith('/state')).length, stateCalls);
});

test('double retry deduplicates state requests and stale fleet cannot overwrite a refresh', async () => {
  let resolveState, resolveFleet, stateCalls = 0, fleetCalls = 0;
  const app = harness(url => {
    if (url.endsWith('auth-methods')) return response({ oauth: true });
    if (url.endsWith('/fleet')) { fleetCalls++; return fleetCalls === 1 ? new Promise(resolve => { resolveFleet = resolve; }) : response({ fleet: [] }); }
    stateCalls++; return stateCalls === 1 ? new Promise(resolve => { resolveState = resolve; }) : response(valid());
  });
  await flush();
  const retryOne = vm.runInContext('load()', app.context), retryTwo = vm.runInContext('load()', app.context);
  assert.equal(stateCalls, 1);
  resolveState(response(valid())); await Promise.all([retryOne, retryTwo]); await flush();
  await vm.runInContext('load()', app.context); await flush();
  assert.equal(app.elements.get('#fleet-status').hidden, true);
  resolveFleet(response({ error: 'obsolete_fleet_failure' }, 503)); await flush();
  assert.equal(app.elements.get('#fleet-status').hidden, true);
  assert.equal(app.elements.get('#admin-controls').disabled, false);
});


test('bootstrap read timeout does not introduce ambiguous mutation retries', async () => {
  let finishWrite;
  const app = harness(url => url.endsWith('auth-methods') ? response({ oauth: true }) : url.endsWith('oauth/start') ? new Promise(resolve => { finishWrite = resolve; }) : url.endsWith('/fleet') ? response({ fleet: [] }) : response(valid()));
  await flush();
  const write = vm.runInContext('api("/admin/api/v1/oauth/start", {method:"POST"})', app.context);
  await flush();
  assert.equal(app.timers.size, 0, 'POST must not receive the bootstrap GET deadline');
  finishWrite(response({ authorization_url: 'https://slack.example/authorize' }));
  await write;
});
