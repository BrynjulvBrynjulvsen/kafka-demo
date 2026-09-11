import { mountMigrationFlow } from './concepts/migration-flow.js';
import { LiveClient } from './live-client.js';
import { mountMigrationTopology, mountMigrationClients, mountMigrationEvents } from './concepts/migration-views.js';

const registry = { flow: mountMigrationFlow, topology: mountMigrationTopology, clients: mountMigrationClients, events: mountMigrationEvents };
const views = [...document.querySelectorAll('[data-migration-concept]')].map(root => registry[root.dataset.migrationConcept](root));
const client = new LiveClient();
const connection = document.querySelector('#connection');
let snapshot; let connected = false; let receivedAt = 0;
function render() {
  const now = Date.now();
  const current = connected && now - receivedAt < 5000;
  for (const view of views) {
    try { view.onMigration(snapshot, { now, connected: current }); }
    catch (error) { console.error('Migration view failed', error); }
  }
  if (snapshot?.enabled && connected && receivedAt && !current) {
    connection.textContent = 'Snapshot stream stale · last state retained'; connection.dataset.state = 'error';
  }
}
client.addEventListener('status', ({ detail }) => {
  connected = detail.state === 'live'; connection.textContent = detail.text; connection.dataset.state = detail.state; render();
});
client.addEventListener('migration', ({ detail }) => {
  snapshot = detail; receivedAt = Date.now();
  connection.textContent = detail.enabled ? 'Migration observation stream connected' : 'Migration observer disabled';
  connection.dataset.state = detail.enabled ? 'live' : 'error';
  document.querySelector('[data-role="environment"]').textContent = `${detail.context || 'Context unset'} / ${detail.namespace} · read-only`;
  render();
});
document.querySelector('#reconnect').addEventListener('click', () => client.connectMigration());
let timer;
function start() { clearInterval(timer); timer = setInterval(render, 1000); client.connectMigration(); }
window.addEventListener('pagehide', () => { clearInterval(timer); client.disconnect(); });
window.addEventListener('pageshow', event => { if (event.persisted) start(); });
start();
const { default: Reveal } = await import('../vendor/reveal/reveal.esm.js');
await new Reveal({ width: 1200, height: 700, margin: 0.12, hash: true, transition: 'fade', center: true,
  keyboardCondition: event => !event.target.closest('input, textarea, select, button, summary'),
}).initialize();
