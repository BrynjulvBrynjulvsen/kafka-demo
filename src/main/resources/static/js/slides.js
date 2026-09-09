import { LiveClient } from './live-client.js';
import { mountPartitioning } from './concepts/partitioning.js';

const client = new LiveClient();
const concepts = { partitioning: mountPartitioning };
const mounted = [...document.querySelectorAll('[data-concept]')].map(root => concepts[root.dataset.concept](root));
const topic = document.querySelector('#topic');
const send = document.querySelector('#send');
const status = document.querySelector('#produce-status');
const connection = document.querySelector('#connection');
let busy = false;
client.addEventListener('status', ({ detail }) => {
  connection.textContent = detail.text; connection.dataset.state = detail.state;
});
client.addEventListener('record', ({ detail }) => mounted.forEach(concept => concept.onRecord(detail)));

function selectTopic() {
  mounted.forEach(concept => concept.reset());
  status.textContent = 'Ready to produce. Subscription does not confirm Kafka consumer assignment.';
  client.connect(topic.value);
}
async function discover() {
  try {
    const config = await client.topics();
    topic.replaceChildren(...config.topics.map(name => new Option(name, name)));
    topic.value = config.defaultTopic; topic.disabled = false; send.disabled = false;
    selectTopic();
  } catch (error) {
    status.textContent = `${error.message} Use Reconnect stream to retry.`;
    connection.textContent = 'Backend unavailable'; connection.dataset.state = 'error';
  }
}
topic.addEventListener('change', selectTopic);
document.querySelector('#reconnect').addEventListener('click', () => topic.value ? client.connect(topic.value) : discover());
document.querySelector('#producer').addEventListener('submit', async event => {
  event.preventDefault();
  if (busy || !topic.value) return;
  busy = true; send.disabled = true; topic.disabled = true;
  status.textContent = 'Waiting for Kafka acknowledgment…';
  try {
    const result = await client.produce({ topic: topic.value,
      key: document.querySelector('#key').value || null, value: document.querySelector('#value').value });
    status.textContent = `Acknowledged · ${result.topic} · partition ${result.partition} · offset ${result.offset}. Watch for the separate consumed observation.`;
  } catch (error) { status.textContent = error.message; }
  finally { busy = false; send.disabled = false; topic.disabled = false; }
});
window.addEventListener('pagehide', () => client.disconnect());
window.addEventListener('pageshow', event => { if (event.persisted && topic.value) client.connect(topic.value); });
void discover();
try {
  const { default: Reveal } = await import('../vendor/reveal/reveal.esm.js');
  const deck = new Reveal({ width: 1200, height: 700, margin: 0.12, hash: true,
    transition: 'fade', controls: true, progress: true, center: true,
    keyboardCondition: event => !event.target.closest('input, textarea, select, button'),
  });
  await deck.initialize();
} catch {
  connection.textContent = 'Slides could not load reveal.js. Run npm ci and npm run vendor, then rebuild.';
  document.querySelector('.reveal').style.overflow = 'auto';
}
