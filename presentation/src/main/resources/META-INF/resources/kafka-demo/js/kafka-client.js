import { LiveClient } from './live-client.js';
export class KafkaClient extends LiveClient {
  constructor() {
    super();
    this.addEventListener('observation', ({ detail: event }) => {
      if (event.type === 'record-consumed' && Number.isInteger(event.partition)
        && Number.isSafeInteger(event.offset) && (event.key === null || typeof event.key === 'string')
        && (event.value === null || typeof event.value === 'string')) this.emit('record', event);
    });
  }
  connect(topic) {
    this.topic = topic;
    this.connectChannel({ path: `/ws/topics/${encodeURIComponent(topic)}`, channel: topic,
      connectingText: `Connecting to ${topic}…`, liveText: `Subscribed · ${topic} · no replay` });
  }
  async topics() {
    const response = await fetch('/api/topics', { signal: AbortSignal.timeout(8000) });
    if (!response.ok) throw new Error(`Topic discovery failed (HTTP ${response.status}).`);
    return response.json();
  }
  async produce(message) {
    let response;
    try {
      response = await fetch('/api/messages', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(message), signal: AbortSignal.timeout(15000),
      });
    } catch { throw new Error('No acknowledgment received. The write may have succeeded; retrying can duplicate it.'); }
    if (!response.ok) throw new Error(response.status >= 500
      ? `HTTP ${response.status}: no acknowledgment. The write may have succeeded; retrying can duplicate it.`
      : `HTTP ${response.status}: request rejected. Check the topic and input limits.`);
    return response.json();
  }
}
