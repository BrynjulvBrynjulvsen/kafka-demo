// A single connection for the whole deck. Slide navigation never changes it.
export class LiveClient extends EventTarget {
  constructor() { super(); this.generation = 0; }
  emit(type, detail) { this.dispatchEvent(new CustomEvent(type, { detail })); }
  connect(topic) {
    this.disconnect();
    const generation = this.generation;
    this.topic = topic;
    let attempts = 0;
    let lastExperiment; let receivedAt = 0;
    this.freshnessTimer = setInterval(() => {
      if (generation === this.generation && lastExperiment && Date.now() - receivedAt > 5000) {
        this.emit('experiment', { ...lastExperiment, at: Date.now(), error: 'STALE · no experiment update for more than 5 seconds' });
      }
    }, 1000);
    const open = () => {
      if (generation !== this.generation) return;
      this.emit('status', { state: 'connecting', text: `Connecting to ${topic}…` });
      const url = new URL(`/ws/topics/${encodeURIComponent(topic)}`, location.href);
      url.protocol = location.protocol === 'https:' ? 'wss:' : 'ws:';
      const socket = this.socket = new WebSocket(url);
      socket.onmessage = ({ data }) => {
        if (generation !== this.generation) return;
        let event;
        try { event = JSON.parse(data); } catch { return; }
        if (event.version !== 1 || event.topic !== topic) return;
        if (event.type === 'subscribed') {
          attempts = 0;
          this.emit('status', { state: 'live', text: `Subscribed · ${topic} · no replay` });
        } else if (event.type === 'experiment-snapshot' && Array.isArray(event.members) && Array.isArray(event.offsets)) {
          lastExperiment = event; receivedAt = Date.now();
          this.emit('experiment', event);
        } else if (event.type === 'record-consumed' && Number.isInteger(event.partition)
          && Number.isSafeInteger(event.offset) && (event.key === null || typeof event.key === 'string')
          && (event.value === null || typeof event.value === 'string')) {
          this.emit('record', event);
        }
      };
      socket.onclose = ({ code }) => {
        if (generation !== this.generation) return;
        const delay = Math.min(1000 * 2 ** attempts++, 15000);
        this.emit('status', { state: 'error', text: `Stream closed (${code}) · retry in ${delay / 1000}s · observations may be missed` });
        this.timer = setTimeout(open, delay);
      };
      socket.onerror = () => socket.close();
    };
    open();
  }
  disconnect() { this.generation++; clearTimeout(this.timer); clearInterval(this.freshnessTimer); this.socket?.close(); }
  async experiment(command) {
    const response = await fetch('/api/experiment', {
      method: command ? 'POST' : 'GET', headers: { 'Content-Type': 'application/json' },
      ...(command ? { body: JSON.stringify(command) } : {}), signal: AbortSignal.timeout(25000),
    });
    const result = await response.json();
    if (!response.ok) throw new Error(result.message || `Command failed (HTTP ${response.status}); inspect current state before retrying.`);
    return result;
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
