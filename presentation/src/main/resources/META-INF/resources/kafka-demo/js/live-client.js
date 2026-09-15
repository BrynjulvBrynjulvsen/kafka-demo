// One connection per deck; navigation never changes it.
export class LiveClient extends EventTarget {
  constructor() { super(); this.generation = 0; }
  emit(type, detail) { this.dispatchEvent(new CustomEvent(type, { detail })); }
  connectChannel({ path, channel, connectingText = 'Connecting…', liveText = 'Observation stream connected' }) {
    this.disconnect();
    const generation = this.generation;
    let attempts = 0;
    const open = () => {
      if (generation !== this.generation) return;
      this.emit('status', { state: 'connecting', text: connectingText });
      const url = new URL(path, location.href);
      url.protocol = location.protocol === 'https:' ? 'wss:' : 'ws:';
      const socket = this.socket = new WebSocket(url);
      socket.onmessage = ({ data }) => {
        if (generation !== this.generation) return;
        let event;
        try { event = JSON.parse(data); } catch { return; }
        if (!event || event.version !== 1 || (channel !== undefined && event.topic !== channel)) return;
        if (event.type === 'subscribed') {
          attempts = 0;
          this.emit('status', { state: 'live', text: liveText });
        } else this.emit('observation', event);
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
  disconnect() { this.generation++; clearTimeout(this.timer); this.socket?.close(); }
}
