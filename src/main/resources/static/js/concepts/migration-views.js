function el(tag, text, className) {
  const node = document.createElement(tag);
  if (text !== undefined) node.textContent = String(text);
  if (className) node.className = className;
  return node;
}
const age = (at, now) => at == null ? 'never observed' : `${Math.max(0, Math.floor((now - at) / 1000))}s ago`;
const fresh = (source, now, connected) => connected && source?.at != null && !source.error && now - source.at < 15000;
export function sources(root, snapshot, now, connected) {
  const host = root.querySelector('[data-role="sources"]');
  host.replaceChildren(...['kubernetes', 'telemetry'].map(name => {
    const source = snapshot?.[name];
    const ok = snapshot?.enabled && fresh(source, now, connected);
    const detail = !snapshot?.enabled ? 'disabled · start with migration profile' :
      !connected ? 'stream stale' : source?.error || (ok ? 'observing' : 'stale');
    const badge = el('span', `${name === 'kubernetes' ? 'Kubernetes' : 'Telemetry'} · ${detail} · ${age(source?.at, now)}`, 'source-badge');
    badge.dataset.fresh = String(Boolean(ok)); badge.title = badge.textContent;
    return badge;
  }));
}
function empty(host, text) { host.replaceChildren(el('p', text, 'empty')); }

export function mountMigrationTopology(root) {
  const find = role => root.querySelector(`[data-role="${role}"]`);
  return {
    onMigration(snapshot, { now, connected }) {
      sources(root, snapshot, now, connected);
      const infra = snapshot?.infrastructure || {};
      const current = snapshot?.enabled && fresh(snapshot.kubernetes, now, connected);
      const variant = infra.selectedVariant;
      const proxies = infra.proxies || [];
      const selected = proxies.find(p => p.selected);
      const upstream = selected?.config?.upstream;
      find('route').textContent = !current ? 'Route unknown / stale' : variant || 'No selected variant';
      find('endpoints').textContent = `${current ? '' : 'Last known: '}${infra.readyEndpointCount ?? '?'} ready endpoints`;
      const clients = infra.clients || [];
      find('fleet').textContent = clients.length ? `${clients.reduce((n, c) => n + c.ready, 0)} / ${clients.reduce((n, c) => n + c.desired, 0)} pods ready${current ? '' : ' (last known)'}` : 'No workloads observed';
      for (const [name, host] of [['legacy', 'legacy-broker.'], ['target', 'kafkabroker.']]) {
        const active = Boolean(current && upstream?.includes(host));
        find(`${name}-node`).dataset.selected = String(active);
        find(`${name}-route`).textContent = !current || !upstream ? 'Route unconfirmed' : active ? 'Configured upstream' : 'Not selected upstream';
      }
      const family = variant ? proxies.filter(p => p.variant.endsWith('-plaintext') === variant.endsWith('-plaintext')) : proxies;
      find('proxies').replaceChildren(...family.slice(0, 3).map(proxy => {
        const card = el('article', undefined, 'proxy-card'); card.dataset.selected = String(Boolean(current && proxy.selected));
        const config = proxy.config || {};
        card.append(el('b', `${proxy.variant}${proxy.selected ? ' · selected in last sample' : ''}`),
          el('p', `${proxy.ready} ready pods · ${proxy.readyEndpoints} service endpoints`),
          el('p', config.upstream || config.configError || 'Upstream unknown'),
          el('p', `Filters: ${config.filters?.join(', ') || (config.configError ? 'unknown' : 'none')}`),
          el('p', `ConfigMap ${config.hash || 'unknown'} · upstream TLS ${config.upstreamTls == null ? 'unknown' : config.upstreamTls ? 'on' : 'off'}`));
        return card;
      }));
      if (!proxies.length) empty(find('proxies'), 'Proxy configuration has not been observed.');
    },
  };
}

export function mountMigrationClients(root) {
  const workloads = root.querySelector('[data-role="workloads"]');
  const members = root.querySelector('[data-role="members"]');
  let lastWorkloads;
  return {
    onMigration(snapshot, { now, connected }) {
      sources(root, snapshot, now, connected);
      const clients = snapshot?.infrastructure?.clients || [];
      const current = fresh(snapshot?.kubernetes, now, connected);
      const signature = JSON.stringify([clients, current]);
      if (signature !== lastWorkloads) {
        lastWorkloads = signature;
        workloads.replaceChildren(...clients.map(client => {
          const row = el('article', undefined, 'workload');
          row.append(el('strong', client.name), el('p', `${client.ready}/${client.desired} ready · ${client.pods.reduce((n, p) => n + p.restarts, 0)} restarts${current ? '' : ' · last known'}`));
          const details = el('details'); details.append(el('summary', 'Inspect pods'));
          client.pods.forEach(pod => details.append(el('p', `${pod.name} · ${pod.phase} · ${pod.ready ? 'ready' : 'not ready'} · ${pod.restarts} restarts`)));
          if (client.omittedPods) details.append(el('p', `${client.omittedPods} additional pods omitted`));
          row.append(details); return row;
        }));
        if (!clients.length) empty(workloads, 'No client workloads observed. Check the Kubernetes source.');
      }
      root.querySelector('[data-role="consumed"]').textContent = `${snapshot?.consumedObservations ?? 0} consumed observations since backend start · ${snapshot?.malformedEvents ?? 0} unsupported events`;
      const table = el('table');
      const header = el('tr'); ['Group / member', 'Topic / partitions', 'Last consumption'].forEach(label => header.append(el('th', label)));
      const head = el('thead'); head.append(header); table.append(head);
      const body = el('tbody');
      [...(snapshot?.members || [])].sort((a, b) => b.lastSeen - a.lastSeen).slice(0, 100).forEach(member => {
        const row = el('tr'); const identity = el('td', member.group);
        identity.append(el('small', member.member));
        const topic = el('td', member.topic); topic.append(el('small', `Last observed: ${member.partitions.join(', ') || 'none'}`));
        const progress = el('td', member.lastConsumed == null ? (member.observations ? 'time unavailable' : 'none observed') : age(member.lastConsumed, now));
        progress.append(el('small', fresh(snapshot.telemetry, now, connected) ? `${member.observations} observations` : 'source stale / unknown'));
        row.append(identity, topic, progress); body.append(row);
      });
      table.append(body); members.replaceChildren(table);
      if (!snapshot?.members?.length) empty(members, 'Waiting for consumed-record or partition-assignment telemetry.');
    },
  };
}

export function mountMigrationEvents(root) {
  const host = root.querySelector('[data-role="events"]');
  let signature;
  return {
    onMigration(snapshot, { now, connected }) {
      sources(root, snapshot, now, connected);
      const events = snapshot?.events || [];
      const next = JSON.stringify([snapshot?.runId, events]);
      if (next === signature) return;
      signature = next;
      host.replaceChildren(...events.slice(-48).reverse().map(event => {
        const row = el('article', undefined, 'timeline-event');
        row.append(el('small', `${new Date(event.at).toLocaleTimeString()} · ${event.source} · observed by backend`), el('p', event.message));
        return row;
      }));
      if (!events.length) empty(host, 'No transitions observed in this backend session.');
    },
  };
}
