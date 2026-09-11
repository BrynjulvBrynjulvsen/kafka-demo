import { sources } from './migration-views.js';

const fresh = (source, now, connected) => connected && source?.at != null && !source.error && now - source.at < 15000;
const text = (node, value) => { node.textContent = value; };
const svgNS = 'http://www.w3.org/2000/svg';

export function mountMigrationFlow(root) {
  const find = role => root.querySelector(`[data-role="${role}"]`);
  const particles = find('particles');
  const motion = window.matchMedia('(prefers-reduced-motion: reduce)');
  let baseline; let lastSample; let rate = null; let recentGroups = new Set();
  function pulse(count) {
    if (motion.matches || !root.classList.contains('present')) return;
    particles.replaceChildren();
    for (let i = 0; i < Math.min(6, count); i++) {
      const dot = document.createElementNS(svgNS, 'circle');
      dot.setAttribute('r', '5'); dot.setAttribute('fill', 'var(--datablaa)');
      dot.setAttribute('cx', '470'); dot.setAttribute('cy', '210');
      const animation = document.createElementNS(svgNS, 'animateMotion');
      // The path is schematic: it never selects an inferred source broker.
      animation.setAttribute('path', 'M0 0 C-120 0 -110 75 -220 75');
      animation.setAttribute('dur', '0.85s'); animation.setAttribute('begin', 'indefinite');
      animation.addEventListener('endEvent', () => dot.remove(), { once: true });
      dot.append(animation); particles.append(dot);
      animation.beginElementAt(i * 0.075);
    }
  }
  return {
    onMigration(snapshot, { now, connected }) {
      sources(root, snapshot, now, connected);
      const telemetryOk = Boolean(snapshot?.enabled && fresh(snapshot.telemetry, now, connected));
      const kubeOk = Boolean(snapshot?.enabled && fresh(snapshot.kubernetes, now, connected));
      const infra = snapshot?.infrastructure || {};
      const selected = infra.proxies?.find(p => p.selected);
      const upstream = selected?.config?.upstream;
      text(find('variant'), kubeOk ? infra.selectedVariant || 'No route' : 'Route unknown');
      const blocking = selected?.config?.filters?.includes('block-produce');
      root.dataset.blocking = String(Boolean(kubeOk && blocking));
      text(find('policy'), !kubeOk || !selected || selected.config?.configError ? 'Configuration stale / unavailable' : blocking ? 'Produce blocking configured' : 'No produce block configured');
      text(find('endpoint-count'), `${kubeOk ? '' : 'Last known: '}${infra.readyEndpointCount ?? '—'} ready service endpoints`);
      for (const [name, prefix] of [['legacy', 'legacy-broker.'], ['target', 'kafkabroker.']]) {
        const active = Boolean(kubeOk && upstream?.includes(prefix));
        find(name).dataset.active = String(active); find(`${name}-wire`).dataset.active = String(active);
        text(find(`${name}-state`), !kubeOk || !upstream ? 'Route unconfirmed' : active ? 'Selected upstream' : 'Standby route');
      }
      for (const role of ['producer', 'consumer']) {
        const workload = infra.clients?.find(c => c.name === `demo-${role}`);
        text(find(`${role}-count`), workload ? `${workload.ready} / ${workload.desired} pods ready${kubeOk ? '' : ' · last known'}` : 'No pod sample');
        const lights = (workload?.pods || []).slice(0, 20).map(pod => {
          const light = document.createElement('span'); light.dataset.ready = String(kubeOk && pod.ready);
          light.title = `${pod.name} · ${kubeOk ? pod.ready ? 'ready' : 'not ready' : 'last known'}`;
          return light;
        });
        find(`${role}-lights`).replaceChildren(...lights);
      }
      if (!telemetryOk) { baseline = null; rate = null; recentGroups.clear(); particles.replaceChildren(); }
      else if (!baseline || snapshot.at !== lastSample) {
        const sameRun = baseline?.runId === snapshot.runId;
        const elapsed = sameRun ? snapshot.at - baseline.at : 0;
        const delta = sameRun ? snapshot.consumedObservations - baseline.total : 0;
        const validDelta = sameRun && elapsed > 0 && elapsed <= 5000 && delta >= 0;
        rate = validDelta ? delta * 1000 / elapsed : null;
        recentGroups = new Set();
        if (validDelta) {
          for (const member of snapshot.members || []) {
            const key = JSON.stringify([member.group, member.member, member.topic]);
            if (baseline.members.has(key) && member.observations > baseline.members.get(key)) recentGroups.add(member.group);
          }
          if (delta > 0) pulse(delta);
        }
        baseline = { runId: snapshot.runId, at: snapshot.at, total: snapshot.consumedObservations,
          members: new Map((snapshot.members || []).map(m => [JSON.stringify([m.group, m.member, m.topic]), m.observations])) };
        lastSample = snapshot.at;
      }
      if (!root.classList.contains('present') || motion.matches) particles.replaceChildren();
      text(find('rate'), rate == null ? '—' : rate.toFixed(0));
      text(find('activity'), !telemetryOk ? 'Telemetry unavailable' : rate == null ? 'Establishing activity baseline' : rate > 0 ? 'Consumption reports arriving' : 'No new reports in last sample');
      const groups = [...new Set((snapshot?.members || []).map(m => m.group))].sort();
      find('groups').replaceChildren(...groups.slice(0, 8).map(group => {
        const chip = document.createElement('span'); chip.textContent = group; chip.dataset.active = String(telemetryOk && recentGroups.has(group)); return chip;
      }));
      if (!groups.length) text(find('groups'), 'Waiting for consumer telemetry');
      if (groups.length > 8) { const more = document.createElement('span'); more.textContent = `+${groups.length - 8}`; find('groups').append(more); }
      const latest = snapshot?.events?.at(-1);
      text(find('transition'), latest ? latest.message : 'No transitions observed yet');
    },
  };
}
