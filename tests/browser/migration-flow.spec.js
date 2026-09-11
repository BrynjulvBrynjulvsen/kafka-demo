import { test, expect } from '@playwright/test';

function sample() {
  const at = Date.now();
  return { type: 'migration-snapshot', version: 1, enabled: true, context: 'kind-kafka-proxy-poc', namespace: 'kafka-platform-poc', runId: 'flow-test', at,
    kubernetes: { at, error: null }, telemetry: { at, error: null },
    consumedObservations: 100, members: [{ group: 'g1', member: 'c1', topic: 'demo.topic', observations: 100, partitions: [0], lastSeen: at, lastConsumed: at }], malformedEvents: 0, events: [],
    infrastructure: { selectedVariant: 'legacy', readyEndpointCount: 1,
      clients: ['producer', 'consumer'].map(role => ({ name: `demo-${role}`, ready: 10, desired: 10, pods: Array.from({ length: 10 }, (_, i) => ({ name: `${role}-${i}`, ready: true })) })),
      proxies: [{ selected: true, config: { upstream: 'legacy-broker.kafka-platform-poc.svc.cluster.local:9097', filters: ['sasl-termination'] } }] } };
}

test('flow pulses use new reports, stop on stale sources, and do not replay on reconnect', async ({ page }) => {
  let current = sample(); let socket; let sockets = 0;
  const errors = []; page.on('pageerror', e => errors.push(e.message));
  await page.routeWebSocket('**/ws/migration', ws => {
    socket = ws; sockets++;
    ws.send(JSON.stringify({ type: 'subscribed', version: 1, topic: '/migration' })); ws.send(JSON.stringify(current));
  });
  await page.goto('/migration.html#/migration-flow');
  const flow = page.locator('#migration-flow');
  await expect(flow).toHaveClass(/present/);
  await expect(flow.locator('[data-role="particles"] circle')).toHaveCount(0);
  await expect(flow.locator('[data-role="legacy"]')).toHaveAttribute('data-active', 'true');
  current.at += 1000; current.consumedObservations += 24; current.members[0].observations += 24;
  socket.send(JSON.stringify(current));
  await expect(flow.locator('[data-role="rate"]')).toHaveText('24');
  await expect(flow.locator('[data-role="groups"] span')).toHaveAttribute('data-active', 'true');
  await expect(flow.locator('[data-role="particles"] circle')).toHaveCount(6);
  await page.screenshot({ path: 'build/migration-flow.png' });
  current.telemetry.error = 'Disconnected'; socket.send(JSON.stringify(current));
  await expect(flow.locator('[data-role="rate"]')).toHaveText('—');
  await expect(flow.locator('[data-role="particles"] circle')).toHaveCount(0);
  current.telemetry.error = null; current.at += 1000; current.consumedObservations += 2000;
  socket.send(JSON.stringify(current));
  await expect(flow.locator('[data-role="rate"]')).toHaveText('—');
  current.infrastructure.selectedVariant = 'target';
  current.infrastructure.proxies[0].config.upstream = 'kafkabroker.kafka-platform-poc.svc.cluster.local:9097';
  socket.send(JSON.stringify(current));
  await expect(flow.locator('[data-role="target"]')).toHaveAttribute('data-active', 'true');
  current.kubernetes.error = 'unavailable'; socket.send(JSON.stringify(current));
  await expect(flow.locator('[data-role="target"]')).toHaveAttribute('data-active', 'false');
  await page.locator('#reconnect').click(); await expect.poll(() => sockets).toBe(2);
  await expect(flow.locator('[data-role="particles"] circle')).toHaveCount(0);
  await page.emulateMedia({ reducedMotion: 'reduce' });
  current.at += 1000; current.consumedObservations += 5; socket.send(JSON.stringify(current));
  await expect(flow.locator('[data-role="rate"]')).toHaveText('5');
  await expect(flow.locator('[data-role="particles"] circle')).toHaveCount(0);
  await page.setViewportSize({ width: 1024, height: 768 });
  await page.screenshot({ path: 'build/migration-flow-laptop.png', animations: 'disabled' });
  expect(errors).toEqual([]);
});
