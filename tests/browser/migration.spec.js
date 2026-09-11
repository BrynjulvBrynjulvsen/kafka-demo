import { test, expect } from '@playwright/test';

test('a failed presentation import reports startup failure rather than connecting', async ({ page }) => {
  await page.route('**/js/concepts/migration-flow.js', route => route.abort());
  await page.goto('/migration.html#/migration-flow');
  await expect(page.locator('#connection')).toHaveText('Presentation failed to load · reload the page to retry');
  await expect(page.locator('#connection')).toHaveAttribute('data-state', 'error');
  await page.unroute('**/js/concepts/migration-flow.js');
  await page.routeWebSocket('**/ws/migration', ws => {
    ws.send(JSON.stringify({ type: 'subscribed', version: 1, topic: '/migration' }));
    ws.send(JSON.stringify(snapshot()));
  });
  await page.reload();
  await expect(page.locator('#connection')).toHaveText('Migration observation stream connected');
});

function snapshot(variant = 'legacy') {
  const at = Date.now();
  return { type: 'migration-snapshot', version: 1, enabled: true, context: 'kind-kafka-proxy-poc',
    namespace: 'kafka-platform-poc', runId: 'fixture-run', startedAt: at, at,
    kubernetes: { at, error: null }, telemetry: { at, error: null },
    infrastructure: { selectedVariant: variant, readyEndpointCount: 1,
      clients: [{ name: 'demo-producer', desired: 10, ready: 10, pods: [{ name: '<b>pod</b>', phase: 'Running', ready: true, restarts: 0 }], omittedPods: 0 },
        { name: 'demo-consumer', desired: 10, ready: 9, pods: [], omittedPods: 0 }],
      proxies: ['legacy', 'migration-blocking', 'target'].map(name => ({ name: `kroxylicious-${name}`, variant: name,
        selected: name === variant, ready: 1, readyEndpoints: name === variant ? 1 : 0,
        config: { upstream: `${name === 'target' ? 'kafkabroker' : 'legacy-broker'}.kafka-platform-poc.svc.cluster.local:9097`,
          filters: name === 'migration-blocking' ? ['sasl-termination', 'block-produce'] : [], hash: 'abc123def456', upstreamTls: name === 'target' } })) },
    members: [{ group: 'g1', member: 'consumer-member-1', topic: 'demo.topic', partitions: [0, 1, 2], lastSeen: at, lastConsumed: at, observations: 120 }],
    producers: [], consumedObservations: 120, malformedEvents: 0,
    events: [{ id: 1, at, source: 'kubernetes', message: `Service selector: ${variant}` },
      { id: 2, at, source: 'telemetry', message: '<img src=x onerror=alert(1)> Assigned demo.topic/1 · g1' }],
  };
}

test('migration routing, safe text, source failures, navigation and reconnect', async ({ page }) => {
  let connections = 0; let socket; let current = snapshot();
  const errors = [];
  page.on('pageerror', error => errors.push(error.message));
  await page.routeWebSocket('**/ws/migration', ws => {
    connections++; socket = ws;
    ws.send(JSON.stringify({ type: 'subscribed', version: 1, topic: '/migration' }));
    ws.send(JSON.stringify(current));
  });
  await page.goto('/migration.html');
  await expect(page.locator('[data-role="route"]')).toHaveText('legacy');
  await expect(page.locator('[data-role="legacy-node"]')).toHaveAttribute('data-selected', 'true');
  await page.screenshot({ animations: 'disabled', path: 'build/migration-topology.png' });
  await page.keyboard.press('ArrowRight');
  await expect(page.locator('#migration-clients')).toHaveClass(/present/);
  await expect(page.locator('[data-role="members"]')).toContainText('g1');
  await expect(page.locator('[data-role="workloads"] b')).toHaveCount(0);
  await page.screenshot({ animations: 'disabled', path: 'build/migration-clients.png' });
  await page.keyboard.press('ArrowRight');
  await expect(page.locator('#migration-events')).toHaveClass(/present/);
  await expect(page.locator('[data-role="events"]')).toContainText('<img src=x');
  await expect(page.locator('[data-role="events"] img')).toHaveCount(0);
  expect(connections).toBe(1);
  await page.screenshot({ animations: 'disabled', path: 'build/migration-events.png' });
  current = snapshot('target'); socket.send(JSON.stringify(current));
  await expect(page.locator('[data-role="target-node"]')).toHaveAttribute('data-selected', 'true');
  current.kubernetes.error = 'Kubernetes read failed'; socket.send(JSON.stringify(current));
  await expect(page.locator('[data-role="route"]')).toHaveText('Route unknown / stale');
  await expect(page.locator('#migration-events .source-badge').nth(1)).toHaveAttribute('data-fresh', 'true');
  await page.locator('#reconnect').click();
  await expect.poll(() => connections).toBe(2);
  await expect(page.locator('[data-role="events"] .timeline-event')).toHaveCount(2);
  current = snapshot('migration-blocking');
  current.events = Array.from({ length: 70 }, (_, id) => ({ id, at: Date.now(), source: 'telemetry', message: `event ${id}` }));
  socket.send(JSON.stringify(current));
  await expect(page.locator('[data-role="events"] .timeline-event')).toHaveCount(48);
  await page.setViewportSize({ width: 1024, height: 768 });
  await page.goto('/migration.html?viewport=laptop#/migration');
  await expect(page.locator('[data-role="route"]')).toHaveText('migration-blocking');
  await expect(page.locator('#migration')).toHaveClass(/present/);
  await page.screenshot({ animations: 'disabled', path: 'build/migration-laptop.png' });
  expect(errors).toEqual([]);
});

test('lost snapshot stream makes retained routing unknown', async ({ page }) => {
  await page.routeWebSocket('**/ws/migration', ws => {
    ws.send(JSON.stringify({ type: 'subscribed', version: 1, topic: '/migration' }));
    ws.send(JSON.stringify(snapshot()));
  });
  await page.goto('/migration.html');
  await expect(page.locator('[data-role="route"]')).toHaveText('legacy');
  await expect(page.locator('[data-role="route"]')).toHaveText('Route unknown / stale', { timeout: 10000 });
  await expect(page.locator('#connection')).toContainText('stale');
});
