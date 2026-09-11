import { test, expect } from '@playwright/test';

test('live migration observations reach the deck across navigation and reconnect', async ({ page, request }) => {
  test.skip(process.env.MIGRATION_LIVE !== '1', 'Opt in against the running read-only migration observer');
  const initial = await (await request.get('/api/migration')).json();
  expect(initial.enabled).toBe(true);
  expect(initial.context).toBe('kind-kafka-proxy-poc');
  expect(initial.kubernetes.error).toBeNull();
  expect(initial.telemetry.error).toBeNull();
  expect(initial.consumedObservations).toBeGreaterThan(0);
  let connections = 0;
  page.on('websocket', () => connections++);
  await page.goto('/migration.html');
  await expect(page.locator('[data-role="route"]')).toHaveText(initial.infrastructure.selectedVariant);
  await expect(page.locator('#migration .source-badge[data-fresh="true"]')).toHaveCount(2);
  await page.keyboard.press('ArrowRight');
  await expect(page.locator('#migration-clients')).toHaveClass(/present/);
  await expect.poll(async () => Number((await page.locator('[data-role="consumed"]').innerText()).split(' ')[0])).toBeGreaterThan(initial.consumedObservations);
  await expect(page.locator('[data-role="members"] tbody tr')).not.toHaveCount(0);
  await page.screenshot({ path: 'build/migration-live-clients.png', animations: 'disabled' });
  await page.keyboard.press('ArrowRight');
  await expect(page.locator('#migration-events')).toHaveClass(/present/);
  await expect(page.locator('[data-role="events"]')).toContainText('telemetry');
  expect(connections).toBe(1);
  await page.screenshot({ path: 'build/migration-live-events.png', animations: 'disabled' });
  await page.locator('#reconnect').click();
  await expect.poll(() => connections).toBe(2);
  await expect(page.locator('#migration-events .source-badge[data-fresh="true"]')).toHaveCount(2);
  const final = await (await request.get('/api/migration')).json();
  expect(final.runId).toBe(initial.runId);
  expect(final.malformedEvents).toBe(initial.malformedEvents);
  await page.goto('/migration.html?live-flow#/migration-flow');
  await expect(page.locator('#migration-flow')).toHaveClass(/present/);
  await expect(page.locator('#migration-flow [data-role="rate"]')).not.toHaveText('—', { timeout: 10000 });
  await page.screenshot({ path: 'build/migration-flow-live.png', animations: 'disabled' });

});
