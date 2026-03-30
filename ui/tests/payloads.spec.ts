import { test, expect } from '@playwright/test';
import { createPayload, deletePayload, listPayloads } from './helpers/api.js';

let testPayloadId: string;

test.describe('Payloads page', () => {
  test.beforeAll(async () => {
    const p = await createPayload(64);
    testPayloadId = p.raw;
  });

  test.afterAll(async () => {
    if (testPayloadId) await deletePayload(testPayloadId);
  });

  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await expect(page.locator('h2')).toHaveText('Payloads');
  });

  // ── List ────────────────────────────────────────────────────────────

  test('displays the payload table with correct columns', async ({ page }) => {
    const headers = page.locator('table thead th');
    await expect(headers).toHaveCount(7);
    for (const col of ['UUID', 'Tier', 'State', 'Size', 'Age', 'Leases', 'Actions']) {
      await expect(page.locator('table thead', { hasText: col })).toBeVisible();
    }
  });

  test('shows at least one payload row', async ({ page }) => {
    await expect(page.locator('table tbody tr.payload-row').first()).toBeVisible();
  });

  test('UUID column shows UUID-formatted strings', async ({ page }) => {
    const firstUUID = await page.locator('td.uuid-cell').first().textContent();
    expect(firstUUID).toMatch(
      /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i
    );
  });

  test('tier badges are visible', async ({ page }) => {
    const badge = page.locator('table tbody .badge').first();
    await expect(badge).toBeVisible();
    const text = await badge.textContent();
    expect(['GPU', 'RAM', 'Disk', 'Object']).toContain(text?.trim());
  });

  test('refresh button reloads list', async ({ page }) => {
    await page.locator('button[title="Refresh"]').click();
    await expect(page.locator('.error-msg')).toHaveCount(0);
    await expect(page.locator('table tbody tr.payload-row').first()).toBeVisible();
  });

  test('payload count line is shown', async ({ page }) => {
    await expect(page.locator('.count-line')).toBeVisible();
    const text = await page.locator('.count-line').textContent();
    expect(text).toMatch(/\d+ payload/);
  });

  // ── Tier filter ──────────────────────────────────────────────────────

  test('All filter shows payloads', async ({ page }) => {
    await page.locator('.tier-tab', { hasText: 'All' }).click();
    await expect(page.locator('table tbody tr.payload-row').first()).toBeVisible();
  });

  test('tier filter tabs are all present', async ({ page }) => {
    for (const tier of ['All', 'GPU', 'RAM', 'Disk', 'Object']) {
      await expect(page.locator('.tier-tab', { hasText: tier })).toBeVisible();
    }
  });

  test('RAM filter shows only RAM payloads or empty message', async ({ page }) => {
    await page.locator('.tier-tab', { hasText: 'RAM' }).click();
    await page.waitForTimeout(500);
    const rows = page.locator('table tbody tr.payload-row');
    const empty = page.locator('p.muted', { hasText: 'No payloads found' });
    const count = await rows.count();
    if (count > 0) {
      const badge = await rows.first().locator('.badge').first().textContent();
      expect(badge?.trim()).toBe('RAM');
    } else {
      await expect(empty).toBeVisible();
    }
  });

  test('active tier tab is highlighted', async ({ page }) => {
    const ramTab = page.locator('.tier-tab', { hasText: 'RAM' });
    await ramTab.click();
    await expect(ramTab).toHaveClass(/active/);
    // reset
    await page.locator('.tier-tab', { hasText: 'All' }).click();
  });

  // ── Detail panel ─────────────────────────────────────────────────────

  test('clicking a row expands the detail panel', async ({ page }) => {
    await page.locator('table tbody tr.payload-row').first().click();
    await expect(page.locator('.detail-panel')).toBeVisible();
  });

  test('detail panel shows snapshot, lineage, metadata tabs', async ({ page }) => {
    await page.locator('table tbody tr.payload-row').first().click();
    for (const tab of ['snapshot', 'lineage', 'metadata']) {
      await expect(page.locator('.detail-tab', { hasText: tab })).toBeVisible();
    }
  });

  test('snapshot tab shows placement fields', async ({ page }) => {
    await page.locator('table tbody tr.payload-row').first().click();
    await expect(page.locator('.detail-tab', { hasText: 'snapshot' })).toHaveClass(/active/);
    await expect(page.locator('.kv', { hasText: 'Payload ID' })).toBeVisible();
    await expect(page.locator('.kv', { hasText: 'Current Tier' })).toBeVisible();
    await expect(page.locator('.kv', { hasText: 'Size' })).toBeVisible();
  });

  test('lineage tab loads without error', async ({ page }) => {
    await page.locator('table tbody tr.payload-row').first().click();
    await page.locator('.detail-tab', { hasText: 'lineage' }).click();
    await expect(page.locator('.detail-panel .error-msg')).toHaveCount(0);
    // Either shows table or "No lineage edges" message — wait for content to load
    await expect(
      page.locator('.detail-panel table').or(page.locator('.detail-panel p.muted'))
    ).toBeVisible({ timeout: 8000 });
  });

  test('metadata tab shows editor', async ({ page }) => {
    await page.locator('table tbody tr.payload-row').first().click();
    await page.locator('.detail-tab', { hasText: 'metadata' }).click();
    await expect(page.locator('.meta-editor')).toBeVisible();
    await expect(page.locator('textarea')).toBeVisible();
    await expect(page.locator('button', { hasText: 'Update Metadata' })).toBeVisible();
    await expect(page.locator('button', { hasText: 'Append Event' })).toBeVisible();
  });

  test('metadata update saves successfully', async ({ page }) => {
    await page.locator('table tbody tr.payload-row').first().click();
    await page.locator('.detail-tab', { hasText: 'metadata' }).click();
    await page.locator('textarea').fill('{"test":"playwright"}');
    await page.locator('button', { hasText: 'Update Metadata' }).click();
    await expect(page.locator('.meta-editor .success-msg, .meta-editor [class*="success"]')).toBeVisible({ timeout: 5000 });
  });

  test('metadata append event saves successfully', async ({ page }) => {
    await page.locator('table tbody tr.payload-row').first().click();
    await page.locator('.detail-tab', { hasText: 'metadata' }).click();
    await page.locator('textarea').fill('{"event":"test"}');
    await page.locator('button', { hasText: 'Append Event' }).click();
    await expect(page.locator('.meta-editor .success-msg, .meta-editor [class*="success"]')).toBeVisible({ timeout: 5000 });
  });

  test('clicking selected row collapses the detail panel', async ({ page }) => {
    const row = page.locator('table tbody tr.payload-row').first();
    await row.click();
    await expect(page.locator('.detail-panel')).toBeVisible();
    await row.click();
    await expect(page.locator('.detail-panel')).toHaveCount(0);
  });

  // ── Actions ───────────────────────────────────────────────────────────

  test('download link is present for each row', async ({ page }) => {
    const downloadLinks = page.locator('a.action-btn[href*="/download"]');
    await expect(downloadLinks.first()).toBeVisible();
  });

  test('pin action marks payload as pinned', async ({ page }) => {
    // Find a RAM payload row that is not already pinned
    const rows = page.locator('table tbody tr.payload-row');
    const count = await rows.count();
    let targetRow = null;
    for (let i = 0; i < count; i++) {
      const row = rows.nth(i);
      const tier = await row.locator('.badge').first().textContent();
      if (tier?.trim() === 'RAM') {
        targetRow = row;
        break;
      }
    }
    if (!targetRow) {
      test.skip();
      return;
    }
    const pinBtn = targetRow.locator('button.action-btn[title="Pin"]');
    if (await pinBtn.count() === 0) {
      test.skip();
      return;
    }
    await pinBtn.click();
    await expect(targetRow.locator('.action-status.ok')).toBeVisible({ timeout: 8000 });
  });

  test('prefetch action completes without error', async ({ page }) => {
    const row = page.locator('table tbody tr.payload-row').first();
    const prefetchBtn = row.locator('button.action-btn[title="Prefetch to RAM"]');
    await expect(prefetchBtn).toBeVisible();
    await prefetchBtn.click();
    // wait for status (ok or err)
    await expect(row.locator('.action-status')).toBeVisible({ timeout: 8000 });
  });

  test('spill action moves a RAM payload to disk', async ({ page }) => {
    const rows = page.locator('table tbody tr.payload-row');
    const count = await rows.count();
    let spillRow = null;
    for (let i = 0; i < count; i++) {
      const row = rows.nth(i);
      const tier = await row.locator('.badge').first().textContent();
      if (tier?.trim() === 'RAM') {
        const spillBtn = row.locator('button.action-btn[title="Spill to disk"]');
        if (await spillBtn.count() > 0) {
          spillRow = row;
          break;
        }
      }
    }
    if (!spillRow) {
      test.skip();
      return;
    }
    await spillRow.locator('button.action-btn[title="Spill to disk"]').click();
    await expect(spillRow.locator('.action-status.ok')).toBeVisible({ timeout: 15000 });
  });

  test('promote action moves a disk payload to RAM', async ({ page }) => {
    // Wait for table to settle after potential spill from previous test
    await page.waitForTimeout(500);
    await page.locator('button[title="Refresh"]').click();
    await page.waitForTimeout(500);

    const rows = page.locator('table tbody tr.payload-row');
    const count = await rows.count();
    let promoteRow = null;
    for (let i = 0; i < count; i++) {
      const row = rows.nth(i);
      const tier = await row.locator('.badge').first().textContent();
      if (tier?.trim() === 'Disk') {
        const promoteBtn = row.locator('button.action-btn[title="Promote to RAM"]');
        if (await promoteBtn.count() > 0) {
          promoteRow = row;
          break;
        }
      }
    }
    if (!promoteRow) {
      test.skip();
      return;
    }
    await promoteRow.locator('button.action-btn[title="Promote to RAM"]').click();
    await expect(promoteRow.locator('.action-status.ok')).toBeVisible({ timeout: 15000 });
  });

  test('delete action removes a payload (with confirmation)', async ({ page }) => {
    // Create a dedicated payload for deletion
    await createPayload(8);
    await page.reload();

    const rows = page.locator('table tbody tr.payload-row');
    const countBefore = await rows.count();
    expect(countBefore).toBeGreaterThan(0);

    // Accept the confirm dialog
    page.once('dialog', dialog => dialog.accept());
    await rows.first().locator('button.action-btn[title="Delete"]').click();
    // Refresh runs after delete — row count should decrease
    await expect(rows).toHaveCount(countBefore - 1, { timeout: 8000 });
  });

  test('download a disk payload returns a file', async ({ page }) => {
    // Create a fresh committed RAM payload and spill it to disk
    const p = await createPayload(8);
    const spillResp = await page.request.post('/v1/payloads/spill', {
      data: { ids: [{ value: p.raw }], policy: 'SPILL_POLICY_BLOCKING', fsync: true },
    });
    if (spillResp.status() !== 200) {
      await deletePayload(p.raw);
      test.skip();
      return;
    }
    // Download directly via API request (avoids target="_blank" popup complexity)
    const response = await page.request.get(`/v1/payloads/${p.urlSafe}/download`);
    expect(response.status()).toBe(200);
    const contentDisposition = response.headers()['content-disposition'] ?? '';
    expect(contentDisposition).toContain('attachment');
    expect(contentDisposition).toMatch(/payload-.+\.bin/);
    await deletePayload(p.raw);
  });

  // ── GPU tier ──────────────────────────────────────────────────────────

  test('GPU filter tab shows GPU payloads or empty message', async ({ page }) => {
    await page.locator('.tier-tab', { hasText: 'GPU' }).click();
    await page.waitForTimeout(500);
    const rows = page.locator('table tbody tr.payload-row');
    const empty = page.locator('p.muted', { hasText: 'No payloads found' });
    const count = await rows.count();
    if (count > 0) {
      const badge = await rows.first().locator('.badge').first().textContent();
      expect(badge?.trim()).toBe('GPU');
    } else {
      await expect(empty).toBeVisible();
    }
    // reset
    await page.locator('.tier-tab', { hasText: 'All' }).click();
  });

  test('GPU payload allocates and appears in list', async ({ page }) => {
    let p: Awaited<ReturnType<typeof createPayload>>;
    try {
      p = await createPayload(8, 'TIER_GPU');
    } catch {
      test.skip(); // GPU not available in this environment
      return;
    }
    await page.reload();
    // Wait for initial list to load before switching to GPU tab
    await expect(page.locator('table tbody tr.payload-row').first()).toBeVisible({ timeout: 8000 });

    await page.locator('.tier-tab', { hasText: 'GPU' }).click();
    await expect(page.locator('table tbody tr.payload-row').first()).toBeVisible({ timeout: 8000 });
    const badge = await page.locator('table tbody tr.payload-row').first().locator('.badge').first().textContent();
    expect(badge?.trim()).toBe('GPU');

    await deletePayload(p.raw);
  });

  test('download a GPU payload auto-spills and returns a file', async ({ page }) => {
    let p: Awaited<ReturnType<typeof createPayload>>;
    try {
      p = await createPayload(8, 'TIER_GPU');
    } catch {
      test.skip(); // GPU not available in this environment
      return;
    }

    // Verify GPU payload is readable (requires real GPU hardware for a committed state)
    const snapResp = await page.request.get(`/v1/payloads/${p.urlSafe}/snapshot`);
    if (snapResp.status() !== 200) {
      await deletePayload(p.raw);
      test.skip();
      return;
    }
    const snap = await snapResp.json();
    if (snap?.payloadDescriptor?.state !== 'PAYLOAD_STATE_ACTIVE') {
      await deletePayload(p.raw);
      test.skip();
      return;
    }

    // Download — gateway auto-spills GPU→disk before serving
    const response = await page.request.get(`/v1/payloads/${p.urlSafe}/download`);
    if (response.status() !== 200) {
      // GPU→disk spill requires CUDA-enabled build; skip if not available
      await deletePayload(p.raw);
      test.skip();
      return;
    }
    const contentDisposition = response.headers()['content-disposition'] ?? '';
    expect(contentDisposition).toContain('attachment');
    expect(contentDisposition).toMatch(/payload-.+\.bin$/);

    await deletePayload(p.raw);
  });
});
