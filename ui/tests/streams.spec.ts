import { test, expect } from '@playwright/test';
import { createPayload, createStream, deleteStream, deletePayload } from './helpers/api.js';

const NS = 'playwright';
const STREAM = 'e2e-test';

test.describe('Streams page', () => {
  test.beforeAll(async () => {
    // clean up any leftover stream from a previous run
    await deleteStream(NS, STREAM);
  });

  test.afterAll(async () => {
    await deleteStream(NS, STREAM);
  });

  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.locator('.nav-item', { hasText: 'Streams' }).click();
    await expect(page.locator('h2')).toHaveText('Streams');
  });

  /**
   * Ensure the e2e-test stream exists on the server AND appears in the sidebar.
   * Each test gets a fresh browser context (fresh localStorage), so we must
   * add it to the local list with "+ Local" rather than relying on previous tests.
   */
  async function ensureStream(page: any) {
    try { await createStream(NS, STREAM); } catch {} // ignore "already exists"
    const existing = await page.locator('.stream-item', { hasText: STREAM }).count();
    if (existing === 0) {
      await page.locator('input[placeholder="Namespace"]').fill(NS);
      await page.locator('input[placeholder="Name"]').fill(STREAM);
      await page.locator('button', { hasText: '+ Local' }).click();
      await expect(page.locator('.stream-item', { hasText: STREAM })).toBeVisible({ timeout: 5000 });
    }
  }

  // ── Create ────────────────────────────────────────────────────────────

  test('shows create stream form', async ({ page }) => {
    await expect(page.locator('input[placeholder="Namespace"]')).toBeVisible();
    await expect(page.locator('input[placeholder="Name"]')).toBeVisible();
    await expect(page.locator('button', { hasText: 'Create' })).toBeVisible();
  });

  test('creates a new stream via the form', async ({ page }) => {
    await page.locator('input[placeholder="Namespace"]').fill(NS);
    await page.locator('input[placeholder="Name"]').fill(STREAM);
    await page.locator('button', { hasText: 'Create' }).click();
    await expect(page.locator('.success-msg', { hasText: STREAM })).toBeVisible({ timeout: 8000 });
    await expect(page.locator('.stream-item', { hasText: STREAM })).toBeVisible();
  });

  test('stream appears in the sidebar list', async ({ page }) => {
    await ensureStream(page);
    await expect(page.locator('.stream-item', { hasText: STREAM })).toBeVisible();
  });

  // ── Select + view entries ─────────────────────────────────────────────

  test('clicking a stream selects it and shows the viewer', async ({ page }) => {
    await ensureStream(page);
    await page.locator('.stream-item', { hasText: STREAM }).click();
    await expect(page.locator('.stream-path', { hasText: `${NS}/${STREAM}` })).toBeVisible();
    await expect(page.locator('button', { hasText: '↺ Load' })).toBeVisible();
    await expect(page.locator('.append-section')).toBeVisible();
  });

  test('empty stream shows "No entries in range" message', async ({ page }) => {
    await ensureStream(page);
    await page.locator('.stream-item', { hasText: STREAM }).click();
    await expect(page.locator('p.muted', { hasText: 'No entries' })).toBeVisible({ timeout: 5000 });
  });

  // ── Append ────────────────────────────────────────────────────────────

  test('appending a payload ID adds an entry', async ({ page }) => {
    const p = await createPayload(8);
    await ensureStream(page);

    await page.locator('.stream-item', { hasText: STREAM }).click();
    await page.locator('input[placeholder="Payload ID (base64 or UUID)"]').fill(p.raw);
    await page.locator('button', { hasText: 'Append' }).click();
    await expect(page.locator('.success-msg', { hasText: 'Appended' })).toBeVisible({ timeout: 8000 });

    // entry should now appear in the table
    await expect(page.locator('table tbody tr').first()).toBeVisible();
    await deletePayload(p.raw);
  });

  // ── Load controls ─────────────────────────────────────────────────────

  test('Load button refreshes entries', async ({ page }) => {
    await ensureStream(page);
    await page.locator('.stream-item', { hasText: STREAM }).click();
    await page.locator('button', { hasText: '↺ Load' }).click();
    await expect(page.locator('.error-msg')).toHaveCount(0);
  });

  test('max entries selector is present', async ({ page }) => {
    await ensureStream(page);
    await page.locator('.stream-item', { hasText: STREAM }).click();
    const sel = page.locator('select');
    await expect(sel).toBeVisible();
    for (const opt of ['20', '50', '100']) {
      await expect(sel.locator(`option[value="${opt}"]`)).toHaveCount(1);
    }
  });

  // ── Consumer group / commit ───────────────────────────────────────────

  test('consumer group input is present', async ({ page }) => {
    await ensureStream(page);
    await page.locator('.stream-item', { hasText: STREAM }).click();
    await expect(page.locator('input[placeholder="Consumer group"]')).toBeVisible();
    await expect(page.locator('button', { hasText: 'Commit last offset' })).toBeVisible();
  });

  test('commit last offset works when entries exist', async ({ page }) => {
    const p = await createPayload(8);
    await ensureStream(page);
    await page.locator('.stream-item', { hasText: STREAM }).click();

    // ensure at least one entry
    const entries = await page.locator('table tbody tr').count();
    if (entries === 0) {
      await page.locator('input[placeholder="Payload ID (base64 or UUID)"]').fill(p.raw);
      await page.locator('button', { hasText: 'Append' }).click();
      await expect(page.locator('.success-msg', { hasText: 'Appended' })).toBeVisible({ timeout: 8000 });
    }

    await page.locator('button', { hasText: 'Commit last offset' }).click();
    await expect(page.locator('.consumer-row .muted', { hasText: 'Committed:' })).toBeVisible({ timeout: 8000 });
    await deletePayload(p.raw);
  });

  // ── Local-only add ────────────────────────────────────────────────────

  test('+ Local button adds stream to sidebar without API call', async ({ page }) => {
    const localName = 'local-only-stream';
    await page.locator('input[placeholder="Namespace"]').fill('test');
    await page.locator('input[placeholder="Name"]').fill(localName);
    await page.locator('button', { hasText: '+ Local' }).click();
    await expect(page.locator('.stream-item', { hasText: localName })).toBeVisible();
    await expect(page.locator('.success-msg', { hasText: 'Added to local list' })).toBeVisible();
  });

  // ── Delete ────────────────────────────────────────────────────────────

  test('delete button removes stream from sidebar (with confirm)', async ({ page }) => {
    // Create a fresh stream to delete
    const delStream = 'e2e-delete-me';
    await page.locator('input[placeholder="Namespace"]').fill(NS);
    await page.locator('input[placeholder="Name"]').fill(delStream);
    await page.locator('button', { hasText: 'Create' }).click();
    await expect(page.locator('.stream-item', { hasText: delStream })).toBeVisible({ timeout: 8000 });

    page.once('dialog', dialog => dialog.accept());
    await page.locator('.stream-item', { hasText: delStream }).locator('button.icon-action').click();
    await expect(page.locator('.stream-item', { hasText: delStream })).toHaveCount(0);
  });
});
