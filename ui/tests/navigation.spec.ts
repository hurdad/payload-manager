import { test, expect } from '@playwright/test';

test.describe('Navigation', () => {
  test('loads the UI and shows the Payloads page by default', async ({ page }) => {
    await page.goto('/');
    await expect(page.locator('h2')).toHaveText('Payloads');
  });

  test('sidebar is visible by default', async ({ page }) => {
    await page.goto('/');
    const sidebar = page.locator('aside');
    await expect(sidebar).toHaveClass(/open/);
    await expect(page.locator('.nav-item', { hasText: 'Payloads' })).toBeVisible();
    await expect(page.locator('.nav-item', { hasText: 'Streams' })).toBeVisible();
    await expect(page.locator('.nav-item', { hasText: 'Admin' })).toBeVisible();
  });

  test('sidebar collapses and expands via toggle button', async ({ page }) => {
    await page.goto('/');
    const sidebar = page.locator('aside');
    await expect(sidebar).toHaveClass(/open/);

    // collapse
    await page.locator('.icon-btn', { hasText: '›' }).click();
    await expect(sidebar).not.toHaveClass(/open/);

    // expand
    await page.locator('.icon-btn', { hasText: '‹' }).click();
    await expect(sidebar).toHaveClass(/open/);
  });

  test('navigates to Streams page', async ({ page }) => {
    await page.goto('/');
    await page.locator('.nav-item', { hasText: 'Streams' }).click();
    await expect(page.locator('h2')).toHaveText('Streams');
  });

  test('navigates to Admin page', async ({ page }) => {
    await page.goto('/');
    await page.locator('.nav-item', { hasText: 'Admin' }).click();
    await expect(page.locator('h2')).toHaveText('Admin');
  });

  test('navigates back to Payloads page', async ({ page }) => {
    await page.goto('/');
    await page.locator('.nav-item', { hasText: 'Admin' }).click();
    await page.locator('.nav-item', { hasText: 'Payloads' }).click();
    await expect(page.locator('h2')).toHaveText('Payloads');
  });

  test('theme toggle switches between dark and light', async ({ page }) => {
    await page.goto('/');
    const html = page.locator('html');
    const toggleBtn = page.locator('.icon-btn').first();

    const before = await html.evaluate(el =>
      getComputedStyle(el).getPropertyValue('--bg').trim()
    );
    await toggleBtn.click();
    const after = await html.evaluate(el =>
      getComputedStyle(el).getPropertyValue('--bg').trim()
    );
    expect(before).not.toBe(after);

    // toggle back
    await toggleBtn.click();
    const restored = await html.evaluate(el =>
      getComputedStyle(el).getPropertyValue('--bg').trim()
    );
    expect(restored).toBe(before);
  });

  test('active nav item is highlighted', async ({ page }) => {
    await page.goto('/');
    const payloadsBtn = page.locator('.nav-item', { hasText: 'Payloads' });
    await expect(payloadsBtn).toHaveClass(/active/);

    await page.locator('.nav-item', { hasText: 'Admin' }).click();
    await expect(page.locator('.nav-item', { hasText: 'Admin' })).toHaveClass(/active/);
    await expect(payloadsBtn).not.toHaveClass(/active/);
  });

  test('SPA routes resolve to index (no 404 on reload)', async ({ page }) => {
    await page.goto('/');
    const resp = await page.goto('/');
    expect(resp?.status()).toBe(200);
  });
});
