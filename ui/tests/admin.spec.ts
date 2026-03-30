import { test, expect } from '@playwright/test';

test.describe('Admin page', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.locator('.nav-item', { hasText: 'Admin' }).click();
    await expect(page.locator('h2')).toHaveText('Admin');
  });

  test('shows all four tier stat cards', async ({ page }) => {
    for (const tier of ['GPU', 'RAM', 'Disk', 'Object']) {
      await expect(page.locator('.tier-card', { hasText: tier })).toBeVisible();
    }
  });

  test('each card shows a payload count and bytes value', async ({ page }) => {
    // Wait for stats to load (cards only render once API returns)
    const cards = page.locator('.tier-card');
    await expect(cards).toHaveCount(4, { timeout: 8000 });

    for (let i = 0; i < 4; i++) {
      const card = cards.nth(i);
      // stat-value elements: first is payload count, second is bytes
      const statValues = card.locator('.stat-value');
      await expect(statValues.nth(0)).toBeVisible();  // count
      await expect(statValues.nth(1)).toBeVisible();  // bytes
    }
  });

  test('shows totals card with total payloads and bytes', async ({ page }) => {
    const totals = page.locator('.totals');
    await expect(totals).toBeVisible();
    await expect(totals.locator('.totals-row', { hasText: 'Total payloads' })).toBeVisible();
    await expect(totals.locator('.totals-row', { hasText: 'Total bytes' })).toBeVisible();
  });

  test('refresh button reloads stats without error', async ({ page }) => {
    await page.locator('button', { hasText: '↺ Refresh' }).click();
    await expect(page.locator('.error-msg')).toHaveCount(0);
    await expect(page.locator('.tier-card').first()).toBeVisible();
  });

  test('stat values are numeric strings or formatted bytes', async ({ page }) => {
    const firstCard = page.locator('.tier-card').first();
    const countText = await firstCard.locator('.stat-value').first().textContent();
    expect(countText).toMatch(/^\d+$/);
  });
});
