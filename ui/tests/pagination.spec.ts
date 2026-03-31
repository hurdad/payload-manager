/**
 * Pagination tests — split into two describe blocks:
 *
 *  1. "api.js listPayloads" — unit-level: intercept the HTTP request and assert
 *     the correct query parameters are sent by the api.js helper.
 *
 *  2. "Payloads pagination UI" — component-level: mock the API with page.route()
 *     so the entire pagination state machine (buttons, count line, page token
 *     passing) is exercised without a real backend.
 */

import { test, expect } from '@playwright/test';

// ---------------------------------------------------------------------------
// Shared mock data factories
// ---------------------------------------------------------------------------

function makeSummary(i: number, tier = 'TIER_RAM') {
  // 16-byte UUIDs encoded as base64 — use a simple fixed pattern per index
  const hex = i.toString(16).padStart(32, '0');
  const bytes = hex.match(/.{1,2}/g)!.map((b) => parseInt(b, 16));
  const raw = Buffer.from(bytes).toString('base64');
  return {
    id: { value: raw },
    tier,
    state: 'PAYLOAD_STATE_ACTIVE',
    sizeBytes: '64',
    createdAtMs: String(Date.now() - i * 1000),
    expiresAtMs: '0',
    activeLeaseCount: 0,
  };
}

function makePageResponse(opts: {
  count?: number;
  total: number;
  nextPageToken?: string;
  tier?: string;
}) {
  const { count = opts.total, total, nextPageToken = '', tier } = opts;
  return {
    payloads: Array.from({ length: count }, (_, i) => makeSummary(i, tier)),
    totalCount: total,
    nextPageToken,
  };
}

// ---------------------------------------------------------------------------
// 1. api.js unit tests — verify query parameters sent by listPayloads()
// ---------------------------------------------------------------------------

test.describe('api.js listPayloads', () => {
  test('sends no extra params by default (tier=all, page 1)', async ({ page }) => {
    let capturedUrl: string | null = null;

    await page.route('/v1/payloads**', (route) => {
      capturedUrl = route.request().url();
      route.fulfill({ json: makePageResponse({ total: 3 }) });
    });

    await page.goto('/');
    await page.waitForSelector('table tbody tr.payload-row');

    expect(capturedUrl).toBeTruthy();
    const url = new URL(capturedUrl!);
    // Default tier filter from localStorage may set tierFilter='' which is omitted
    expect(url.searchParams.has('pageToken')).toBe(false);
    expect(url.searchParams.has('pageSize')).toBe(false);
  });

  test('sends pageToken when navigating to next page', async ({ page }) => {
    const urls: string[] = [];

    await page.route('/v1/payloads**', (route) => {
      const url = new URL(route.request().url());
      const token = url.searchParams.get('pageToken') ?? '';
      urls.push(url.search);
      if (token === '') {
        route.fulfill({ json: makePageResponse({ count: 3, total: 6, nextPageToken: '3' }) });
      } else {
        route.fulfill({ json: makePageResponse({ count: 3, total: 6 }) });
      }
    });

    await page.goto('/');
    await page.waitForSelector('table tbody tr.payload-row');
    await page.locator('button.page-btn', { hasText: 'Next' }).click();
    await page.waitForSelector('table tbody tr.payload-row');

    // Second request must include pageToken=3
    const nextReqUrl = urls.find((u) => u.includes('pageToken=3'));
    expect(nextReqUrl).toBeTruthy();
  });

  test('sends tierFilter param when tier tab is selected', async ({ page }) => {
    let lastUrl: string | null = null;

    await page.route('/v1/payloads**', (route) => {
      lastUrl = route.request().url();
      route.fulfill({ json: makePageResponse({ total: 2, tier: 'TIER_DISK' }) });
    });

    await page.goto('/');
    // Clear localStorage tier filter so we start on All
    await page.evaluate(() => localStorage.setItem('pm-tier-filter', ''));
    await page.goto('/');
    await page.waitForSelector('table tbody tr.payload-row');
    await page.locator('.tier-tab', { hasText: 'Disk' }).click();
    await page.waitForTimeout(300);

    const url = new URL(lastUrl!);
    expect(url.searchParams.get('tierFilter')).toBe('TIER_DISK');
  });
});

// ---------------------------------------------------------------------------
// 2. Payloads pagination UI tests
// ---------------------------------------------------------------------------

test.describe('Payloads pagination UI', () => {
  test.beforeEach(async ({ page }) => {
    await page.evaluate(() => localStorage.setItem('pm-tier-filter', ''));
  });

  test('Next button disabled and Prev button disabled on single page', async ({ page }) => {
    await page.route('/v1/payloads**', (route) =>
      route.fulfill({ json: makePageResponse({ total: 3 }) })
    );

    await page.goto('/');
    await page.waitForSelector('table tbody tr.payload-row');

    await expect(page.locator('button.page-btn', { hasText: 'Next' })).toBeDisabled();
    await expect(page.locator('button.page-btn', { hasText: 'Prev' })).toBeDisabled();
  });

  test('Next button enabled when next_page_token present', async ({ page }) => {
    await page.route('/v1/payloads**', (route) =>
      route.fulfill({ json: makePageResponse({ count: 3, total: 7, nextPageToken: '3' }) })
    );

    await page.goto('/');
    await page.waitForSelector('table tbody tr.payload-row');

    await expect(page.locator('button.page-btn', { hasText: 'Next' })).toBeEnabled();
    await expect(page.locator('button.page-btn', { hasText: 'Prev' })).toBeDisabled();
  });

  test('clicking Next loads next page and enables Prev', async ({ page }) => {
    let call = 0;
    await page.route('/v1/payloads**', (route) => {
      call++;
      if (call === 1) {
        route.fulfill({ json: makePageResponse({ count: 3, total: 6, nextPageToken: '3' }) });
      } else {
        route.fulfill({ json: makePageResponse({ count: 3, total: 6 }) });
      }
    });

    await page.goto('/');
    await page.waitForSelector('table tbody tr.payload-row');
    await page.locator('button.page-btn', { hasText: 'Next' }).click();
    await page.waitForSelector('table tbody tr.payload-row');

    await expect(page.locator('button.page-btn', { hasText: 'Prev' })).toBeEnabled();
    await expect(page.locator('button.page-btn', { hasText: 'Next' })).toBeDisabled();
  });

  test('clicking Prev returns to first page and disables Prev again', async ({ page }) => {
    let call = 0;
    await page.route('/v1/payloads**', (route) => {
      call++;
      if (call % 2 === 1) {
        route.fulfill({ json: makePageResponse({ count: 3, total: 6, nextPageToken: '3' }) });
      } else {
        route.fulfill({ json: makePageResponse({ count: 3, total: 6 }) });
      }
    });

    await page.goto('/');
    await page.waitForSelector('table tbody tr.payload-row');
    await page.locator('button.page-btn', { hasText: 'Next' }).click();
    await page.waitForSelector('table tbody tr.payload-row');
    await page.locator('button.page-btn', { hasText: 'Prev' }).click();
    await page.waitForSelector('table tbody tr.payload-row');

    await expect(page.locator('button.page-btn', { hasText: 'Prev' })).toBeDisabled();
    await expect(page.locator('button.page-btn', { hasText: 'Next' })).toBeEnabled();
  });

  test('count line shows X–Y of Z format', async ({ page }) => {
    await page.route('/v1/payloads**', (route) =>
      route.fulfill({ json: makePageResponse({ count: 5, total: 12, nextPageToken: '5' }) })
    );

    await page.goto('/');
    await page.waitForSelector('table tbody tr.payload-row');

    const text = await page.locator('.count-line').textContent();
    expect(text).toMatch(/1–5 of 12 payload/);
  });

  test('count line advances correctly on second page', async ({ page }) => {
    let call = 0;
    await page.route('/v1/payloads**', (route) => {
      call++;
      if (call === 1) {
        route.fulfill({ json: makePageResponse({ count: 5, total: 12, nextPageToken: '5' }) });
      } else {
        route.fulfill({ json: makePageResponse({ count: 5, total: 12, nextPageToken: '10' }) });
      }
    });

    await page.goto('/');
    await page.waitForSelector('table tbody tr.payload-row');
    await page.locator('button.page-btn', { hasText: 'Next' }).click();
    await page.waitForSelector('table tbody tr.payload-row');

    const text = await page.locator('.count-line').textContent();
    expect(text).toMatch(/6–10 of 12 payload/);
  });

  test('count line shows 0 payloads on empty response', async ({ page }) => {
    await page.route('/v1/payloads**', (route) =>
      route.fulfill({ json: { payloads: [], totalCount: 0, nextPageToken: '' } })
    );

    await page.goto('/');
    await page.waitForSelector('.muted', { state: 'visible' });

    // Empty state message
    await expect(page.locator('p.muted', { hasText: 'No payloads' })).toBeVisible();
  });

  test('changing tier filter resets to page 1', async ({ page }) => {
    const tokens: string[] = [];
    await page.route('/v1/payloads**', (route) => {
      const url = new URL(route.request().url());
      tokens.push(url.searchParams.get('pageToken') ?? '');
      if (tokens.length === 1) {
        route.fulfill({ json: makePageResponse({ count: 3, total: 9, nextPageToken: '3' }) });
      } else {
        route.fulfill({ json: makePageResponse({ count: 3, total: 9 }) });
      }
    });

    await page.goto('/');
    await page.waitForSelector('table tbody tr.payload-row');
    await page.locator('button.page-btn', { hasText: 'Next' }).click();
    await page.waitForSelector('table tbody tr.payload-row');

    // Switch tier tab — should reset pageToken to ''
    await page.locator('.tier-tab', { hasText: 'RAM' }).click();
    await page.waitForTimeout(300);

    // The request after switching tier must not have a pageToken
    const afterSwitch = tokens[tokens.length - 1];
    expect(afterSwitch).toBe('');
  });

  test('pagination bar is not rendered on empty result', async ({ page }) => {
    await page.route('/v1/payloads**', (route) =>
      route.fulfill({ json: { payloads: [], totalCount: 0, nextPageToken: '' } })
    );

    await page.goto('/');
    await expect(page.locator('p.muted', { hasText: 'No payloads' })).toBeVisible();
    await expect(page.locator('.pagination-bar')).toHaveCount(0);
  });
});
