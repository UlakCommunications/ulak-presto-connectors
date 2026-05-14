/**
 * Grafana Dashboard Crawler — QW connector regression detector
 *
 * Visits every dashboard in the Grafana instance, waits for all panels to
 * settle, and reports panel-level errors (COLUMN_NOT_FOUND, query failures,
 * etc.). Designed to run against the real cluster after each connector deploy.
 *
 * Usage:
 *   cd e2e && npm install && npx playwright install chromium
 *
 *   GRAFANA_URL=https://10.20.4.165/grafana \
 *   GRAFANA_USER=admin GRAFANA_PASS=admin1 \
 *   npm run crawl
 *
 * Environment variables:
 *   GRAFANA_URL   — base URL of Grafana (no trailing slash)
 *   GRAFANA_USER  — Grafana admin username  (default: admin)
 *   GRAFANA_PASS  — Grafana admin password  (default: admin1)
 *   PANEL_WAIT_MS — max ms to wait for panels to load (default: 30000)
 */

import { test, request, Page } from '@playwright/test';
import * as fs from 'fs';
import * as path from 'path';

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

const BASE_URL      = (process.env.GRAFANA_URL  ?? 'https://10.20.4.165/grafana').replace(/\/$/, '');
const GF_USER       = process.env.GRAFANA_USER  ?? 'admin';
const GF_PASS       = process.env.GRAFANA_PASS  ?? 'admin1';
const PANEL_WAIT_MS = Number(process.env.PANEL_WAIT_MS ?? 30_000);
const POST_LOAD_MS  = 4_000;
const TIME_RANGE    = 'from=now-30m&to=now&refresh=';

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface DashboardMeta {
  uid:   string;
  title: string;
  url:   string;
}

interface PanelError {
  panel:   string;
  message: string;
}

// ---------------------------------------------------------------------------
// Grafana REST API helpers
// ---------------------------------------------------------------------------

async function listAllDashboards(): Promise<DashboardMeta[]> {
  const ctx = await request.newContext({ ignoreHTTPSErrors: true });
  try {
    const resp = await ctx.get(`${BASE_URL}/api/search`, {
      params: { type: 'dash-db', limit: '2000' },
      headers: { Authorization: 'Basic ' + Buffer.from(`${GF_USER}:${GF_PASS}`).toString('base64') },
    });
    if (!resp.ok()) throw new Error(`Grafana API ${resp.status()}: ${await resp.text()}`);
    const items: DashboardMeta[] = await resp.json();
    return items.filter(d => d.uid && d.url);
  } finally {
    await ctx.dispose();
  }
}

// ---------------------------------------------------------------------------
// Browser helpers
// ---------------------------------------------------------------------------

async function login(page: Page): Promise<void> {
  await page.goto(`${BASE_URL}/login`, { waitUntil: 'domcontentloaded', timeout: 20_000 });

  const userField = page.locator('input[name="user"], [data-testid="data-testid Username input field"]').first();
  const passField = page.locator('input[name="password"], [data-testid="data-testid Password input field"]').first();
  await userField.fill(GF_USER);
  await passField.fill(GF_PASS);
  await page.locator('button[type="submit"], [data-testid="data-testid Login button"]').first().click();

  // Dismiss optional "change password" screen
  try {
    await page.locator('[data-testid="data-testid Skip change password button"]')
              .waitFor({ timeout: 3_000 });
    await page.locator('[data-testid="data-testid Skip change password button"]').click();
  } catch { /* not shown */ }

  await page.waitForURL(url => !url.toString().includes('/login'), { timeout: 15_000 });
  console.log('  ✓ logged in');
}

async function waitForPanels(page: Page): Promise<void> {
  // Wait for loading spinners to disappear
  const spinnerSel = [
    '[aria-label="Panel loading bar"]',
    '[data-testid="panel-loading-bar"]',
    '.panel-loading',
    '.loading-indicator',
  ].join(', ');

  const deadline = Date.now() + PANEL_WAIT_MS;
  while (Date.now() < deadline) {
    const count = await page.locator(spinnerSel).count();
    if (count === 0) break;
    await page.waitForTimeout(500);
  }
  await page.waitForLoadState('networkidle', { timeout: 10_000 }).catch(() => {});
  await page.waitForTimeout(POST_LOAD_MS);
}

async function collectPanelErrors(page: Page): Promise<PanelError[]> {
  const raw: PanelError[] = [];

  // ── Strategy 1: Grafana 9/10 error corner icons ─────────────────────────
  const errorIconSels = [
    '[data-testid="panel-error"]',
    '.panel-info-corner--error',
    '[aria-label*="Error"]',
    '[class*="panel-status"]',
  ].join(', ');
  const icons = page.locator(errorIconSels);
  for (let i = 0; i < await icons.count(); i++) {
    try {
      await icons.nth(i).hover({ timeout: 800 });
      await page.waitForTimeout(400);
      const tooltip = page.locator('[role="tooltip"], .popper-content, .grafana-tooltip, [class*="tooltip"]').last();
      const msg = (await tooltip.innerText({ timeout: 800 }).catch(() => '')).trim();
      if (msg) raw.push({ panel: `error-icon-${i}`, message: msg });
    } catch { /* no tooltip */ }
  }

  // ── Strategy 2: visible error text inside panel bodies ───────────────────
  const panelSels = [
    '[data-testid="panel-content"]',
    '.panel-content',
    '.react-grid-item',
  ].join(', ');
  const panels = page.locator(panelSels);
  for (let i = 0; i < await panels.count(); i++) {
    try {
      const text = (await panels.nth(i).innerText({ timeout: 500 })).toLowerCase();
      if (
        text.includes('error querying') ||
        text.includes('cannot be resolved') ||
        text.includes('user_error') ||
        text.includes('column') && text.includes('cannot')
      ) {
        raw.push({ panel: `panel-body-${i}`, message: text.slice(0, 400) });
      }
    } catch { /* stale element */ }
  }

  // ── Strategy 3: browser console errors collected via page.on earlier ────
  // (console errors are collected outside this function — see test body)

  // Deduplicate by message prefix
  const seen = new Set<string>();
  return raw.filter(e => {
    const k = e.message.slice(0, 160);
    if (seen.has(k)) return false;
    seen.add(k);
    return true;
  });
}

// ---------------------------------------------------------------------------
// Main test
// ---------------------------------------------------------------------------

test('crawl all Grafana dashboards', async ({ page }) => {
  test.setTimeout(600_000); // 10 min max total

  // Ensure screenshot output directory exists
  const screenshotDir = path.join(__dirname, 'dashboard-report', 'errors');
  fs.mkdirSync(screenshotDir, { recursive: true });

  // Collect browser console errors
  const consoleErrors: string[] = [];
  page.on('console', msg => {
    if (msg.type() === 'error') consoleErrors.push(msg.text());
  });

  // ── Login ─────────────────────────────────────────────────────────────────
  await login(page);

  // ── List dashboards ───────────────────────────────────────────────────────
  const dashboards = await listAllDashboards();
  console.log(`\nCrawling ${dashboards.length} dashboards against ${BASE_URL}\n`);

  const allErrors: Record<string, { panel: string; message: string }[]> = {};

  // ── Visit each dashboard ──────────────────────────────────────────────────
  for (const dash of dashboards) {
    consoleErrors.length = 0; // reset per dashboard
    const dashUrl = `${BASE_URL}${dash.url}?${TIME_RANGE}`;
    console.log(`→ [${dash.uid}] ${dash.title}`);

    try {
      await page.goto(dashUrl, { waitUntil: 'domcontentloaded', timeout: 30_000 });
    } catch (e) {
      allErrors[dash.title] = [{ panel: 'navigation', message: String(e) }];
      console.log(`  ✗ navigation failed: ${e}`);
      continue;
    }

    await waitForPanels(page);
    const panelErrors = await collectPanelErrors(page);

    // Also surface Trino-specific console errors
    const trinoConsoleErrors = consoleErrors
      .filter(e => e.toLowerCase().includes('error') || e.toLowerCase().includes('column'))
      .map((e, i) => ({ panel: `console-${i}`, message: e.slice(0, 400) }));

    const allDashErrors = [...panelErrors, ...trinoConsoleErrors];

    if (allDashErrors.length > 0) {
      allErrors[dash.title] = allDashErrors;
      console.log(`  ✗ ${allDashErrors.length} error(s) found:`);
      allDashErrors.forEach(e => console.log(`    [${e.panel}] ${e.message.slice(0, 200)}`));
      const screenshotPath = path.join(screenshotDir, `${dash.uid}.png`);
      await page.screenshot({ path: screenshotPath, fullPage: true });
      console.log(`    → screenshot: ${screenshotPath}`);
    } else {
      console.log('  ✓ no errors');
    }
  }

  // ── Final report ──────────────────────────────────────────────────────────
  const errorDashes = Object.keys(allErrors);
  console.log(`\n${'─'.repeat(60)}`);
  console.log(`SUMMARY: ${errorDashes.length} / ${dashboards.length} dashboards have errors`);
  if (errorDashes.length > 0) {
    errorDashes.forEach(d => {
      console.log(`\n  ✗ ${d}`);
      allErrors[d].forEach(e => console.log(`      [${e.panel}] ${e.message.slice(0, 200)}`));
    });

    // Write JSON report
    const reportPath = path.join(__dirname, 'dashboard-report', 'errors.json');
    fs.writeFileSync(reportPath, JSON.stringify(allErrors, null, 2));
    console.log(`\nDetailed errors written to: ${reportPath}`);

    throw new Error(
      `${errorDashes.length} dashboard(s) have panel errors. See dashboard-report/errors.json`
    );
  }
  console.log('All dashboards OK ✓');
});
