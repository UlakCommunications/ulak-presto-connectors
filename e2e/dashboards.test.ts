/**
 * Grafana Dashboard Crawler
 *
 * - Headed Chrome: her dashboard'u açar, yüklenmesini bekler
 * - Tam sayfa screenshot alır (veri geldi mi görmek için)
 * - Trino hata metinlerini tarar (COLUMN_NOT_FOUND, USER_ERROR, vb.)
 * - Final: errors.json + screenshots/ klasörü
 *
 * Çalıştır:
 *   cd e2e
 *   GRAFANA_URL=https://<grafana-host>/grafana \
 *   GRAFANA_USER=admin GRAFANA_PASS=<password> \
 *   npm run crawl
 */

import { test, request, Page } from '@playwright/test';
import * as fs from 'fs';
import * as path from 'path';

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

const BASE_URL   = (process.env.GRAFANA_URL ?? '').replace(/\/$/, '');
const GF_USER    = process.env.GRAFANA_USER  ?? 'admin';
const GF_PASS    = process.env.GRAFANA_PASS  ?? '';
const WAIT_MS    = Number(process.env.PANEL_WAIT_MS ?? 20_000);
const TIME_RANGE = 'from=now-1h&to=now&refresh=';

// Trino/connector kaynaklı hata belirteçleri
const TRINO_ERROR_MARKERS = [
  'error querying',
  'cannot be resolved',
  'user_error',
  'column',      // "Column X cannot be resolved" için
  'query failed',
];

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface DashMeta  { uid: string; title: string; url: string; }
interface DashError { uid: string; title: string; errors: string[]; screenshot: string; }

// ---------------------------------------------------------------------------
// Grafana API
// ---------------------------------------------------------------------------

async function listDashboards(): Promise<DashMeta[]> {
  const ctx = await request.newContext({ ignoreHTTPSErrors: true });
  try {
    const r = await ctx.get(`${BASE_URL}/api/search`, {
      params: { type: 'dash-db', limit: '2000' },
      headers: { Authorization: 'Basic ' + Buffer.from(`${GF_USER}:${GF_PASS}`).toString('base64') },
    });
    if (!r.ok()) throw new Error(`${r.status()}`);
    return (await r.json() as DashMeta[]).filter(d => d.uid && d.url);
  } finally { await ctx.dispose(); }
}

// ---------------------------------------------------------------------------
// Browser helpers
// ---------------------------------------------------------------------------

async function login(page: Page) {
  await page.goto(`${BASE_URL}/login`, { waitUntil: 'domcontentloaded', timeout: 20_000 });
  await page.locator('input[name="user"], [data-testid="data-testid Username input field"]').first().fill(GF_USER);
  await page.locator('input[name="password"], [data-testid="data-testid Password input field"]').first().fill(GF_PASS);
  await page.locator('button[type="submit"], [data-testid="data-testid Login button"]').first().click();
  try {
    const skip = page.locator('[data-testid="data-testid Skip change password button"]');
    await skip.waitFor({ timeout: 3_000 });
    await skip.click();
  } catch { /* yok */ }
  await page.waitForURL(u => !u.toString().includes('/login'), { timeout: 15_000 });
  console.log('  ✓ giriş yapıldı');
}

/** Yükleme bitti mi: spinner yoksa + networkidle + 5 sn settle */
async function waitLoaded(page: Page) {
  const spinSel = [
    '[aria-label="Panel loading bar"]',
    '[data-testid="panel-loading-bar"]',
    '.panel-loading',
    '.loading-indicator',
  ].join(', ');

  const deadline = Date.now() + WAIT_MS;
  while (Date.now() < deadline) {
    if (await page.locator(spinSel).count() === 0) break;
    await page.waitForTimeout(700);
  }
  await page.waitForLoadState('networkidle', { timeout: 15_000 }).catch(() => {});
  await page.waitForTimeout(5_000);   // chart render için biraz daha bekle
}

/** Sayfada Trino hata metni var mı? Varsa döndür. */
async function findTrinoErrors(page: Page): Promise<string[]> {
  const bodyText = await page.evaluate(() => document.body.innerText).catch(() => '');
  const lower    = bodyText.toLowerCase();

  const found: string[] = [];

  // "cannot be resolved" içeren satırları yakala
  for (const line of bodyText.split('\n')) {
    const l = line.toLowerCase();
    if (
      l.includes('error querying') ||
      (l.includes('column') && l.includes('cannot')) ||
      l.includes('user_error') ||
      l.includes('query failed')
    ) {
      found.push(line.trim().slice(0, 300));
    }
  }

  return [...new Set(found)];
}

// ---------------------------------------------------------------------------
// Ana test
// ---------------------------------------------------------------------------

test('Grafana dashboard crawler', async ({ browser }) => {
  test.setTimeout(600_000);

  // Playwright fixture baseURL ayarı SSL/redirect sorununa yol açıyor.
  // Bağımsız context ile debug node script'tekiyle aynı davranış elde ediyoruz.
  const ctx  = await browser.newContext({ ignoreHTTPSErrors: true, viewport: { width: 1600, height: 900 } });
  const page = await ctx.newPage();

  // dashboard-report/ HTML reporter tarafından temizleniyor — ayrı dizin kullan
  const screenshotDir = path.join(__dirname, 'screenshots');
  fs.mkdirSync(screenshotDir, { recursive: true });

  await login(page);

  const dashboards = await listDashboards();
  console.log(`\n${dashboards.length} dashboard taranıyor  (${BASE_URL})\n`);

  const errors:  DashError[] = [];
  const noError: string[]    = [];

  for (const dash of dashboards) {
    const url = `${BASE_URL}${dash.url}?${TIME_RANGE}`;
    console.log(`→ [${dash.uid}] ${dash.title}`);

    try {
      await page.goto(url, { waitUntil: 'networkidle', timeout: 45_000 });
    } catch (e) {
      console.log(`  ✗ navigasyon hatası: ${e}`);
      errors.push({ uid: dash.uid, title: dash.title, errors: [String(e)], screenshot: '' });
      continue;
    }

    await waitLoaded(page);

    // Tam sayfa screenshot — veri görünüyor mu elle kontrol edilebilir
    const ssPath = path.join(screenshotDir, `${dash.uid}.png`);
    await page.screenshot({ path: ssPath, fullPage: true });

    // Trino hata taraması
    const trinoErrors = await findTrinoErrors(page);

    if (trinoErrors.length > 0) {
      console.log(`  ✗ ${trinoErrors.length} Trino hatası:`);
      trinoErrors.forEach(e => console.log(`    ${e}`));
      errors.push({ uid: dash.uid, title: dash.title, errors: trinoErrors, screenshot: ssPath });
    } else {
      console.log(`  ✓ hata yok  →  ${ssPath}`);
      noError.push(dash.title);
    }
  }

  // ── Rapor ─────────────────────────────────────────────────────────────────
  const reportPath = path.join(__dirname, 'errors.json');
  fs.writeFileSync(reportPath, JSON.stringify({ ok: noError, errors }, null, 2));

  console.log(`\n${'─'.repeat(60)}`);
  console.log(`TOPLAM  ${dashboards.length} dashboard`);
  console.log(`  ✓ hatasız   : ${noError.length}`);
  console.log(`  ✗ Trino hatası: ${errors.length}`);
  console.log(`\nScreenshotlar : ${screenshotDir}`);
  console.log(`Rapor         : ${reportPath}`);

  if (errors.length > 0) {
    const summary = errors.map(d =>
      `  [${d.title}]\n${d.errors.map(e => '    ' + e).join('\n')}`
    ).join('\n');
    throw new Error(`${errors.length} dashboard Trino hatası var:\n${summary}`);
  }
});
