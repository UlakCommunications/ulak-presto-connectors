/**
 * Grafana Dashboard Crawler — standalone Node.js script
 *
 * Playwright test framework bypass: direkt chromium API kullanır.
 *
 * Çalıştır:
 *   cd e2e
 *   GRAFANA_URL=https://<grafana-host>/grafana \
 *   GRAFANA_USER=admin GRAFANA_PASS=<password> \
 *   node crawl.js
 */

const { chromium } = require('@playwright/test');
const https  = require('https');
const fs     = require('fs');
const path   = require('path');

const BASE_URL = (process.env.GRAFANA_URL ?? '').replace(/\/$/, '');
const GF_USER  = process.env.GRAFANA_USER ?? 'admin';
const GF_PASS  = process.env.GRAFANA_PASS ?? '';

if (!BASE_URL) { console.error('GRAFANA_URL env var required'); process.exit(1); }
if (!GF_PASS)  { console.error('GRAFANA_PASS env var required'); process.exit(1); }

const SCREENSHOT_DIR = path.join(__dirname, 'screenshots');
const ERRORS_FILE    = path.join(__dirname, 'errors.json');

// Trino/connector hata belirteçleri
const ERROR_MARKERS = [
  'error querying', 'cannot be resolved', 'user_error', 'query failed',
];

// ---------------------------------------------------------------------------

function httpsGet(url, headers) {
  return new Promise((resolve, reject) => {
    const u = new URL(url);
    const opts = {
      hostname: u.hostname, port: u.port || 443, path: u.pathname + u.search,
      method: 'GET', rejectUnauthorized: false,
      headers: headers || {},
    };
    const req = https.request(opts, res => {
      let data = '';
      res.on('data', d => data += d);
      res.on('end', () => { try { resolve(JSON.parse(data)); } catch { resolve(data); } });
    });
    req.on('error', reject);
    req.end();
  });
}

async function listDashboards() {
  const auth = 'Basic ' + Buffer.from(`${GF_USER}:${GF_PASS}`).toString('base64');
  const data = await httpsGet(
    `${BASE_URL}/api/search?type=dash-db&limit=2000`,
    { Authorization: auth }
  );
  return Array.isArray(data) ? data.filter(d => d.uid && d.url) : [];
}

async function main() {
  fs.mkdirSync(SCREENSHOT_DIR, { recursive: true });

  const dashboards = await listDashboards();
  console.log(`\n${dashboards.length} dashboard taranıyor  (${BASE_URL})\n`);

  const browser = await chromium.launch({
    headless: true,
    args: ['--ignore-certificate-errors', '--no-sandbox', '--disable-dev-shm-usage'],
  });
  const ctx  = await browser.newContext({ ignoreHTTPSErrors: true, viewport: { width: 1600, height: 900 }, serviceWorkers: 'block' });
  const page = await ctx.newPage();

  // Login
  await page.goto(`${BASE_URL}/login`, { waitUntil: 'networkidle', timeout: 15000 });
  await page.locator('input[name="user"]').fill(GF_USER);
  await page.locator('input[name="password"]').fill(GF_PASS);
  await page.locator('button[type="submit"]').click();
  await page.waitForURL(u => !u.toString().includes('/login'), { timeout: 10000 });
  await page.waitForLoadState('networkidle', { timeout: 10000 }).catch(() => {});
  await page.waitForTimeout(2000);
  const homeTitle = await page.title().catch(() => '?');
  const homeUrl = page.url().slice(-40);
  console.log(`  ✓ giriş yapıldı  [home: "${homeTitle.slice(0,30)}" url:...${homeUrl}]\n`);

  const errors  = [];
  const ok      = [];

  for (const dash of dashboards) {
    // Slug URL değil UID-only URL: debug testlerde slug URL'ler {} döndürüyordu
    const url = `${BASE_URL}/d/${dash.uid}?from=now-1h&to=now`;
    process.stdout.write(`→ [${dash.uid}] ${dash.title} ... `);

    try {
      await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 30000 });
    } catch (e) {
      console.log(`\n  ✗ navigasyon: ${e}`);
      errors.push({ uid: dash.uid, title: dash.title, errors: [String(e)], screenshot: '' });
      continue;
    }

    // Grafana SPA render için bekle + debug log
    await page.waitForTimeout(10000);
    const pageInfo = await page.evaluate(() => ({
      url: window.location.href.slice(-50),
      title: document.title.slice(0, 40),
      len: document.body.innerText.length
    })).catch(() => ({}));
    process.stdout.write(` [url:...${pageInfo.url||'?'} title:${pageInfo.title||'?'} len:${pageInfo.len||0}] `);

    // Screenshot
    const ssPath = path.join(SCREENSHOT_DIR, `${dash.uid}.png`);
    await page.screenshot({ path: ssPath, fullPage: true });

    // Trino hata taraması
    const bodyText = await page.evaluate(() => document.body.innerText).catch(() => '');
    const lower    = bodyText.toLowerCase();
    const found    = [];

    for (const line of bodyText.split('\n')) {
      const l = line.toLowerCase();
      if (ERROR_MARKERS.some(m => l.includes(m))) {
        found.push(line.trim().slice(0, 300));
      }
    }

    if (found.length > 0) {
      console.log(`✗ ${found.length} Trino hatası`);
      found.forEach(e => console.log(`    ${e}`));
      errors.push({ uid: dash.uid, title: dash.title, errors: found, screenshot: ssPath });
    } else {
      console.log(`✓  →  ${path.basename(ssPath)}`);
      ok.push(dash.title);
    }
  }

  await browser.close();

  // Rapor
  fs.writeFileSync(ERRORS_FILE, JSON.stringify({ ok, errors }, null, 2));

  console.log(`\n${'─'.repeat(60)}`);
  console.log(`TOPLAM  ${dashboards.length} dashboard`);
  console.log(`  ✓ hatasız      : ${ok.length}`);
  console.log(`  ✗ Trino hatası : ${errors.length}`);
  console.log(`\nScreenshotlar : ${SCREENSHOT_DIR}`);
  console.log(`Rapor         : ${ERRORS_FILE}`);

  if (errors.length > 0) {
    console.error('\nHatalı dashboardlar:');
    errors.forEach(d => {
      console.error(`  [${d.title}]`);
      d.errors.forEach(e => console.error(`    ${e}`));
    });
    process.exit(1);
  }
}

main().catch(e => { console.error(e); process.exit(1); });
