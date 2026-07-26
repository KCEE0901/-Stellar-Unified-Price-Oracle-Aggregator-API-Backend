import {
  AssetQuerySchema,
  HistoryQuerySchema,
  CursorHistoryQuerySchema,
  OffsetQuerySchema,
  formatValidationResponse,
} from '../services/validation';
import { readAssetPrices, readPriceHistory, readPriceHistoryCursor } from '../services/price-store';
import { buildCursorMeta, applyOffsetPagination } from '../services/pagination';
import { HybridCache } from '../services/cache';
import { cacheHitTotal, cacheMissTotal, lastPriceTimestamp, priceQueriesTotal } from '../middleware/metrics';
import { issueWsCsrfToken, isCsrfEnabled } from '../websocket/csrf';
import { config } from '../config';
import { links, withLinks } from '../services/hypermedia';
import { Router, Request, Response } from 'express';
import { getExportEngine, ExportJobRequest } from '../services/export-engine';
import { conditionalCache } from '../middleware/conditional-cache';

const router = Router();
let pricesCache: HybridCache<any>;

export function initializeCache(cache: HybridCache<any>): void {
  pricesCache = cache;
}

router.use(['/prices', '/prices/:asset'], conditionalCache);

router.get('/', (_req: Request, res: Response) => {
  res.json({
    name: 'Stellar Unified Price Oracle & Aggregator API',
    version: '1.0.0',
    endpoints: {
      prices: '/api/v1/prices',
      price: '/api/v1/prices/:asset',
      history: '/api/v1/history/:asset',
      sources: '/api/v1/sources',
      health: '/api/v1/health',
      healthLive: '/api/v1/health/live',
      healthReady: '/api/v1/health/ready',
      docs: '/api/v1/docs',
      portal: '/portal',
      metrics: '/metrics',
    },
    pagination: {
      history: 'cursor-based (?cursor=<token>&limit=50)',
      sources: 'offset-based (?page=1&limit=20)',
      prices: 'offset-based (?page=1&limit=20)',
    },
  });
});

// GET /prices — offset-paginated list of all asset prices
router.get('/prices', async (req: Request, res: Response) => {
  const assetQuery = AssetQuerySchema.safeParse(req.query);
  if (!assetQuery.success) {
    return res.status(400).json(formatValidationResponse(assetQuery.error));
  }

  const pageQuery = OffsetQuerySchema.safeParse(req.query);
  if (!pageQuery.success) {
    return res.status(400).json(formatValidationResponse(pageQuery.error));
  }

  const { page, limit } = pageQuery.data;
  const cacheKey = `prices:${assetQuery.data.asset || '*'}:p${page}:l${limit}`;
  const cached = await pricesCache.get(cacheKey);
  if (cached) {
    cacheHitTotal.inc();
    return res.json({ success: true, data: cached, cached: true });
  }
  cacheMissTotal.inc();

  const prices = await readAssetPrices();
  const filtered = assetQuery.data.asset
    ? prices.filter((p) => p.asset === assetQuery.data.asset?.toUpperCase())
    : prices;

  for (const p of filtered) {
    priceQueriesTotal.inc({ asset: p.asset });
    lastPriceTimestamp.set({ asset: p.asset }, p.timestamp);
  }

  const { items: paged, meta: pagination } = applyOffsetPagination(filtered, page, limit);

  const aggregated = {
    timestamp: Math.floor(Date.now() / 1000),
    prices: paged,
    pagination,
  };

  await pricesCache.set(cacheKey, aggregated, 'prices');
  res.json({ success: true, data: withLinks(aggregated, links.prices()) });
});

router.get('/prices/:asset', async (req: Request, res: Response) => {
  const asset = req.params.asset.toUpperCase();
  const cacheKey = `price:${asset}`;
  const cached = await pricesCache.get(cacheKey);
  if (cached) {
    cacheHitTotal.inc();
    return res.json({ success: true, data: cached, cached: true });
  }
  cacheMissTotal.inc();

  const prices = await readAssetPrices();
  const price = prices.find((p) => p.asset === asset);

  if (!price) {
    return res.status(404).json({
      success: false,
      error: { code: 'INTERNAL_ERROR', message: 'Failed to fetch price' },
    });
  }

  priceQueriesTotal.inc({ asset });
  lastPriceTimestamp.set({ asset }, price.timestamp);
  await pricesCache.set(cacheKey, price, 'price');
  res.json({ success: true, data: withLinks(price, links.asset(asset)) });
});

// GET /history/:asset — cursor-paginated time-series
router.get('/history/:asset', async (req: Request, res: Response) => {
  const cursorParams = CursorHistoryQuerySchema.safeParse({ ...req.params, ...req.query });
  if (!cursorParams.success) {
    return res.status(400).json(formatValidationResponse(cursorParams.error));
  }

  const { asset, cursor, limit, to } = cursorParams.data;
  const upperAsset = asset.toUpperCase();
  const cacheKey = `history:${upperAsset}:c${cursor || ''}:l${limit}:t${to || 0}`;
  const cached = await pricesCache.get(cacheKey);
  if (cached) {
    cacheHitTotal.inc();
    return res.json({ success: true, data: cached, cached: true });
  }
  cacheMissTotal.inc();

  const history = await readPriceHistoryCursor(upperAsset, cursor, limit, to);
  const pagination = buildCursorMeta(history, limit, 'timestamp');

  const response = {
    asset: upperAsset,
    to: to || null,
    prices: history,
    pagination,
  };

  await pricesCache.set(cacheKey, response, 'history');
  res.json({ success: true, data: response });
});

// GET /history/:asset/legacy — original non-paginated endpoint kept for backward compatibility
router.get('/history/:asset/legacy', async (req: Request, res: Response) => {
  const params = HistoryQuerySchema.safeParse({ ...req.params, ...req.query });
  if (!params.success) {
    return res.status(400).json(formatValidationResponse(params.error));
  }

  const { asset, from, to, limit } = params.data;
  const cacheKey = `history:legacy:${asset.toUpperCase()}:${from || 0}:${to || 0}:${limit}`;
  const cached = await pricesCache.get(cacheKey);
  if (cached) {
    cacheHitTotal.inc();
    return res.json({ success: true, data: cached, cached: true });
  }
  cacheMissTotal.inc();

  const history = await readPriceHistory(asset.toUpperCase(), from, to, limit);
  const response = {
    asset: asset.toUpperCase(),
    from: from || null,
    to: to || null,
    count: history.length,
    prices: history,
  };

  await pricesCache.set(cacheKey, response, 'history');
  res.json({ success: true, data: withLinks(response, links.history(asset)) });
});

// GET /sources — offset-paginated
router.get('/sources', async (req: Request, res: Response) => {
  const pageQuery = OffsetQuerySchema.safeParse(req.query);
  if (!pageQuery.success) {
    return res.status(400).json(formatValidationResponse(pageQuery.error));
  }

  const { page, limit } = pageQuery.data;
  const cacheKey = `sources:p${page}:l${limit}`;
  const cached = await pricesCache.get(cacheKey);
  if (cached) {
    cacheHitTotal.inc();
    return res.json({ success: true, data: cached, cached: true });
  }
  cacheMissTotal.inc();

  const allSources = [
    { name: 'Chainlink', active: true, type: 'off-chain', website: 'https://chain.link' },
    { name: 'Redstone', active: true, type: 'off-chain', website: 'https://redstone.finance' },
    { name: 'Band Protocol', active: true, type: 'off-chain', website: 'https://bandprotocol.com' },
    { name: 'Reflector', active: true, type: 'off-chain', website: 'https://reflector.xyz' },
  ];

  const { items: sources, meta: pagination } = applyOffsetPagination(allSources, page, limit);
  const data = { sources, pagination };

  await pricesCache.set(cacheKey, data, 'sources');
  res.json({ success: true, data });
});

router.get('/health/live', (_req: Request, res: Response) => {
  res.json({ status: 'alive', uptime: process.uptime() });
});

router.get('/health/ready', async (_req: Request, res: Response) => {
  const prices = await readAssetPrices();
  const ready = prices.length > 0;
  res.status(ready ? 200 : 503).json({ status: ready ? 'ready' : 'not_ready', assetsTracked: prices.length });
});

router.get('/health', async (req: Request, res: Response) => {
  const verbose = req.query.verbose === 'true';
  const cacheKey = `health:status:${verbose}`;
  const cached = await pricesCache.get(cacheKey);
  if (cached) {
    cacheHitTotal.inc();
    return res.json({ success: true, data: cached, cached: true });
  }
  cacheMissTotal.inc();

  const prices = await readAssetPrices();
  const hasStale = prices.some((p) => Date.now() / 1000 - p.timestamp > 120);
  const status = prices.length === 0 ? 'unhealthy' : hasStale ? 'degraded' : 'healthy';

  const data: Record<string, any> = {
    service: 'stellar-price-oracle-api',
    status,
    uptime: process.uptime(),
    timestamp: Math.floor(Date.now() / 1000),
    assetsTracked: prices.length,
    degradedAssets: prices.filter((p) => Date.now() / 1000 - p.timestamp > 120).map((p) => p.asset),
    endpoints: {
      liveness: '/api/v1/health/live',
      readiness: '/api/v1/health/ready',
    },
  };

  if (verbose) {
    data.prices = prices.map((p) => ({
      asset: p.asset,
      timestamp: p.timestamp,
      sources: p.sources,
      stale: Date.now() / 1000 - p.timestamp > 120,
    }));
    data.processMemoryMb = Math.round(process.memoryUsage().heapUsed / 1024 / 1024);
    data.nodeVersion = process.version;
  }

  await pricesCache.set(cacheKey, data, 'health');
  res.status(status === 'unhealthy' ? 503 : 200).json({ success: true, data });
});

// ── Issue #123: Streaming Data Export API ────────────────────────────────────

/**
 * POST /v1/exports
 * Create a new export job. Immediately returns job metadata; export runs async.
 */
router.post('/exports', async (req: Request, res: Response) => {
  const { assets, startTime, endTime, format, destination, destinationConfig, schedule, encryptionKeyId } = req.body as ExportJobRequest;

  if (!assets || !Array.isArray(assets) || assets.length === 0) {
    return res.status(400).json({ success: false, error: 'assets must be a non-empty array' });
  }
  if (!startTime || !endTime || endTime <= startTime) {
    return res.status(400).json({ success: false, error: 'invalid startTime/endTime range' });
  }
  const validFormats = ['csv', 'ndjson', 'parquet', 'arrow'];
  if (!validFormats.includes(format)) {
    return res.status(400).json({ success: false, error: `format must be one of: ${validFormats.join(', ')}` });
  }

  const engine = getExportEngine();
  const job = engine.createJob({ assets, startTime, endTime, format, destination: destination ?? 'download', destinationConfig, schedule, encryptionKeyId });
  res.status(202).json({ success: true, data: job });
});

/**
 * GET /v1/exports
 * List all export jobs.
 */
router.get('/exports', (_req: Request, res: Response) => {
  const engine = getExportEngine();
  res.json({ success: true, data: engine.listJobs() });
});

/**
 * GET /v1/exports/:id
 * Get status and progress of an export job.
 */
router.get('/exports/:id', (req: Request, res: Response) => {
  const engine = getExportEngine();
  const job = engine.getJob(req.params.id);
  if (!job) return res.status(404).json({ success: false, error: 'Export job not found' });
  res.json({ success: true, data: job });
});

/**
 * DELETE /v1/exports/:id
 * Cancel a running export job.
 */
router.delete('/exports/:id', (req: Request, res: Response) => {
  const engine = getExportEngine();
  const cancelled = engine.cancelJob(req.params.id);
  if (!cancelled) return res.status(404).json({ success: false, error: 'Job not found or already terminal' });
  res.json({ success: true, message: 'Export job cancelled' });
});

/**
 * GET /v1/exports/:id/download
 * Download a completed export (for destination=download jobs).
 */
router.get('/exports/:id/download', (req: Request, res: Response) => {
  const engine = getExportEngine();
  const job = engine.getJob(req.params.id);
  if (!job) return res.status(404).json({ success: false, error: 'Export job not found' });
  if (job.status !== 'completed') return res.status(409).json({ success: false, error: 'Export not yet completed', status: job.status });

  const token = req.query['token'] as string;
  if (!token) return res.status(400).json({ success: false, error: 'Missing download token' });

  const buffer = engine.getDownloadBuffer(token);
  if (!buffer) return res.status(410).json({ success: false, error: 'Download link expired or not found' });

  const ext = job.request.format === 'csv' ? 'csv' : job.request.format === 'arrow' ? 'arrow' : 'json';
  const contentType =
    job.request.format === 'csv' ? 'text/csv' :
    job.request.format === 'arrow' ? 'application/octet-stream' :
    'application/x-ndjson';

  res.setHeader('Content-Type', contentType);
  res.setHeader('Content-Disposition', `attachment; filename="export-${job.id}.${ext}"`);
  if (job.checksum) res.setHeader('X-Checksum-SHA256', job.checksum);
  res.send(buffer);
});

// ── Issue #124: Data Quality API ─────────────────────────────────────────────

/**
 * GET /v1/sources/:source/quality
 * Returns quality metrics time series for a given source.
 */
router.get('/sources/:source/quality', async (req: Request, res: Response) => {
  const { source } = req.params;
  const { asset, from, to } = req.query as { asset?: string; from?: string; to?: string };

  const validSources = ['chainlink', 'redstone', 'band', 'reflector'];
  if (!validSources.includes(source)) {
    return res.status(400).json({ success: false, error: `source must be one of: ${validSources.join(', ')}` });
  }

  // qualityScorer lives in the aggregator process; the API returns a placeholder
  // when running standalone. In a co-located or shared-state deployment, this
  // would query the aggregator's qualityScorer via IPC / shared cache.
  const fromTs = from ? parseInt(from, 10) : Math.floor(Date.now() / 1000) - 7 * 24 * 3600;
  const toTs = to ? parseInt(to, 10) : Math.floor(Date.now() / 1000);

  res.json({
    success: true,
    data: {
      source,
      asset: asset ?? 'all',
      timeRange: { from: fromTs, to: toTs },
      note: 'Quality metrics are computed by the aggregator. Wire qualityScorer via shared cache for live data.',
      metrics: [],
    },
  });
});

// ── Issue #125: CEP Alert Rules API ──────────────────────────────────────────

// In-process in-memory store for alert rules; the CEP engine runs in the
// aggregator process. A production deployment would persist rules to the DB
// and sync to the aggregator via pub/sub.
const alertRulesStore: Map<string, Record<string, unknown>> = new Map();

/**
 * GET /v1/alerts/rules
 */
router.get('/alerts/rules', (_req: Request, res: Response) => {
  res.json({ success: true, data: [...alertRulesStore.values()] });
});

/**
 * POST /v1/alerts/rules
 */
router.post('/alerts/rules', (req: Request, res: Response) => {
  const body = req.body as Record<string, unknown>;
  const requiredFields = ['name', 'asset', 'category', 'threshold', 'windowSeconds'];
  for (const f of requiredFields) {
    if (body[f] === undefined) {
      return res.status(400).json({ success: false, error: `Missing required field: ${f}` });
    }
  }

  const validCategories = ['cross_source_deviation', 'flash_crash', 'staleness', 'price_velocity', 'volume_anomaly', 'quality_degradation'];
  if (!validCategories.includes(body['category'] as string)) {
    return res.status(400).json({ success: false, error: `category must be one of: ${validCategories.join(', ')}` });
  }

  const { randomUUID } = require('crypto') as typeof import('crypto');
  const rule = {
    id: randomUUID(),
    ...body,
    enabled: body['enabled'] !== false,
    hysteresisCount: body['hysteresisCount'] ?? 3,
    severity: body['severity'] ?? 'warning',
    adaptiveThreshold: body['adaptiveThreshold'] ?? false,
    channels: body['channels'] ?? [],
    createdAt: Date.now(),
    updatedAt: Date.now(),
  };
  alertRulesStore.set(rule.id as string, rule);
  res.status(201).json({ success: true, data: rule });
});

/**
 * PUT /v1/alerts/rules/:id
 */
router.put('/alerts/rules/:id', (req: Request, res: Response) => {
  const existing = alertRulesStore.get(req.params.id);
  if (!existing) return res.status(404).json({ success: false, error: 'Alert rule not found' });
  const updated = { ...existing, ...req.body, id: req.params.id, updatedAt: Date.now() };
  alertRulesStore.set(req.params.id, updated);
  res.json({ success: true, data: updated });
});

/**
 * DELETE /v1/alerts/rules/:id
 */
router.delete('/alerts/rules/:id', (req: Request, res: Response) => {
  if (!alertRulesStore.has(req.params.id)) {
    return res.status(404).json({ success: false, error: 'Alert rule not found' });
  }
  alertRulesStore.delete(req.params.id);
  res.json({ success: true, message: 'Alert rule deleted' });
});

/**
 * GET /v1/alerts/incidents
 * In a real deployment, incidents come from the aggregator's CEP engine.
 */
router.get('/alerts/incidents', (_req: Request, res: Response) => {
  res.json({
    success: true,
    data: [],
    note: 'Live incidents are managed by the aggregator CEP engine. Connect via WebSocket for real-time updates.',
  });
});

export default router;
