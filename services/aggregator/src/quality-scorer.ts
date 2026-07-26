/**
 * Multi-dimensional data quality scoring engine.
 * Addresses Issue #124: 8 quality dimensions, SPC control charts,
 * automated source-weight adjustment, and quality-aware API responses.
 */

import { logger } from './utils/logger';
import { OracleSourceName, AggregatedPrice } from './types';

export type FeedGrade = 'A+' | 'A' | 'B' | 'C' | 'D' | 'F';

export interface SourceQualityMetrics {
  source: OracleSourceName;
  asset: string;
  /** Dimension 1: % of polls where source responded within timeout */
  uptime: number;
  /** Dimension 2: Mean absolute percentage error vs aggregated median */
  accuracy: number;
  /** Dimension 3: Latency between source timestamp and aggregator receipt, ms */
  timeliness: number;
  /** Dimension 4: Variance of submissions in rolling window */
  consistency: number;
  /** Dimension 5: Lag vs expected cadence in seconds (0 = perfectly fresh) */
  freshnessLag: number;
  /** Dimension 6: Signed % deviation from median (+ = consistently over) */
  bias: number;
  /** Dimension 7: Number of assets this source provides */
  availability: number;
  /** Dimension 8: Rate of malformed/missing/out-of-range values (0–1) */
  responseIntegrity: number;
  /** Composite score 0–100 */
  compositeScore: number;
  /** Current sampling weight in quality-weighted median (0–1) */
  weight: number;
  computedAt: number;
}

export interface QualityWeightMode {
  mode: 'equal' | 'quality_weighted' | 'custom';
  customWeights?: Partial<Record<OracleSourceName, number>>;
}

interface SpcState {
  /** Individual measurements */
  values: number[];
  /** Moving ranges */
  movingRanges: number[];
  /** Control chart center line */
  mean: number;
  /** Upper control limit */
  ucl: number;
  /** Lower control limit */
  lcl: number;
  /** Process capability index (Cpk proxy) */
  cpk: number;
  /** Western Electric rule violations */
  violations: string[];
}

interface SubmissionRecord {
  price: number;
  timestamp: number;
  responseTime: number;
  isValid: boolean;
}

export class QualityScorer {
  /** key: `${source}:${asset}` */
  private submissions: Map<string, SubmissionRecord[]> = new Map();
  private pollAttempts: Map<string, { total: number; success: number }> = new Map();
  private assetCounts: Map<OracleSourceName, Set<string>> = new Map();
  private spcState: Map<string, SpcState> = new Map();
  private weights: Map<string, number> = new Map();
  private qualityHistory: Map<string, SourceQualityMetrics[]> = new Map();
  private weightAuditLog: Array<{ timestamp: number; source: OracleSourceName; asset: string; oldWeight: number; newWeight: number; reason: string }> = [];
  private readonly windowSize = 100;
  private readonly expectedCadenceSeconds = 60;
  private readonly minWeight = 0.05;
  private readonly maxWeight = 1.0;
  private readonly weightDecayRate = 0.1;
  private readonly weightRecoveryRate = 0.05;

  // ── Ingest ───────────────────────────────────────────────────────────────────

  recordSubmission(
    source: OracleSourceName,
    asset: string,
    price: number,
    timestamp: number,
    responseTimeMs: number,
    isValid = true,
  ): void {
    const key = `${source}:${asset}`;
    const records = this.submissions.get(key) ?? [];
    records.push({ price, timestamp, responseTime: responseTimeMs, isValid });
    if (records.length > this.windowSize) records.shift();
    this.submissions.set(key, records);

    // Track poll success
    const poll = this.pollAttempts.get(key) ?? { total: 0, success: 0 };
    poll.total++;
    if (isValid) poll.success++;
    this.pollAttempts.set(key, poll);

    // Track asset coverage
    const assets = this.assetCounts.get(source) ?? new Set();
    assets.add(asset.toUpperCase());
    this.assetCounts.set(source, assets);
  }

  recordPollFailure(source: OracleSourceName, asset: string): void {
    const key = `${source}:${asset}`;
    const poll = this.pollAttempts.get(key) ?? { total: 0, success: 0 };
    poll.total++;
    this.pollAttempts.set(key, poll);
  }

  // ── Score computation ────────────────────────────────────────────────────────

  computeMetrics(source: OracleSourceName, asset: string, aggregatedMedian: number, now = Date.now()): SourceQualityMetrics {
    const key = `${source}:${asset}`;
    const records = this.submissions.get(key) ?? [];
    const poll = this.pollAttempts.get(key) ?? { total: 1, success: 1 };

    // D1: Uptime
    const uptime = (poll.success / poll.total) * 100;

    // D2: Accuracy (MAPE vs median)
    let accuracy = 100;
    if (records.length > 0 && aggregatedMedian > 0) {
      const mape = records.reduce((sum, r) => sum + Math.abs(r.price - aggregatedMedian) / aggregatedMedian, 0) / records.length * 100;
      accuracy = Math.max(0, 100 - mape);
    }

    // D3: Timeliness (median response time)
    const timeliness = records.length > 0
      ? records.reduce((s, r) => s + r.responseTime, 0) / records.length
      : 0;

    // D4: Consistency (variance of prices in window)
    let consistency = 100;
    if (records.length > 1) {
      const prices = records.map((r) => r.price);
      const mean = prices.reduce((s, p) => s + p, 0) / prices.length;
      const variance = prices.reduce((s, p) => s + (p - mean) ** 2, 0) / prices.length;
      const cv = mean > 0 ? Math.sqrt(variance) / mean * 100 : 0;
      consistency = Math.max(0, 100 - cv);
    }

    // D5: Freshness lag (seconds since last submission vs expected cadence)
    const lastRecord = records[records.length - 1];
    const freshnessLag = lastRecord
      ? Math.max(0, (now / 1000 - lastRecord.timestamp) - this.expectedCadenceSeconds)
      : this.expectedCadenceSeconds * 10;

    // D6: Bias (signed % deviation from median)
    let bias = 0;
    if (records.length > 0 && aggregatedMedian > 0) {
      const meanPrice = records.reduce((s, r) => s + r.price, 0) / records.length;
      bias = ((meanPrice - aggregatedMedian) / aggregatedMedian) * 100;
    }

    // D7: Availability (asset count)
    const availability = this.assetCounts.get(source)?.size ?? 1;

    // D8: Response integrity (rate of valid submissions)
    const responseIntegrity = records.length > 0
      ? records.filter((r) => r.isValid).length / records.length
      : 1;

    // Composite score (weighted average of dimensions, normalized to 0–100)
    const freshnessScore = Math.max(0, 100 - (freshnessLag / this.expectedCadenceSeconds) * 100);
    const timelinessScore = Math.max(0, 100 - timeliness / 50); // penalize >5s response
    const biasScore = Math.max(0, 100 - Math.abs(bias) * 10);
    const availabilityScore = Math.min(100, availability * 25);

    const compositeScore = (
      uptime * 0.20 +
      accuracy * 0.25 +
      timelinessScore * 0.10 +
      consistency * 0.10 +
      freshnessScore * 0.15 +
      biasScore * 0.05 +
      availabilityScore * 0.05 +
      responseIntegrity * 100 * 0.10
    );

    const currentWeight = this.weights.get(key) ?? 1.0;
    const metrics: SourceQualityMetrics = {
      source,
      asset,
      uptime,
      accuracy,
      timeliness,
      consistency,
      freshnessLag,
      bias,
      availability,
      responseIntegrity,
      compositeScore,
      weight: currentWeight,
      computedAt: now,
    };

    // Update SPC state
    this.updateSpc(key, compositeScore);

    // Auto-adjust weight
    this.adjustWeight(source, asset, key, compositeScore);
    metrics.weight = this.weights.get(key) ?? currentWeight;

    // Store history
    const history = this.qualityHistory.get(key) ?? [];
    history.push(metrics);
    if (history.length > 1000) history.shift();
    this.qualityHistory.set(key, history);

    return metrics;
  }

  // ── Source-weight adjustment ─────────────────────────────────────────────────

  private adjustWeight(source: OracleSourceName, asset: string, key: string, score: number): void {
    const currentWeight = this.weights.get(key) ?? 1.0;
    let newWeight = currentWeight;
    let reason = '';

    if (score < 40) {
      newWeight = Math.max(this.minWeight, currentWeight - this.weightDecayRate);
      reason = `Quality score ${score.toFixed(1)} < 40 threshold`;
    } else if (score < 60) {
      newWeight = Math.max(this.minWeight, currentWeight - this.weightDecayRate * 0.5);
      reason = `Quality score ${score.toFixed(1)} < 60 threshold`;
    } else if (score >= 80 && currentWeight < this.maxWeight) {
      newWeight = Math.min(this.maxWeight, currentWeight + this.weightRecoveryRate);
      reason = `Quality score ${score.toFixed(1)} recovered above 80`;
    }

    if (Math.abs(newWeight - currentWeight) > 0.001) {
      this.weights.set(key, newWeight);
      this.weightAuditLog.push({ timestamp: Date.now(), source, asset, oldWeight: currentWeight, newWeight, reason });
      logger.info({ source, asset, oldWeight: currentWeight, newWeight, reason }, 'Source weight adjusted');
    }
  }

  getWeightAuditLog() {
    return [...this.weightAuditLog];
  }

  // ── SPC control charts ───────────────────────────────────────────────────────

  private updateSpc(key: string, value: number): void {
    const state = this.spcState.get(key) ?? {
      values: [],
      movingRanges: [],
      mean: 0,
      ucl: 0,
      lcl: 0,
      cpk: 0,
      violations: [],
    };

    state.values.push(value);
    if (state.values.length > 50) state.values.shift();

    if (state.values.length > 1) {
      const last = state.values[state.values.length - 1] ?? value;
      const prev = state.values[state.values.length - 2] ?? value;
      state.movingRanges.push(Math.abs(last - prev));
      if (state.movingRanges.length > 49) state.movingRanges.shift();
    }

    if (state.values.length >= 5) {
      const n = state.values.length;
      state.mean = state.values.reduce((s, v) => s + v, 0) / n;
      const avgMr = state.movingRanges.length > 0
        ? state.movingRanges.reduce((s, r) => s + r, 0) / state.movingRanges.length
        : 0;
      const d2 = 1.128; // for n=2 subgroups (individuals chart)
      const sigma = avgMr / d2;
      state.ucl = state.mean + 3 * sigma;
      state.lcl = Math.max(0, state.mean - 3 * sigma);

      // Cpk: assume spec limits are 0 (LSL) and 100 (USL)
      const usl = 100, lsl = 0;
      state.cpk = sigma > 0
        ? Math.min((usl - state.mean) / (3 * sigma), (state.mean - lsl) / (3 * sigma))
        : 0;

      // Western Electric rules
      state.violations = this.westernElectricRules(state.values, state.mean, sigma);
    }

    this.spcState.set(key, state);

    if (state.violations.length > 0) {
      logger.warn({ key, violations: state.violations }, 'SPC out-of-control signal detected');
    }
  }

  private westernElectricRules(values: number[], mean: number, sigma: number): string[] {
    const violations: string[] = [];
    const n = values.length;
    if (n < 8) return violations;

    const last = values[n - 1] ?? mean;
    // Rule 1: 1 point beyond 3σ
    if (Math.abs(last - mean) > 3 * sigma) violations.push('Rule1: point beyond 3σ');

    // Rule 2: 9 consecutive points on same side of mean
    if (n >= 9) {
      const last9 = values.slice(-9);
      if (last9.every((v) => v > mean) || last9.every((v) => v < mean)) {
        violations.push('Rule2: 9 consecutive points same side');
      }
    }

    // Rule 3: 6 consecutive points trending in one direction
    if (n >= 6) {
      const last6 = values.slice(-6);
      let ascending = true, descending = true;
      for (let i = 1; i < last6.length; i++) {
        if ((last6[i] ?? 0) <= (last6[i - 1] ?? 0)) ascending = false;
        if ((last6[i] ?? 0) >= (last6[i - 1] ?? 0)) descending = false;
      }
      if (ascending || descending) violations.push('Rule3: 6 consecutive trending');
    }

    return violations;
  }

  getSpcState(source: OracleSourceName, asset: string): SpcState | undefined {
    return this.spcState.get(`${source}:${asset}`);
  }

  // ── Quality history ──────────────────────────────────────────────────────────

  getQualityHistory(source: OracleSourceName, asset: string): SourceQualityMetrics[] {
    return this.qualityHistory.get(`${source}:${asset}`) ?? [];
  }

  // ── Weighted median helpers ──────────────────────────────────────────────────

  computeWeightedMedian(
    prices: Array<{ source: OracleSourceName; price: number }>,
    mode: QualityWeightMode,
    asset: string,
  ): number {
    if (prices.length === 0) return 0;

    const weighted = prices.map((p) => {
      let w = 1.0;
      if (mode.mode === 'quality_weighted') {
        w = this.weights.get(`${p.source}:${asset}`) ?? 1.0;
      } else if (mode.mode === 'custom') {
        w = mode.customWeights?.[p.source] ?? 1.0;
      }
      return { price: p.price, weight: w };
    });

    const totalWeight = weighted.reduce((s, w) => s + w.weight, 0);
    if (totalWeight === 0) return prices[0]?.price ?? 0;

    weighted.sort((a, b) => a.price - b.price);
    let cumulative = 0;
    for (const w of weighted) {
      cumulative += w.weight / totalWeight;
      if (cumulative >= 0.5) return w.price;
    }
    return weighted[weighted.length - 1]?.price ?? 0;
  }

  // ── Feed grade ───────────────────────────────────────────────────────────────

  computeFeedGrade(metricsArray: SourceQualityMetrics[]): FeedGrade {
    if (metricsArray.length === 0) return 'F';
    const avg = metricsArray.reduce((s, m) => s + m.compositeScore, 0) / metricsArray.length;
    if (avg >= 95) return 'A+';
    if (avg >= 85) return 'A';
    if (avg >= 70) return 'B';
    if (avg >= 55) return 'C';
    if (avg >= 40) return 'D';
    return 'F';
  }

  computeConfidenceInterval(metricsArray: SourceQualityMetrics[], priceNum: number): [number, number] {
    if (metricsArray.length === 0) return [priceNum, priceNum];
    const avgMape = metricsArray.reduce((s, m) => s + (100 - m.accuracy), 0) / metricsArray.length / 100;
    const margin = priceNum * avgMape * 2;
    return [priceNum - margin, priceNum + margin];
  }
}

/** Singleton instance */
export const qualityScorer = new QualityScorer();
