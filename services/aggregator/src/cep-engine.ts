/**
 * Complex Event Processing (CEP) engine for real-time price alerts.
 * Addresses Issue #125: CEP engine with adaptive ML thresholds, multi-channel
 * delivery, alert correlation/deduplication, hysteresis, and incident management.
 */

import crypto from 'crypto';
import { EventEmitter } from 'events';
import { logger } from './utils/logger';
import { AggregatedPrice, OracleSourceName } from './types';

export type AlertRuleCategory =
  | 'cross_source_deviation'
  | 'flash_crash'
  | 'staleness'
  | 'price_velocity'
  | 'volume_anomaly'
  | 'quality_degradation';

export type AlertSeverity = 'info' | 'warning' | 'critical';
export type IncidentStatus = 'firing' | 'acknowledged' | 'investigating' | 'resolved';

export interface AlertChannel {
  type: 'webhook' | 'email' | 'slack' | 'pagerduty';
  config: Record<string, string>;
}

export interface AlertRule {
  id: string;
  name: string;
  /** Asset pair, e.g. "BTC/USD", or "*" for all */
  asset: string;
  category: AlertRuleCategory;
  severity: AlertSeverity;
  threshold: number;
  /** Sliding window length in seconds */
  windowSeconds: number;
  /** Number of consecutive evaluations condition must hold before firing */
  hysteresisCount: number;
  channels: AlertChannel[];
  /** When true, threshold is adjusted by the learned normal pattern */
  adaptiveThreshold: boolean;
  enabled: boolean;
  createdAt: number;
  updatedAt: number;
}

export interface AlertIncident {
  id: string;
  ruleId: string;
  asset: string;
  status: IncidentStatus;
  firedAt: number;
  resolvedAt?: number;
  acknowledgedAt?: number;
  /** Hash of (ruleId + asset + windowBucket) for deduplication */
  deduplicationKey: string;
  correlatedIncidentIds: string[];
  /** SHA-256 of incident record for tamper-evident audit log */
  auditHash: string;
  details: Record<string, unknown>;
}

interface AdaptiveThresholdState {
  /** Rolling mean of the metric per hour-of-day slot */
  hourlyMean: number[];
  /** Rolling std-dev per hour-of-day slot */
  hourlyStdDev: number[];
  sampleCount: number[];
}

interface HysteresisState {
  consecutiveHits: number;
  consecutiveMisses: number;
}

export class CepEngine extends EventEmitter {
  private rules: Map<string, AlertRule> = new Map();
  private incidents: Map<string, AlertIncident> = new Map();
  /** key: `${ruleId}:${asset}` */
  private hysteresis: Map<string, HysteresisState> = new Map();
  private adaptiveState: Map<string, AdaptiveThresholdState> = new Map();
  /** key: deduplicationKey → last fired timestamp */
  private suppressionWindows: Map<string, number> = new Map();
  /** Price windows: key `${asset}` → array of {price, timestamp, source} */
  private priceWindows: Map<string, Array<{ price: number; timestamp: number; source: OracleSourceName }>> = new Map();
  /** Per-source submission count windows: key `${source}:${asset}` → timestamps */
  private submissionCounts: Map<string, number[]> = new Map();
  private readonly suppressionWindowMs = 60_000;
  private readonly correlationWindowMs = 30_000;

  // ── Rule CRUD ────────────────────────────────────────────────────────────────

  addRule(rule: AlertRule): void {
    this.rules.set(rule.id, rule);
    logger.info({ ruleId: rule.id, category: rule.category, asset: rule.asset }, 'CEP rule added');
  }

  updateRule(id: string, patch: Partial<AlertRule>): AlertRule | null {
    const existing = this.rules.get(id);
    if (!existing) return null;
    const updated: AlertRule = { ...existing, ...patch, id, updatedAt: Date.now() };
    this.rules.set(id, updated);
    return updated;
  }

  deleteRule(id: string): boolean {
    return this.rules.delete(id);
  }

  getRule(id: string): AlertRule | undefined {
    return this.rules.get(id);
  }

  listRules(): AlertRule[] {
    return [...this.rules.values()];
  }

  // ── Incident management ──────────────────────────────────────────────────────

  listIncidents(status?: IncidentStatus): AlertIncident[] {
    const all = [...this.incidents.values()];
    return status ? all.filter((i) => i.status === status) : all;
  }

  acknowledgeIncident(id: string): AlertIncident | null {
    const inc = this.incidents.get(id);
    if (!inc || inc.status !== 'firing') return null;
    const updated: AlertIncident = { ...inc, status: 'acknowledged', acknowledgedAt: Date.now() };
    updated.auditHash = this.hashIncident(updated);
    this.incidents.set(id, updated);
    this.appendAuditLog(updated, 'acknowledged');
    this.emit('incidentUpdated', updated);
    return updated;
  }

  resolveIncident(id: string): AlertIncident | null {
    const inc = this.incidents.get(id);
    if (!inc || inc.status === 'resolved') return null;
    const updated: AlertIncident = { ...inc, status: 'resolved', resolvedAt: Date.now() };
    updated.auditHash = this.hashIncident(updated);
    this.incidents.set(id, updated);
    this.appendAuditLog(updated, 'resolved');
    this.emit('incidentUpdated', updated);
    return updated;
  }

  // ── Price ingestion and evaluation ───────────────────────────────────────────

  /**
   * Ingest an aggregated price and evaluate all matching rules.
   * Must complete in <1ms per call for 1000+ concurrent rules.
   */
  evaluate(price: AggregatedPrice, sourcePrices?: Array<{ source: OracleSourceName; price: number; timestamp: number }>): void {
    const now = Date.now();
    const asset = price.asset.toUpperCase();
    const priceNum = parseFloat(price.price);

    // Update price window
    const window = this.priceWindows.get(asset) ?? [];
    if (sourcePrices) {
      for (const sp of sourcePrices) {
        window.push(sp);
        const subKey = `${sp.source}:${asset}`;
        const counts = this.submissionCounts.get(subKey) ?? [];
        counts.push(sp.timestamp);
        this.submissionCounts.set(subKey, counts);
      }
    } else {
      window.push({ price: priceNum, timestamp: price.timestamp * 1000, source: price.sources[0] ?? 'chainlink' });
    }
    this.priceWindows.set(asset, window);

    // Evaluate each enabled rule
    for (const rule of this.rules.values()) {
      if (!rule.enabled) continue;
      if (rule.asset !== '*' && rule.asset.toUpperCase() !== asset) continue;
      this.evaluateRule(rule, price, priceNum, now, window);
    }
  }

  private evaluateRule(
    rule: AlertRule,
    price: AggregatedPrice,
    priceNum: number,
    now: number,
    window: Array<{ price: number; timestamp: number; source: OracleSourceName }>,
  ): void {
    const asset = price.asset.toUpperCase();
    const hKey = `${rule.id}:${asset}`;
    const hState = this.hysteresis.get(hKey) ?? { consecutiveHits: 0, consecutiveMisses: 0 };

    const effectiveThreshold = rule.adaptiveThreshold
      ? this.adaptiveThresholdFor(rule, asset, now)
      : rule.threshold;

    const windowStart = now - rule.windowSeconds * 1000;
    const windowedPrices = window.filter((p) => p.timestamp >= windowStart);

    const conditionMet = this.evaluateCondition(rule, price, priceNum, windowedPrices, effectiveThreshold, now);

    if (conditionMet) {
      hState.consecutiveHits++;
      hState.consecutiveMisses = 0;
    } else {
      hState.consecutiveMisses++;
      hState.consecutiveHits = 0;
    }
    this.hysteresis.set(hKey, hState);

    // Hysteresis: only fire after N consecutive hits
    if (conditionMet && hState.consecutiveHits >= rule.hysteresisCount) {
      this.maybeFire(rule, asset, price, priceNum, now);
    }

    // Auto-resolve: condition clear for 3 consecutive evals
    if (!conditionMet && hState.consecutiveMisses >= 3) {
      this.maybeAutoResolve(rule, asset, now);
    }

    // Update adaptive model
    if (rule.adaptiveThreshold) {
      this.updateAdaptiveState(rule, asset, priceNum, now);
    }
  }

  private evaluateCondition(
    rule: AlertRule,
    price: AggregatedPrice,
    priceNum: number,
    windowedPrices: Array<{ price: number; timestamp: number; source: OracleSourceName }>,
    threshold: number,
    now: number,
  ): boolean {
    switch (rule.category) {
      case 'cross_source_deviation': {
        if (windowedPrices.length < 2) return false;
        const prices = windowedPrices.map((p) => p.price);
        const min = Math.min(...prices);
        const max = Math.max(...prices);
        const deviation = ((max - min) / min) * 100;
        return deviation > threshold;
      }
      case 'flash_crash': {
        if (windowedPrices.length < 2) return false;
        const oldest = windowedPrices[0]?.price ?? priceNum;
        const drop = ((oldest - priceNum) / oldest) * 100;
        return drop > threshold;
      }
      case 'staleness': {
        const lastTs = price.timestamp * 1000;
        return now - lastTs > threshold * 1000;
      }
      case 'price_velocity': {
        if (windowedPrices.length < 2) return false;
        const oldest = windowedPrices[0];
        if (!oldest) return false;
        const elapsed = (now - oldest.timestamp) / 1000;
        if (elapsed === 0) return false;
        const velocity = Math.abs((priceNum - oldest.price) / oldest.price / elapsed) * 100;
        return velocity > threshold;
      }
      case 'volume_anomaly': {
        // Check submission frequency change (sudden drop or spike)
        const sourceKey = `${price.sources[0]}:${price.asset.toUpperCase()}`;
        const counts = this.submissionCounts.get(sourceKey) ?? [];
        const windowStart = now - rule.windowSeconds * 1000;
        const recent = counts.filter((t) => t >= windowStart).length;
        // Expected based on historical average
        const expected = (rule.windowSeconds / 60) * 10; // rough 10/min expected
        if (expected === 0) return false;
        const ratio = Math.abs(recent - expected) / expected * 100;
        return ratio > threshold;
      }
      case 'quality_degradation': {
        if (!price.anomaly) return false;
        return price.anomaly.score > threshold;
      }
      default:
        return false;
    }
  }

  private maybeFire(rule: AlertRule, asset: string, price: AggregatedPrice, priceNum: number, now: number): void {
    const hourBucket = Math.floor(now / (rule.windowSeconds * 1000));
    const dedupKey = `${rule.id}:${asset}:${hourBucket}`;
    const lastFired = this.suppressionWindows.get(dedupKey) ?? 0;
    if (now - lastFired < this.suppressionWindowMs) return;
    this.suppressionWindows.set(dedupKey, now);

    const incident: AlertIncident = {
      id: crypto.randomUUID(),
      ruleId: rule.id,
      asset,
      status: 'firing',
      firedAt: now,
      deduplicationKey: dedupKey,
      correlatedIncidentIds: this.findCorrelated(asset, rule, now),
      auditHash: '',
      details: { price: priceNum, threshold: rule.threshold, category: rule.category },
    };
    incident.auditHash = this.hashIncident(incident);
    this.incidents.set(incident.id, incident);
    this.appendAuditLog(incident, 'fired');
    logger.warn({ incidentId: incident.id, rule: rule.name, asset }, 'CEP alert fired');
    this.emit('alertFired', incident, rule);
    this.deliverAlert(incident, rule).catch((err) =>
      logger.error({ err, incidentId: incident.id }, 'Alert delivery failed'),
    );
  }

  private maybeAutoResolve(rule: AlertRule, asset: string, now: number): void {
    for (const [, inc] of this.incidents) {
      if (inc.ruleId === rule.id && inc.asset === asset && inc.status === 'firing') {
        this.resolveIncident(inc.id);
      }
    }
  }

  private findCorrelated(asset: string, rule: AlertRule, now: number): string[] {
    const correlated: string[] = [];
    for (const [, inc] of this.incidents) {
      if (inc.asset === asset && inc.status === 'firing' && inc.ruleId !== rule.id) {
        if (now - inc.firedAt < this.correlationWindowMs) {
          correlated.push(inc.id);
        }
      }
    }
    return correlated;
  }

  // ── Adaptive thresholds (ML learned normal pattern) ──────────────────────────

  private adaptiveThresholdFor(rule: AlertRule, asset: string, now: number): number {
    const stateKey = `${rule.id}:${asset}`;
    const state = this.adaptiveState.get(stateKey);
    if (!state) return rule.threshold;
    const hour = new Date(now).getHours();
    const mean = state.hourlyMean[hour] ?? 0;
    const std = state.hourlyStdDev[hour] ?? 0;
    // Adaptive threshold = base + learned_normal + 2*std
    return Math.max(rule.threshold, mean + 2 * std);
  }

  private updateAdaptiveState(rule: AlertRule, asset: string, priceNum: number, now: number): void {
    const stateKey = `${rule.id}:${asset}`;
    const state: AdaptiveThresholdState = this.adaptiveState.get(stateKey) ?? {
      hourlyMean: new Array(24).fill(0),
      hourlyStdDev: new Array(24).fill(0),
      sampleCount: new Array(24).fill(0),
    };
    const hour = new Date(now).getHours();
    const n = (state.sampleCount[hour] ?? 0) + 1;
    const prevMean = state.hourlyMean[hour] ?? 0;
    // Welford's online algorithm
    const delta = priceNum - prevMean;
    const newMean = prevMean + delta / n;
    const delta2 = priceNum - newMean;
    // We store variance in std slot temporarily until we sqrt at read time
    const prevVar = (state.hourlyStdDev[hour] ?? 0) ** 2;
    const newVar = (prevVar * (n - 1) + delta * delta2) / n;
    state.hourlyMean[hour] = newMean;
    state.hourlyStdDev[hour] = Math.sqrt(newVar);
    state.sampleCount[hour] = n;
    this.adaptiveState.set(stateKey, state);
  }

  // ── Delivery ─────────────────────────────────────────────────────────────────

  private async deliverAlert(incident: AlertIncident, rule: AlertRule): Promise<void> {
    for (const channel of rule.channels) {
      try {
        await this.deliverToChannel(channel, incident, rule);
      } catch (err) {
        logger.error({ err, channelType: channel.type, incidentId: incident.id }, 'Channel delivery error');
      }
    }
  }

  private async deliverToChannel(channel: AlertChannel, incident: AlertIncident, rule: AlertRule): Promise<void> {
    const payload = {
      incidentId: incident.id,
      rule: rule.name,
      asset: incident.asset,
      severity: rule.severity,
      firedAt: incident.firedAt,
      details: incident.details,
      auditHash: incident.auditHash,
    };

    switch (channel.type) {
      case 'webhook': {
        const url = channel.config['url'];
        if (!url) return;
        const maxRetries = 3;
        for (let attempt = 0; attempt < maxRetries; attempt++) {
          try {
            const resp = await fetch(url, {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify(payload),
            });
            if (resp.ok) return;
            logger.warn({ attempt, status: resp.status }, 'Webhook delivery failed, retrying');
          } catch {
            if (attempt === maxRetries - 1) throw new Error(`Webhook failed after ${maxRetries} attempts`);
          }
          await new Promise((r) => setTimeout(r, 1000 * 2 ** attempt));
        }
        break;
      }
      case 'slack': {
        const url = channel.config['webhookUrl'];
        if (!url) return;
        const slackPayload = {
          text: `*[${rule.severity.toUpperCase()}]* ${rule.name}`,
          attachments: [
            {
              color: rule.severity === 'critical' ? 'danger' : rule.severity === 'warning' ? 'warning' : 'good',
              fields: [
                { title: 'Asset', value: incident.asset, short: true },
                { title: 'Category', value: rule.category, short: true },
                { title: 'Incident ID', value: incident.id, short: false },
              ],
              footer: `Audit hash: ${incident.auditHash.slice(0, 16)}...`,
            },
          ],
        };
        await fetch(url, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(slackPayload) });
        break;
      }
      case 'pagerduty': {
        const key = channel.config['routingKey'];
        if (!key) return;
        await fetch('https://events.pagerduty.com/v2/enqueue', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            routing_key: key,
            event_action: 'trigger',
            dedup_key: incident.deduplicationKey,
            payload: {
              summary: `${rule.name}: ${incident.asset}`,
              severity: rule.severity === 'critical' ? 'critical' : rule.severity === 'warning' ? 'warning' : 'info',
              source: 'stellar-unified-price-oracle',
              custom_details: payload,
            },
          }),
        });
        break;
      }
      case 'email': {
        const emailWebhookUrl = channel.config['webhookUrl'];
        if (!emailWebhookUrl) return;
        await fetch(emailWebhookUrl, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ ...payload, to: channel.config['recipients'] }),
        });
        break;
      }
    }
  }

  // ── Audit ────────────────────────────────────────────────────────────────────

  private hashIncident(incident: AlertIncident): string {
    const data = JSON.stringify({
      id: incident.id,
      ruleId: incident.ruleId,
      asset: incident.asset,
      status: incident.status,
      firedAt: incident.firedAt,
      resolvedAt: incident.resolvedAt,
      deduplicationKey: incident.deduplicationKey,
    });
    return crypto.createHash('sha256').update(data).digest('hex');
  }

  private appendAuditLog(incident: AlertIncident, action: string): void {
    // Emit event for any attached audit log handler (file, DB, etc.)
    this.emit('auditLog', { action, incident, timestamp: Date.now() });
    logger.info({ action, incidentId: incident.id, auditHash: incident.auditHash }, 'CEP audit log');
  }
}

/** Singleton instance */
export const cepEngine = new CepEngine();
