/**
 * @stellar-oracle/shared-types
 *
 * Shared domain types used across the API and Aggregator services.
 * Infrastructure-specific types (Express, Axios, WS) must NOT be added here.
 */

export type OracleSourceName = 'chainlink' | 'redstone' | 'band' | 'reflector';

export type DegradationLevel = 'healthy' | 'degraded' | 'critical';

export type FeedGrade = 'A+' | 'A' | 'B' | 'C' | 'D' | 'F';

export interface NormalizedPrice {
  asset: string;
  price: bigint;
  decimals: number;
  source: OracleSourceName;
  timestamp: number;
}

export interface AnomalyScore {
  isAnomaly: boolean;
  score: number;
  method: 'zscore' | 'moving_average' | 'volatility' | 'isolation_forest' | 'dbscan';
  details: string;
}

/** Per-source quality metrics (Issue #124) */
export interface SourceQualityMetrics {
  source: OracleSourceName;
  asset: string;
  /** % of polls that responded within timeout (0–100) */
  uptime: number;
  /** Mean absolute percentage error vs aggregated median */
  accuracy: number;
  /** Median latency between source timestamp and aggregator receipt, ms */
  timeliness: number;
  /** Variance of submissions in rolling window */
  consistency: number;
  /** How recently source submitted vs expected cadence, 0=fresh */
  freshnessLag: number;
  /** Signed deviation from median – positive = consistently over */
  bias: number;
  /** Number of assets this source provides */
  availability: number;
  /** Rate of malformed/missing/out-of-range values (0–1) */
  responseIntegrity: number;
  /** Composite score 0–100, higher is better */
  compositeScore: number;
  /** Sampling weight used in quality-weighted median (0–1) */
  weight: number;
  computedAt: number;
}

export interface AggregatedPrice {
  asset: string;
  price: string;
  decimals: number;
  sources: OracleSourceName[];
  timestamp: number;
  confidence: number;
  degradationLevel: DegradationLevel;
  stale: boolean;
  anomaly?: AnomalyScore;
  /** Quality metadata (Issue #124) */
  quality?: {
    feedGrade: FeedGrade;
    perSourceScores: SourceQualityMetrics[];
    confidenceInterval: [number, number];
  };
}

/** Export job (Issue #123) */
export type ExportFormat = 'csv' | 'ndjson' | 'parquet' | 'arrow';
export type ExportDestinationType = 'download' | 's3' | 'gcs' | 'azure' | 'websocket';
export type ExportStatus = 'pending' | 'running' | 'completed' | 'failed' | 'cancelled';

export interface ExportJobRequest {
  assets: string[];
  startTime: number;
  endTime: number;
  format: ExportFormat;
  destination: ExportDestinationType;
  destinationConfig?: Record<string, string>;
  /** cron expression for recurring exports */
  schedule?: string;
  encryptionKeyId?: string;
}

export interface ExportJob {
  id: string;
  status: ExportStatus;
  request: ExportJobRequest;
  progress: number;
  bytesWritten: number;
  checkpoint?: string;
  downloadUrl?: string;
  checksum?: string;
  createdAt: number;
  updatedAt: number;
  error?: string;
}

/** CEP alert rules (Issue #125) */
export type AlertRuleCategory =
  | 'cross_source_deviation'
  | 'flash_crash'
  | 'staleness'
  | 'price_velocity'
  | 'volume_anomaly'
  | 'quality_degradation';

export type AlertSeverity = 'info' | 'warning' | 'critical';
export type IncidentStatus = 'firing' | 'acknowledged' | 'investigating' | 'resolved';

export interface AlertRule {
  id: string;
  name: string;
  asset: string;
  category: AlertRuleCategory;
  severity: AlertSeverity;
  threshold: number;
  windowSeconds: number;
  hysteresisCount: number;
  channels: AlertChannel[];
  adaptiveThreshold: boolean;
  enabled: boolean;
  createdAt: number;
  updatedAt: number;
}

export interface AlertChannel {
  type: 'webhook' | 'email' | 'slack' | 'pagerduty';
  config: Record<string, string>;
}

export interface AlertIncident {
  id: string;
  ruleId: string;
  asset: string;
  status: IncidentStatus;
  firedAt: number;
  resolvedAt?: number;
  acknowledgedAt?: number;
  deduplicationKey: string;
  correlatedIncidentIds: string[];
  auditHash: string;
}
