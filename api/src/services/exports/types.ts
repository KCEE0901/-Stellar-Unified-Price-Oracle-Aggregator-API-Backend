import { z } from 'zod';

export enum ExportFormat {
  CSV = 'csv',
  NDJSON = 'ndjson',
  PARQUET = 'parquet',
  ARROW_IPC = 'arrow_ipc',
}

export enum CloudProvider {
  AWS_S3 = 'aws_s3',
  GCS = 'gcs',
  AZURE_BLOB = 'azure_blob',
}

export enum ExportStatus {
  PENDING = 'pending',
  RUNNING = 'running',
  COMPLETED = 'completed',
  FAILED = 'failed',
  CANCELLED = 'cancelled',
}

export interface ExportJob {
  id: string;
  status: ExportStatus;
  format: ExportFormat;
  assetPair: string;
  startTime: Date;
  endTime: Date;
  destinationProvider?: CloudProvider;
  destinationPath?: string;
  rowsProcessed: number;
  totalRows?: number;
  checksum?: string;
  encryptionEnabled: boolean;
  createdAt: Date;
  updatedAt: Date;
  completedAt?: Date;
  error?: string;
  idempotencyKey: string;
}

export interface ExportRequest {
  format: ExportFormat;
  assetPair: string;
  startTime: Date;
  endTime: Date;
  destination?: {
    provider: CloudProvider;
    path: string;
    credentials?: Record<string, string>;
  };
  encryptionConfig?: {
    enabled: boolean;
    algorithm: 'AES-256-GCM';
    kmsProvider?: 'AWS' | 'GCP' | 'AZURE';
  };
  chunkSize?: number;
  compressionEnabled?: boolean;
}

export interface ExportJobRow {
  timestamp: number;
  price: string;
  source: string;
  assetPair: string;
}

export interface PartitionMetadata {
  partitionKey: string;
  minTimestamp: number;
  maxTimestamp: number;
  rowCount: number;
}

export interface ParquetMetadata {
  schema: Record<string, string>;
  partitions: PartitionMetadata[];
  createdAt: Date;
  version: string;
}

export const ExportRequestSchema = z.object({
  format: z.enum(['csv', 'ndjson', 'parquet', 'arrow_ipc']),
  assetPair: z.string().regex(/^[A-Z]{3,4}-[A-Z]{3,4}$/),
  startTime: z.date().or(z.string().datetime()),
  endTime: z.date().or(z.string().datetime()),
  destination: z
    .object({
      provider: z.enum(['aws_s3', 'gcs', 'azure_blob']),
      path: z.string(),
      credentials: z.record(z.string()).optional(),
    })
    .optional(),
  encryptionConfig: z
    .object({
      enabled: z.boolean().default(false),
      algorithm: z.enum(['AES-256-GCM']),
      kmsProvider: z.enum(['AWS', 'GCP', 'AZURE']).optional(),
    })
    .optional(),
  chunkSize: z.number().min(1024).max(52428800).default(1048576), // 1MB default, max 50MB
  compressionEnabled: z.boolean().default(false),
});
