import { EventEmitter } from 'events';
import { ExportRequest, ExportStatus, ExportFormat } from './types';
import { ExportEngine } from './engine';

export interface CDCConfig {
  enabled: boolean;
  pollIntervalMs: number;
  batchSize: number;
  cloudProvider: string;
  destinationPath: string;
  encryptionEnabled: boolean;
}

export interface CDCEvent {
  type: 'data_available' | 'export_started' | 'export_completed' | 'export_failed';
  jobId: string;
  rowsProcessed?: number;
  error?: string;
  timestamp: Date;
}

export class ChangeDataCapture extends EventEmitter {
  private exportEngine: ExportEngine;
  private config: CDCConfig;
  private isRunning = false;
  private pollInterval: NodeJS.Timeout | null = null;
  private lastProcessedTimestamp = new Date();
  private wsClients: Set<WebSocket> = new Set();

  constructor(engine: ExportEngine, config: CDCConfig) {
    super();
    this.exportEngine = engine;
    this.config = config;
  }

  async start(): Promise<void> {
    if (this.isRunning) return;

    this.isRunning = true;
    this.pollInterval = setInterval(() => this.pollForChanges(), this.config.pollIntervalMs);
  }

  async stop(): Promise<void> {
    this.isRunning = false;
    if (this.pollInterval) {
      clearInterval(this.pollInterval);
      this.pollInterval = null;
    }
  }

  private async pollForChanges(): Promise<void> {
    try {
      // Query for new/modified data since lastProcessedTimestamp
      const now = new Date();

      const exportRequest: ExportRequest = {
        format: ExportFormat.PARQUET,
        assetPair: 'BTC-USD',
        startTime: this.lastProcessedTimestamp,
        endTime: now,
        destination: {
          provider: this.config.cloudProvider as any,
          path: this.config.destinationPath,
        },
        encryptionConfig: {
          enabled: this.config.encryptionEnabled,
          algorithm: 'AES-256-GCM',
        },
      };

      const job = await this.exportEngine.createExportJob(exportRequest);

      this.emit('data_available', {
        type: 'data_available',
        jobId: job.id,
        timestamp: new Date(),
      } as CDCEvent);

      // Wait for job completion
      await this.waitForJobCompletion(job.id);

      this.lastProcessedTimestamp = now;
    } catch (error) {
      console.error('CDC polling error:', error);
      this.emit('poll_error', error);
    }
  }

  private async waitForJobCompletion(jobId: string, maxWaitMs = 300000): Promise<void> {
    const startTime = Date.now();

    while (Date.now() - startTime < maxWaitMs) {
      const job = await this.exportEngine.getExportJob(jobId);

      if (!job) {
        throw new Error(`Job ${jobId} not found`);
      }

      if (job.status === ExportStatus.COMPLETED) {
        this.emit('export_completed', {
          type: 'export_completed',
          jobId: job.id,
          rowsProcessed: job.rowsProcessed,
          timestamp: new Date(),
        } as CDCEvent);
        return;
      }

      if (job.status === ExportStatus.FAILED || job.status === ExportStatus.CANCELLED) {
        throw new Error(`Job failed: ${job.error}`);
      }

      // Wait 1 second before polling again
      await new Promise((resolve) => setTimeout(resolve, 1000));
    }

    throw new Error(`Job ${jobId} did not complete within ${maxWaitMs}ms`);
  }

  addWebSocketClient(ws: WebSocket): void {
    this.wsClients.add(ws);
  }

  removeWebSocketClient(ws: WebSocket): void {
    this.wsClients.delete(ws);
  }

  broadcastEvent(event: CDCEvent): void {
    const message = JSON.stringify(event);
    for (const client of this.wsClients) {
      if (client.readyState === 1) {
        // OPEN
        client.send(message);
      }
    }
  }
}
