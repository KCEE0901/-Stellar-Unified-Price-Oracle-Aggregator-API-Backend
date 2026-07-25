import { Readable, Writable, pipeline } from 'stream';
import { promisify } from 'util';
import {
  ExportJob,
  ExportRequest,
  ExportStatus,
  ExportFormat,
  CloudProvider,
  ExportJobRow,
} from './types';
import { createFormatter } from './formatters';
import { EncryptionManager } from './encryption';
import { createCloudProvider, CloudStorageProvider } from './cloud-storage';
import { IdempotencyKeyManager } from './idempotency';

const pipelineAsync = promisify(pipeline) as (
  ...args: Array<Readable | Writable | object>
) => Promise<void>;

export class ExportEngine {
  private jobs: Map<string, ExportJob> = new Map();
  private idempotencyManager = new IdempotencyKeyManager();
  private encryptionManager = new EncryptionManager();
  private nextJobId = 1;

  async createExportJob(request: ExportRequest, idempotencyKey?: string): Promise<ExportJob> {
    // Check for duplicate request
    if (idempotencyKey) {
      const existingJobId = this.idempotencyManager.lookup(idempotencyKey);
      if (existingJobId) {
        const existingJob = this.jobs.get(existingJobId);
        if (existingJob) return existingJob;
      }
    }

    const jobId = `export-${this.nextJobId++}-${Date.now()}`;
    const job: ExportJob = {
      id: jobId,
      status: ExportStatus.PENDING,
      format: request.format,
      assetPair: request.assetPair,
      startTime: request.startTime instanceof Date ? request.startTime : new Date(request.startTime),
      endTime: request.endTime instanceof Date ? request.endTime : new Date(request.endTime),
      destinationProvider: request.destination?.provider,
      destinationPath: request.destination?.path,
      rowsProcessed: 0,
      encryptionEnabled: request.encryptionConfig?.enabled ?? false,
      createdAt: new Date(),
      updatedAt: new Date(),
      idempotencyKey: idempotencyKey || '',
    };

    this.jobs.set(jobId, job);

    if (idempotencyKey) {
      this.idempotencyManager.register(idempotencyKey, jobId);
    }

    this.executeExport(job, request).catch((error) => {
      job.status = ExportStatus.FAILED;
      job.error = error.message;
      job.updatedAt = new Date();
    });

    return job;
  }

  private async executeExport(job: ExportJob, request: ExportRequest): Promise<void> {
    try {
      job.status = ExportStatus.RUNNING;
      job.updatedAt = new Date();

      const formatter = createFormatter(request.format, request.compressionEnabled ?? false);

      const sourceStream = this.createDataStream(request);

      let pipeline_streams: Array<Readable | Writable | object> = [sourceStream, formatter.createTransform()];

      let encryptionKey: Buffer | null = null;
      if (request.encryptionConfig?.enabled) {
        encryptionKey = this.encryptionManager.generateKey();
        pipeline_streams.push(this.encryptionManager.createEncryptionStream(encryptionKey));
      }

      let cloudProvider: CloudStorageProvider | null = null;
      if (request.destination?.provider) {
        cloudProvider = createCloudProvider(
          request.destination.provider,
          request.destination.credentials || {}
        );
      }

      if (cloudProvider) {
        const uploadStream = new Writable({
          write: async (chunk, encoding, callback) => {
            try {
              await cloudProvider?.upload(request.destination?.path || '', chunk);
              callback();
            } catch (error) {
              callback(error as Error);
            }
          },
        });
        pipeline_streams.push(uploadStream);
      }

      await pipelineAsync(...pipeline_streams);

      job.status = ExportStatus.COMPLETED;
      job.rowsProcessed = formatter.getRowCount();
      job.checksum = formatter.getChecksum();
      job.completedAt = new Date();

      if (request.format === ExportFormat.PARQUET) {
        const parquetFormatter = formatter as any;
        // Store metadata for Parquet
      }
    } catch (error) {
      job.status = ExportStatus.FAILED;
      job.error = error instanceof Error ? error.message : 'Unknown error';
    } finally {
      job.updatedAt = new Date();
    }
  }

  private createDataStream(request: ExportRequest): Readable {
    return new Readable({
      read() {
        // Mock data stream - in production, this would query the database
        // with resumable checkpoints for large datasets
        const mockData: ExportJobRow = {
          timestamp: Date.now(),
          price: '45000.50',
          source: 'chainlink',
          assetPair: request.assetPair,
        };
        this.push(JSON.stringify(mockData) + '\n');
        this.push(null);
      },
    });
  }

  async getExportJob(jobId: string): Promise<ExportJob | null> {
    return this.jobs.get(jobId) || null;
  }

  async cancelExportJob(jobId: string): Promise<boolean> {
    const job = this.jobs.get(jobId);
    if (!job) return false;

    if (job.status === ExportStatus.RUNNING) {
      job.status = ExportStatus.CANCELLED;
      job.updatedAt = new Date();
      return true;
    }

    return false;
  }

  async listExportJobs(filters?: { status?: ExportStatus; assetPair?: string }): Promise<ExportJob[]> {
    const jobs = Array.from(this.jobs.values());

    return jobs.filter((job) => {
      if (filters?.status && job.status !== filters.status) return false;
      if (filters?.assetPair && job.assetPair !== filters.assetPair) return false;
      return true;
    });
  }

  async downloadExport(jobId: string): Promise<Readable | null> {
    const job = this.jobs.get(jobId);
    if (!job || job.status !== ExportStatus.COMPLETED) {
      return null;
    }

    // In production, this would fetch from cloud storage or local cache
    return null;
  }
}
