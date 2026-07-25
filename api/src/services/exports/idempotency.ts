import crypto from 'crypto';

export interface IdempotencyRecord {
  key: string;
  jobId: string;
  createdAt: Date;
  expiresAt: Date;
}

export class IdempotencyKeyManager {
  private records: Map<string, IdempotencyRecord> = new Map();
  private readonly DEFAULT_TTL_MS = 86400000; // 24 hours

  generateKey(data: ExportRequestData): string {
    const hash = crypto
      .createHash('sha256')
      .update(JSON.stringify(data))
      .digest('hex');
    return hash;
  }

  register(key: string, jobId: string): void {
    const record: IdempotencyRecord = {
      key,
      jobId,
      createdAt: new Date(),
      expiresAt: new Date(Date.now() + this.DEFAULT_TTL_MS),
    };
    this.records.set(key, record);
  }

  lookup(key: string): string | null {
    const record = this.records.get(key);
    if (!record) return null;

    if (new Date() > record.expiresAt) {
      this.records.delete(key);
      return null;
    }

    return record.jobId;
  }

  isDuplicate(key: string): boolean {
    return this.lookup(key) !== null;
  }

  cleanup(): void {
    const now = new Date();
    for (const [key, record] of this.records.entries()) {
      if (now > record.expiresAt) {
        this.records.delete(key);
      }
    }
  }
}

interface ExportRequestData {
  format: string;
  assetPair: string;
  startTime: number;
  endTime: number;
  destination?: Record<string, unknown>;
}
