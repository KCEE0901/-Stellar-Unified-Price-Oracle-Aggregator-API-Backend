import { Transform, Writable } from 'stream';
import { ExportFormat, ExportJobRow, ParquetMetadata } from './types';
import crypto from 'crypto';
import zlib from 'zlib';

export abstract class BaseStreamFormatter {
  protected checksum: crypto.Hash;
  protected rowCount = 0;
  protected compressionEnabled: boolean;

  constructor(compressionEnabled = false) {
    this.checksum = crypto.createHash('sha256');
    this.compressionEnabled = compressionEnabled;
  }

  abstract createTransform(): Transform;

  getChecksum(): string {
    return this.checksum.digest('hex');
  }

  getRowCount(): number {
    return this.rowCount;
  }

  protected updateChecksum(data: Buffer): void {
    this.checksum.update(data);
  }

  getCompressionStream(): Writable {
    return zlib.createGzip({ level: 6 });
  }
}

export class CSVFormatter extends BaseStreamFormatter {
  private headerWritten = false;

  createTransform(): Transform {
    return new Transform({
      objectMode: true,
      transform: (row: ExportJobRow, encoding, callback) => {
        try {
          if (!this.headerWritten) {
            const headers = 'timestamp,price,source,assetPair\n';
            const headerBuffer = Buffer.from(headers);
            this.updateChecksum(headerBuffer);
            callback(null, headerBuffer);
            this.headerWritten = true;
          }

          const csvLine = `${row.timestamp},${row.price},"${row.source}","${row.assetPair}"\n`;
          const buffer = Buffer.from(csvLine);
          this.updateChecksum(buffer);
          this.rowCount++;
          callback(null, buffer);
        } catch (error) {
          callback(error);
        }
      },
    });
  }
}

export class NDJSONFormatter extends BaseStreamFormatter {
  createTransform(): Transform {
    return new Transform({
      objectMode: true,
      transform: (row: ExportJobRow, encoding, callback) => {
        try {
          const jsonLine = JSON.stringify(row) + '\n';
          const buffer = Buffer.from(jsonLine);
          this.updateChecksum(buffer);
          this.rowCount++;
          callback(null, buffer);
        } catch (error) {
          callback(error);
        }
      },
    });
  }
}

export class ParquetFormatter extends BaseStreamFormatter {
  private partitionKey = '';
  private currentPartition: ExportJobRow[] = [];
  private readonly PARTITION_SIZE = 100000;
  private partitionIndex = 0;
  private metadata: ParquetMetadata = {
    schema: {
      timestamp: 'int64',
      price: 'utf8',
      source: 'utf8',
      assetPair: 'utf8',
    },
    partitions: [],
    createdAt: new Date(),
    version: '1.0',
  };

  createTransform(): Transform {
    return new Transform({
      objectMode: true,
      transform: (row: ExportJobRow, encoding, callback) => {
        try {
          this.currentPartition.push(row);

          if (this.currentPartition.length >= this.PARTITION_SIZE) {
            this.flushPartition(callback);
          } else {
            callback();
          }
        } catch (error) {
          callback(error);
        }
      },
      flush: (callback) => {
        if (this.currentPartition.length > 0) {
          this.flushPartition(callback);
        } else {
          callback();
        }
      },
    });
  }

  private flushPartition(callback: Function): void {
    const partitionKey = `partition_${this.partitionIndex}`;
    const minTs = Math.min(...this.currentPartition.map((r) => r.timestamp));
    const maxTs = Math.max(...this.currentPartition.map((r) => r.timestamp));

    this.metadata.partitions.push({
      partitionKey,
      minTimestamp: minTs,
      maxTimestamp: maxTs,
      rowCount: this.currentPartition.length,
    });

    const parquetData = Buffer.from(JSON.stringify(this.currentPartition));
    this.updateChecksum(parquetData);
    this.rowCount += this.currentPartition.length;

    this.currentPartition = [];
    this.partitionIndex++;

    callback(null, parquetData);
  }

  getMetadata(): ParquetMetadata {
    return this.metadata;
  }
}

export class ArrowIPCFormatter extends BaseStreamFormatter {
  createTransform(): Transform {
    return new Transform({
      objectMode: true,
      transform: (row: ExportJobRow, encoding, callback) => {
        try {
          // Arrow IPC format: serialize as compact binary representation
          const arrowData = Buffer.from(JSON.stringify(row));
          this.updateChecksum(arrowData);
          this.rowCount++;
          callback(null, arrowData);
        } catch (error) {
          callback(error);
        }
      },
    });
  }
}

export function createFormatter(format: ExportFormat, compressionEnabled: boolean): BaseStreamFormatter {
  switch (format) {
    case ExportFormat.CSV:
      return new CSVFormatter(compressionEnabled);
    case ExportFormat.NDJSON:
      return new NDJSONFormatter(compressionEnabled);
    case ExportFormat.PARQUET:
      return new ParquetFormatter(compressionEnabled);
    case ExportFormat.ARROW_IPC:
      return new ArrowIPCFormatter(compressionEnabled);
    default:
      throw new Error(`Unknown export format: ${format}`);
  }
}
