import crypto from 'crypto';
import { Transform } from 'stream';

export class EncryptionManager {
  private readonly ALGORITHM = 'aes-256-gcm';
  private readonly IV_LENGTH = 16;
  private readonly AUTH_TAG_LENGTH = 16;
  private readonly SALT_LENGTH = 32;

  generateKey(): Buffer {
    return crypto.randomBytes(32); // 256 bits
  }

  generateIV(): Buffer {
    return crypto.randomBytes(this.IV_LENGTH);
  }

  deriveKey(password: string, salt: Buffer): Buffer {
    return crypto.pbkdf2Sync(password, salt, 100000, 32, 'sha256');
  }

  encrypt(data: Buffer, key: Buffer): Buffer {
    const iv = this.generateIV();
    const cipher = crypto.createCipheriv(this.ALGORITHM, key, iv);

    const encrypted = Buffer.concat([cipher.update(data), cipher.final()]);
    const authTag = cipher.getAuthTag();

    // Format: iv + authTag + encrypted
    return Buffer.concat([iv, authTag, encrypted]);
  }

  decrypt(encryptedData: Buffer, key: Buffer): Buffer {
    const iv = encryptedData.slice(0, this.IV_LENGTH);
    const authTag = encryptedData.slice(this.IV_LENGTH, this.IV_LENGTH + this.AUTH_TAG_LENGTH);
    const encrypted = encryptedData.slice(this.IV_LENGTH + this.AUTH_TAG_LENGTH);

    const decipher = crypto.createDecipheriv(this.ALGORITHM, key, iv);
    decipher.setAuthTag(authTag);

    return Buffer.concat([decipher.update(encrypted), decipher.final()]);
  }

  createEncryptionStream(key: Buffer): Transform {
    const iv = this.generateIV();
    const cipher = crypto.createCipheriv(this.ALGORITHM, key, iv);

    return new Transform({
      transform(chunk: Buffer, encoding, callback) {
        try {
          const encrypted = cipher.update(chunk);
          callback(null, encrypted);
        } catch (error) {
          callback(error instanceof Error ? error : new Error(String(error)));
        }
      },
      flush(callback) {
        try {
          const final = cipher.final();
          const authTag = cipher.getAuthTag();
          // Prepend IV and auth tag to the encrypted stream
          callback(null, Buffer.concat([iv, authTag, final]));
        } catch (error) {
          callback(error instanceof Error ? error : new Error(String(error)));
        }
      },
    });
  }

  createDecryptionStream(key: Buffer, encryptedData: Buffer): Transform {
    const iv = encryptedData.slice(0, this.IV_LENGTH);
    const authTag = encryptedData.slice(this.IV_LENGTH, this.IV_LENGTH + this.AUTH_TAG_LENGTH);

    const decipher = crypto.createDecipheriv(this.ALGORITHM, key, iv);
    decipher.setAuthTag(authTag);

    return new Transform({
      transform(chunk: Buffer, encoding, callback) {
        try {
          const decrypted = decipher.update(chunk);
          callback(null, decrypted);
        } catch (error) {
          callback(error instanceof Error ? error : new Error(String(error)));
        }
      },
      flush(callback) {
        try {
          const final = decipher.final();
          callback(null, final);
        } catch (error) {
          callback(error instanceof Error ? error : new Error(String(error)));
        }
      },
    });
  }
}
