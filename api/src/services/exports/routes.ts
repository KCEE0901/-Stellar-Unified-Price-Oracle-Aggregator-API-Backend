import { Router, Request, Response } from 'express';
import { ExportEngine } from './engine';
import { ExportRequestSchema } from './types';
import { ZodError } from 'zod';

export function createExportRoutes(engine: ExportEngine): Router {
  const router = Router();

  router.post('/v1/exports', async (req: Request, res: Response) => {
    try {
      const validated = ExportRequestSchema.parse(req.body);

      const idempotencyKey = req.headers['idempotency-key'] as string;

      const job = await engine.createExportJob(validated as any, idempotencyKey);

      res.status(202).json({
        id: job.id,
        status: job.status,
        createdAt: job.createdAt,
        _links: {
          self: { href: `/v1/exports/${job.id}` },
          cancel: { href: `/v1/exports/${job.id}/cancel`, method: 'POST' },
          download: { href: `/v1/exports/${job.id}/download` },
        },
      });
    } catch (error) {
      if (error instanceof ZodError) {
        return res.status(400).json({
          error: 'Validation error',
          details: error.errors,
        });
      }

      res.status(500).json({
        error: 'Failed to create export job',
        message: error instanceof Error ? error.message : 'Unknown error',
      });
    }
  });

  router.get('/v1/exports/:id', async (req: Request, res: Response) => {
    try {
      const job = await engine.getExportJob(req.params.id);

      if (!job) {
        return res.status(404).json({ error: 'Export job not found' });
      }

      res.json({
        id: job.id,
        status: job.status,
        format: job.format,
        assetPair: job.assetPair,
        startTime: job.startTime,
        endTime: job.endTime,
        rowsProcessed: job.rowsProcessed,
        totalRows: job.totalRows,
        checksum: job.checksum,
        createdAt: job.createdAt,
        updatedAt: job.updatedAt,
        completedAt: job.completedAt,
        error: job.error,
        _links: {
          self: { href: `/v1/exports/${job.id}` },
          cancel: job.status === 'running' ? { href: `/v1/exports/${job.id}/cancel`, method: 'POST' } : undefined,
          download: job.status === 'completed' ? { href: `/v1/exports/${job.id}/download` } : undefined,
        },
      });
    } catch (error) {
      res.status(500).json({
        error: 'Failed to retrieve export job',
        message: error instanceof Error ? error.message : 'Unknown error',
      });
    }
  });

  router.post('/v1/exports/:id/cancel', async (req: Request, res: Response) => {
    try {
      const cancelled = await engine.cancelExportJob(req.params.id);

      if (!cancelled) {
        return res.status(404).json({ error: 'Export job not found or cannot be cancelled' });
      }

      res.status(204).send();
    } catch (error) {
      res.status(500).json({
        error: 'Failed to cancel export job',
        message: error instanceof Error ? error.message : 'Unknown error',
      });
    }
  });

  router.get('/v1/exports/:id/download', async (req: Request, res: Response) => {
    try {
      const stream = await engine.downloadExport(req.params.id);

      if (!stream) {
        return res.status(404).json({ error: 'Export not found or not ready for download' });
      }

      res.setHeader('Content-Type', 'application/octet-stream');
      res.setHeader('Content-Disposition', `attachment; filename="export-${req.params.id}.bin"`);

      stream.pipe(res);
    } catch (error) {
      res.status(500).json({
        error: 'Failed to download export',
        message: error instanceof Error ? error.message : 'Unknown error',
      });
    }
  });

  router.get('/v1/exports', async (req: Request, res: Response) => {
    try {
      const status = req.query.status as string | undefined;
      const assetPair = req.query.assetPair as string | undefined;

      const jobs = await engine.listExportJobs({
        status: status as any,
        assetPair,
      });

      res.json({
        items: jobs.map((job) => ({
          id: job.id,
          status: job.status,
          format: job.format,
          assetPair: job.assetPair,
          createdAt: job.createdAt,
          completedAt: job.completedAt,
        })),
        total: jobs.length,
        _links: {
          self: { href: '/v1/exports' },
          create: { href: '/v1/exports', method: 'POST' },
        },
      });
    } catch (error) {
      res.status(500).json({
        error: 'Failed to list export jobs',
        message: error instanceof Error ? error.message : 'Unknown error',
      });
    }
  });

  return router;
}
