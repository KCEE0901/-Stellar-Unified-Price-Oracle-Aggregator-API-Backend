import { ExecutorContext, runCommand } from '@nx/devkit';

export interface CargoBuildOptions {
  cwd: string;
  release?: boolean;
  features?: string[];
  verbose?: boolean;
}

export default async function cargoBuildExecutor(
  options: CargoBuildOptions,
  context: ExecutorContext
) {
  const args: string[] = ['build'];

  if (options.release) {
    args.push('--release');
  }

  if (options.features && options.features.length > 0) {
    args.push(`--features=${options.features.join(',')}`);
  }

  if (options.verbose) {
    args.push('--verbose');
  }

  const command = `cargo ${args.join(' ')}`;

  console.log(`Running: ${command}`);
  console.log(`Working directory: ${options.cwd}`);

  const exitCode = await runCommand(command, {
    cwd: options.cwd,
  });

  return { success: exitCode === 0 };
}
