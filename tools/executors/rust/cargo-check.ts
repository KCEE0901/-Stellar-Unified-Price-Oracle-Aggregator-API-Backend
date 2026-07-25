import { ExecutorContext, runCommand } from '@nx/devkit';

export interface CargoCheckOptions {
  cwd: string;
  allTargets?: boolean;
  allFeatures?: boolean;
  verbose?: boolean;
}

export default async function cargoCheckExecutor(
  options: CargoCheckOptions,
  context: ExecutorContext
) {
  const args: string[] = ['check'];

  if (options.allTargets) {
    args.push('--all-targets');
  }

  if (options.allFeatures) {
    args.push('--all-features');
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
