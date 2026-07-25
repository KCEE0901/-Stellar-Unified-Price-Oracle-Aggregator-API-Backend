import { ExecutorContext, runCommand } from '@nx/devkit';

export interface CargoClippyOptions {
  cwd: string;
  allTargets?: boolean;
  allFeatures?: boolean;
  verbose?: boolean;
  denyWarnings?: boolean;
}

export default async function cargoClippyExecutor(
  options: CargoClippyOptions,
  context: ExecutorContext
) {
  const args: string[] = ['clippy'];

  if (options.allTargets) {
    args.push('--all-targets');
  }

  if (options.allFeatures) {
    args.push('--all-features');
  }

  if (options.denyWarnings) {
    args.push('--', '-D', 'warnings');
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
