import { ExecutorContext, runCommand } from '@nx/devkit';

export interface CargoTestOptions {
  cwd: string;
  verbose?: boolean;
  testName?: string;
  nocapture?: boolean;
}

export default async function cargoTestExecutor(
  options: CargoTestOptions,
  context: ExecutorContext
) {
  const args: string[] = ['test'];

  if (options.testName) {
    args.push(options.testName);
  }

  if (options.nocapture) {
    args.push('--nocapture');
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
