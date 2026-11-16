import { doctorCommand } from "./commands/doctor";
import { initCommand } from "./commands/init";
import { USAGE } from "./usage";

export function run(argv: string[]): Promise<number> {
  return Promise.resolve(dispatch(argv));
}

function dispatch(argv: string[]): number {
  const command = argv[0];
  switch (command) {
    case undefined:
    case "help":
    case "-h":
    case "--help":
      process.stdout.write(`${USAGE}\n`);
      return 0;
    case "init":
      return initCommand();
    case "doctor":
      return doctorCommand();
    default:
      process.stderr.write(`unknown command: ${command}\n\n${USAGE}\n`);
      return 1;
  }
}
