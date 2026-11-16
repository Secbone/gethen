import { doctorCommand } from "./commands/doctor";
import { initCommand } from "./commands/init";
import { serveCommand } from "./commands/serve";
import { statusCommand } from "./commands/status";
import { workerCommand } from "./commands/worker";
import { USAGE } from "./usage";

export function run(argv: string[]): Promise<number> {
  return dispatch(argv);
}

async function dispatch(argv: string[]): Promise<number> {
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
      return doctorCommand({ json: argv.slice(1).includes("--json") });
    case "serve":
      return serveCommand(argv.slice(1));
    case "status":
      return statusCommand(argv.slice(1));
    case "worker":
      return workerCommand(argv.slice(1));
    default:
      process.stderr.write(`unknown command: ${command}\n\n${USAGE}\n`);
      return 1;
  }
}
