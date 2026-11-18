import { doctorCommand } from "./commands/doctor";
import { initCommand } from "./commands/init";
import { mailboxCommand } from "./commands/mailbox";
import { runCommand } from "./commands/run";
import { serveCommand } from "./commands/serve";
import { sessionCommand } from "./commands/session";
import { statusCommand } from "./commands/status";
import { taskCommand } from "./commands/task";
import { ticketCommand } from "./commands/ticket";
import { workerCommand } from "./commands/worker";
import { USAGE } from "./usage";

function flagValue(argv: string[], name: string): string | undefined {
  const index = argv.indexOf(name);
  return index < 0 ? undefined : argv[index + 1];
}

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
    case "mailbox":
      return mailboxCommand(argv.slice(1));
    case "doctor": {
      const workerConfig = flagValue(argv.slice(1), "--worker-config");
      return doctorCommand({
        json: argv.slice(1).includes("--json"),
        ...(workerConfig === undefined ? {} : { workerConfig }),
      });
    }
    case "run":
      return runCommand(argv.slice(1));
    case "serve":
      return serveCommand(argv.slice(1));
    case "session":
      return sessionCommand(argv.slice(1));
    case "status":
      return statusCommand(argv.slice(1));
    case "task":
      return taskCommand(argv.slice(1));
    case "ticket":
      return ticketCommand(argv.slice(1));
    case "worker":
      return workerCommand(argv.slice(1));
    default:
      process.stderr.write(`unknown command: ${command}\n\n${USAGE}\n`);
      return 1;
  }
}
