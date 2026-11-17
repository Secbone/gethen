export const USAGE = `gethen — operations CLI

Usage:
  gethen init      Create ~/.gethen and print next steps
  gethen serve     Start the master control plane (mailbox transport)
  gethen worker    Run the worker daemon, or manage tokens: worker add|ls|revoke
  gethen run       Dispatch one run to a master and print the result
  gethen status    Read the master database (counts + recent tickets)
  gethen ticket    Acknowledge, close or archive a ticket (ack|close|archive)
  gethen task      Inspect or steer a running assignment (status|logs|steer|cancel)
  gethen session   List harness sessions and read session history
  gethen doctor    Check Node, dsh, credentials and the local environment
  gethen help      Show this help

GETHEN_* environment variables are optional. Run \`gethen doctor\` to check
this machine before running a worker, and \`gethen status --json\` to inspect a
running master's persisted state. \`gethen serve --escalation auto\` auto-answers
worker escalations for remote testing; the default is to park them for a person.
The CLI speaks one communication stack: L1 ws transport + L2 mailbox.
\`gethen serve\` dispatches over the mailbox when worker tokens are configured
and falls back to the fake launch without tokens (dev convenience).
`;
