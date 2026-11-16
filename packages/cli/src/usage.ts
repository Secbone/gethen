export const USAGE = `gethen — operations CLI

Usage:
  gethen init      Create ~/.gethen and print next steps
  gethen serve     Start the master control plane (HTTP + worker WS endpoint)
  gethen worker    Run the worker daemon (connects out to a master)
  gethen doctor    Check Node, dsh and the local environment
  gethen help      Show this help

GETHEN_* environment variables are optional. Run \`gethen doctor\` to check
this machine before running a worker.
`;
