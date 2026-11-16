export const USAGE = `gethen — a self-iterating agent framework

Usage:
  gethen harness list
  gethen bench list|validate|run|external|import|report|compare|arms|history [options]
  gethen run "<prompt>" [--harness <id>] [--model <id>] [--cwd <path>] [--memory <file>] [--skills <file>] [--remember "<fact>"]... [--reflect] [--reflect-auto] [--experience <dir>] [--recall-chars <n>] [--permission <ask|auto|full>]
  gethen chat [--harness <id>] [--model <id>] [--cwd <path>] [--memory <file>] [--skills <file>] [--experience <dir>] [--recall-chars <n>] [--permission <ask|auto|full>]
  gethen loop demo|memory
  gethen memory migrate [--memory <file>] [--snapshot <file>] [--audit <file>]
  gethen evolve memory --queries <selection-file> (--facts <file> | --agent --training-queries <file> [--from-experience] [--experience <dir>] [--recent <n>]) [--test-queries <final-test-file>] [--model <id>] [--store <dir>] [--memory <file>] [--iterations <n>]
  gethen iterate --target <dir> --task "<goal>" --check "<cmd>" [--eval "<cmd>"] [--prepare "<isolated-environment-setup>"]
  gethen trace [<run-dir>] [--raw|--all|--tree|--follow|--summary|--check|--errors|--context|--memory|--recall] [--tool <id>] [--turn <id>]
  gethen trace compare <before-dir> <after-dir>
  gethen trace feedback <run-dir> --turn <id> [--score <0..1>] [--correction "<text>"]
  gethen help

Options:
  --harness <id>       harness adapter: fake, pi, opencode, or configured acp (default: fake)
  --recall-chars <n>   opt-in scoped native interaction recall; 0 disables injection
  --skills <file>      enable scoped reusable procedures in a separate repository
  --model <id>         model id passed to the harness
  --cwd <path>         working directory (default: process.cwd())
  --permission <mode>  ask | auto | full (default: auto)
  --log-dir <dir>     trajectory root directory (default: .gethen/runs)
  --timeout-ms <ms>   startup/turn timeout for run and chat (default: 120000)
  --log-protocol      capture redacted engine RPC diagnostics (run/chat)
  --execution-policy <file>  run-only exact input/output file policy; requires native engine support
  --output-contract <file>   run-only JSON Schema and optional trusted host validator
  --yes                shorthand for --permission full

iterate options:
  --target <dir>       git repo the agent edits (default: process.cwd())
  --task "<goal>"      what the agent should achieve (required)
  --check "<cmd>"      gate + default evaluator: pass=1, fail=0 (required unless --eval)
  --eval "<cmd>"       evaluator command printing a Signal[] JSON array on stdout
  --iterations <n>     how many iterations to attempt (default: 1)
  --protect <glob>     被保护的路径，禁止 agent 修改（可重复；默认 packages/evolution/src/**）
  --from-experience    feed a digest of recent experiences into the proposer

  --experience <dir>   experience store directory (default: .gethen/experience)
  --recent <n>         how many recent experiences to feed (default: 20)

ACP harness:
  Set GETHEN_ACP_BIN to an ACP agent binary (e.g. an opencode build) to expose the
  "acp" harness. Optional GETHEN_ACP_ARGS (space-separated) override the default "acp".
`;
