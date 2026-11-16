export const name = "gethen-spike";
export const inject = ["tools"];

export function apply(ctx) {
  ctx.provide("gethenSpike", { ping: () => "pong" });
  ctx.tools.register({
    name: "gethen_ping",
    description: "Return pong from the Gethen DSH spike plugin.",
    parameters: {},
    output: {
      schema: {
        type: "object",
        properties: { pong: { type: "boolean" } },
        required: ["pong"],
        additionalProperties: false,
      },
      render: (_args, value) => [{ type: "text", text: JSON.stringify(value) }],
    },
    async execute() {
      return { pong: true };
    },
  });
  console.error("[gethen-spike] loaded");
  setTimeout(() => {
    try {
      const names = ctx.tools.schemas().map((tool) => tool.name);
      console.error(
        "[gethen-spike] tools",
        names.filter((name) => name === "gethen_ping"),
      );
      console.error("[gethen-spike] service", ctx.get("gethenSpike")?.ping?.());
    } catch (error) {
      console.error("[gethen-spike] probe failed", error);
    }
  }, 50);
}
