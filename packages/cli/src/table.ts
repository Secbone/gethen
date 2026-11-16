/**
 * Render a plain text table: one header row followed by aligned cells joined
 * with two spaces (matches the `gethen status` / `gethen worker ls` output).
 */
export function formatTable(
  headers: readonly string[],
  rows: readonly (readonly string[])[],
): string[] {
  const widths = headers.map((header, column) =>
    Math.max(header.length, ...rows.map((row) => (row[column] ?? "").length)),
  );
  const format = (cells: readonly string[]): string =>
    cells
      .map((cell, column) => cell.padEnd(widths[column] ?? 0))
      .join("  ")
      .trimEnd();
  return [format(headers), ...rows.map(format)];
}
