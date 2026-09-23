/**
 * 页码窗口：≤7 页全部展示；否则保留首页、末页与当前页 ±1，其余折叠为省略号（null）。
 * current 超出范围时先收敛到 [1, totalPages]，保证窗口始终有效。
 */
export function pageWindow(current: number, totalPages: number): Array<number | null> {
  if (totalPages <= 0)
    return [];
  const clamped = Math.min(Math.max(current, 1), totalPages);
  if (totalPages <= 7)
    return Array.from({ length: totalPages }, (_, index) => index + 1);
  const kept = new Set(
    [1, totalPages, clamped - 1, clamped, clamped + 1].filter(page => page >= 1 && page <= totalPages),
  );
  const window: Array<number | null> = [];
  let previous = 0;
  for (const page of [...kept].sort((a, b) => a - b)) {
    if (page - previous > 1)
      window.push(null);
    window.push(page);
    previous = page;
  }
  return window;
}
