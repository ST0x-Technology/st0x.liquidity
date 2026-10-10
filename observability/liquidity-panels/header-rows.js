// The header's input rows by their `k` column. The generator prepends this
// file to header.js; dashboard/src/lib/board-header.test.ts imports it as a
// module through the export line, which the generator drops.
//
// The stackdriver datasource runs a range query even for an instant target,
// so each `k` can arrive at several times. A plain value keeps its newest
// row. The commit and info series carry their sample timestamp as the
// value, because a restart or deploy leaves the previous label set in the
// lookback window at the same row times: the largest value is the label set
// the bot runs now. A row whose value is not a finite number carries no
// timestamp, so it never takes the newest slot.
const readHeaderRows = (rows) => {
  const value = {};
  const seen = {};
  const newest = {};
  for (const row of rows) {
    if (row.k === 'commit' || row.k === 'info') {
      const at = row.Value === null || row.Value === undefined || row.Value === '' ? NaN : Number(row.Value);
      if (!Number.isFinite(at)) continue;
      if (!newest[row.k] || at > newest[row.k].at) newest[row.k] = { row, at };
      continue;
    }
    const time = Number(row.Time) || 0;
    if (row.k in seen && seen[row.k] > time) continue;
    seen[row.k] = time;
    value[row.k] =
      row.Value === null || row.Value === undefined || Number.isNaN(Number(row.Value)) ? null : Number(row.Value);
  }
  return {
    value,
    commit: newest.commit ? { sha: newest.commit.row.git_commit, at: newest.commit.at } : null,
    info: newest.info ? newest.info.row : {},
  };
};

export { readHeaderRows };
