// Each inventory table's total in USD, after its title. The generator
// prepends this file to header.js, which calls it on every render;
// dashboard/src/lib/board-totals.test.ts imports it as a module through the
// export line, which the generator drops.
//
// A panel title is fixed text in Grafana, so the total is a CSS ::after on
// the title, from the panel's own data in the dashboard scene
// (window.__grafanaSceneContext): the sum of the named columns over every
// row, by their field names (organize only renames for display). It follows
// each panel's data as it refreshes. Without the scene, or before the data
// arrives, a title shows no total. A NaN cell is a value the generator
// could not price (an asset without a live price), so the total then says
// it is partial; an empty cell (null) is only a row without that column.
//
// A table's entry is its columns, or { columns, stamp, staleAfterSeconds }:
// `stamp` names a column of Unix seconds (when each row's price was quoted),
// and the title also says how old the oldest is once it is older than
// `staleAfterSeconds`, since its values then rest on prices that are not
// live. `parts`, [{ label, columns, rows }], adds a total over some rows
// only after the table's own, each "label $N": `rows` is a pattern for the
// row key in the `rowField` column (an Equities chain row is symbol/chain).

const usd = (value) => `$${Math.round(value).toLocaleString('en-US')}`;

// "Oct 16, 23:41 UTC" for Unix seconds.
const utcStamp = (seconds) => {
  const at = new Date(seconds * 1000);
  const month = at.toLocaleString('en-US', { month: 'short', timeZone: 'UTC' });
  const time = at.toISOString().slice(11, 16);
  return `${month} ${at.getUTCDate()}, ${time} UTC`;
};

// The oldest Unix seconds in `column` of a frame, or null.
const oldestStamp = (frame, column) => {
  const values = (frame?.fields ?? []).find((field) => field.name === column)?.values ?? [];
  const stamps = values.map(Number).filter((value) => Number.isFinite(value) && value > 0);
  return stamps.length ? Math.min(...stamps) : null;
};

// The sum of `columns` over every row of a frame (or the rows `keep`
// takes, by index), and whether a cell had no price, or null when none of
// them has a number.
const frameTotal = (frame, columns, keep = () => true) => {
  let sum = 0;
  let found = false;
  let partial = false;
  for (const field of frame?.fields ?? []) {
    if (!columns.includes(field.name)) continue;
    for (const [index, value] of [...field.values].entries()) {
      if (!keep(index)) continue;
      if (typeof value === 'number' && Number.isNaN(value)) partial = true;
      const number = Number(value);
      if (value === null || value === '' || !Number.isFinite(number)) continue;
      sum += number;
      found = true;
    }
  }
  return found ? { sum, partial } : null;
};

const showTotals = (doc, scene, totals, now = () => Date.now()) => {
  const grid = scene?.state?.body?.state?.grid;
  if (!grid?.state?.children) return;
  const panel = (title) => grid.state.children.find((child) => child.state.body?.state?.title === title)?.state.body;

  let style = doc.getElementById('liq-board-totals');
  if (!style) {
    style = doc.createElement('style');
    style.id = 'liq-board-totals';
    doc.head.appendChild(style);
  }
  const draw = () => {
    style.textContent = Object.entries(totals)
      .map(([title, spec]) => {
        const { columns, stamp, staleAfterSeconds = 0, parts = [], rowField } = Array.isArray(spec)
          ? { columns: spec }
          : spec;
        const frame = panel(title)?.state?.$data?.state?.data?.series?.[0];
        const total = frameTotal(frame, columns);
        if (total === null) return '';
        const keys = (frame?.fields ?? []).find((field) => field.name === rowField)?.values ?? [];
        const split = parts
          .map(({ label, columns: partColumns, rows }) => {
            const pattern = new RegExp(rows);
            const part = frameTotal(frame, partColumns, (index) => pattern.test(String(keys[index] ?? '')));
            return part === null ? '' : ` \\00B7  ${label} ${usd(part.sum)}`;
          })
          .join('');
        const oldest = stamp ? oldestStamp(frame, stamp) : null;
        const stale = oldest !== null && now() / 1000 - oldest > staleAfterSeconds;
        const text =
          usd(total.sum) +
          (total.partial ? ' (some unpriced)' : '') +
          split +
          (stale ? ` \\00B7  prices as of ${utcStamp(oldest)}` : '');
        return (
          `section[data-testid="data-testid Panel header ${title}"] h2::after` +
          `{content:" \\00B7  ${text}";font-weight:400}`
        );
      })
      .join('');
  };
  draw();

  if (grid.liqTotals) return;
  grid.liqTotals = true;
  Object.keys(totals).forEach((title) => panel(title)?.state?.$data?.subscribeToState?.(draw));
};

export { showTotals, frameTotal };
