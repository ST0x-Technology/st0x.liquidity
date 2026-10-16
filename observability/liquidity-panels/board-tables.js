// Behaviour the board's native tables cannot configure. The generator
// prepends this file to header.js, which installs it once per page;
// dashboard/src/lib/board-tables.test.ts imports it as a module through the
// export line, which the generator drops.
//
// - Hover text: a table cell's <abbr title> or <img title> shows its title
//   at once in a tooltip, instead of the browser's late, faint one. The
//   cell sanitizer keeps `title` on <abbr> and <img> only, so the board's
//   lights use <abbr> and its chain marks <img>.
// - Header hover: a column header named in `headerTips` for its panel
//   explains itself in the same tooltip.
// - Row hover: a cell's <abbr title="row:<key>"> in a panel named in
//   `rowTips` shows its row's flags as a table, read from the panel's data
//   in the dashboard scene: a symbol's every chain row, or one chain's. The
//   flags field packs one digit per column, and each column's `states`
//   names that digit's text, colour and light.
// - Chain tints: a row whose cell holds a chain's mark (an <img> whose alt
//   starts with a name in `rowTints`) takes that chain's tint. It shows
//   through a ratio bar's translucent track; the bar's fill is opaque, so
//   its red or green stays the verdict's.
// - Tree sort: clicking a column name sorts the table, and Grafana has no
//   option to turn that off. Tables whose rows are a tree (an asset row with
//   its chain rows under it) lose the tree on Grafana's sort, so a click on a
//   column name of a panel in `lockedSortPanels` never reaches it. A header
//   named in that panel's `treeSorts` sorts it instead, by a hidden key every
//   row of a group shares and then the row key, so each group stays whole:
//   largest first, then smallest first, then the default order again. The
//   first column restores the default order; another header does nothing.
//   The sort swaps the panel's sortBy transformations in the dashboard scene
//   (window.__grafanaSceneContext) and lives on the panel's data, so a
//   refresh keeps it. The filter button beside a name still works.
// - Plain hover: a cell with a tooltip field (the USD worth of a share
//   amount) opens it from a grey corner triangle only. The triangle is
//   stretched over the whole cell and made invisible, so the pointer
//   anywhere on the cell opens it, like the SPA's hover.
// - Aligned edges: the tables in `alignedPanels` always keep room for a
//   scrollbar, so a table that scrolls ends its last column where one that
//   does not ends it, and their ratio bars line up.

const PANEL_TESTID = 'data-testid Panel header ';
const TOOLTIP_CARET = '[data-testid="data-testid tableng tooltip caret"]';
// An img keeps matching once its title moved to data-liq-tip, so the tip
// still hides when the pointer leaves it.
const TIP_CELL =
  '[role="gridcell"] abbr, [role="gridcell"] img[title], [role="gridcell"] img[data-liq-tip], [role="gridcell"][data-liq-unit]';
const ROW_TIP = 'row:';

// The rules and the listeners follow each render's code and options, so a
// board saved with new ones applies them without a page reload: each render
// removes the listeners the previous one added and adds its own.
// The panel titled `title` in the dashboard scene, if the scene has it.
const scenePanel = (doc, title) =>
  doc.liqSceneOf?.()?.state?.body?.state?.grid?.state?.children?.find(
    (child) => child.state.body?.state?.title === title
  )?.state.body;

const escapeTip = (text) =>
  String(text).replace(/[&<>"]/g, (char) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' })[char]);

// A frame's field by its name or the display name a rename gave it.
const fieldNamed = (frame, names) =>
  frame?.fields?.find((field) =>
    names.some((name) => [field.name, field.config?.displayName, field.state?.displayName].includes(name))
  );

// The chain rows a row tip lists for `key`: a symbol's every chain, or the
// one chain of a symbol/chain key, as [chain name, flag digits]. A symbol
// lists its chains without flags too, digits null, so its tip is the asset's
// overview and never reads as one chain's.
const rowFlags = (doc, title, key) => {
  const config = doc.liqRowTips[title];
  const frame = scenePanel(doc, title)?.state?.$data?.state?.data?.series?.[0];
  const rows = fieldNamed(frame, config.rowFields);
  const flags = fieldNamed(frame, config.flagFields);
  if (!rows || !flags) return [];
  return rows.values.flatMap((row, index) => {
    const [symbol, chain] = String(row).split('/');
    const value = Number(flags.values[index]);
    const wanted = key.includes('/') ? String(row) === key : symbol === key;
    if (!wanted || !chain || !(chain in config.chains)) return [];
    const published = flags.values[index] != null && Number.isFinite(value);
    if (!published) return key.includes('/') ? [] : [[config.chains[chain], null]];
    return [[config.chains[chain], String(Math.round(value))]];
  });
};

// A row tip: its symbol (and chain), then one line per chain with a light
// and a word for each flag.
const rowTipHtml = (doc, title, key) => {
  const config = doc.liqRowTips[title];
  const [symbol, chain] = key.split('/');
  const heading = chain ? `${symbol} on ${config.chains[chain] ?? chain}` : symbol;
  const head = `<div class="liq-tip-title">${escapeTip(heading)}</div>`;
  const rows = rowFlags(doc, title, key);
  if (!rows.some(([, digits]) => digits)) return `${head}<div>${escapeTip(config.empty)}</div>`;
  const cell = (column, digit) => {
    const [text, color, light] = column.states[digit] ?? ['unknown', 'inherit', '?'];
    return `<td><span style="color:${escapeTip(color)}">${escapeTip(light)}</span> ${escapeTip(text)}</td>`;
  };
  const header = config.columns.map((column) => `<th>${escapeTip(column.name)}</th>`).join('');
  const lines = rows
    .map(([name, digits]) =>
      digits
        ? `<tr><th>${escapeTip(name)}</th>${config.columns.map((column, index) => cell(column, digits[index])).join('')}</tr>`
        : `<tr><th>${escapeTip(name)}</th><td colspan="${config.columns.length}">not published</td></tr>`
    )
    .join('');
  return `${head}<table><tr><th></th>${header}</tr>${lines}</table>`;
};

// The sorted header's arrow, from each tree table's sort on its data.
const drawSortArrows = (doc, panel) => {
  let style = doc.getElementById('liq-board-sort');
  if (!style) {
    style = doc.createElement('style');
    style.id = 'liq-board-sort';
    doc.head.appendChild(style);
  }
  style.textContent = Object.keys(doc.liqTreeSorts)
    .map((title) => [title, scenePanel(doc, title)?.state?.$data?.liqSort])
    .filter(([, sort]) => sort)
    .map(
      ([title, sort]) =>
        `${panel(title)} [role="columnheader"][aria-colindex="${sort.column}"] button:not([aria-label])::after` +
        // At the header cell's right edge (the cell is positioned; the name
        // button clips its own overflow), so the text keeps its room.
        `{content:"${sort.desc ? '\\25BC' : '\\25B2'}";position:absolute;right:4px;top:50%;` +
        'transform:translateY(-50%);font-size:9px;color:#6e9fff}'
    )
    .join('');
};

// A click on a tree table's header: the next sort in its cycle (largest
// first, smallest first, default), or the default order from the first
// column.
const treeSort = (doc, panel, title, header, column) => {
  const config = doc.liqTreeSorts[title];
  const data = scenePanel(doc, title)?.state?.$data;
  if (!config || !data?.reprocessTransformations) return;
  const key = config.keys[header];
  if (!key && column !== '1') return;
  data.liqDefault = data.liqDefault || data.state.transformations;
  const current = data.liqSort;
  const next = !key ? null : current?.key !== key ? { key, column, desc: true } : current.desc ? { key, column, desc: false } : null;
  const sortBy = (field, desc) => ({ id: 'sortBy', options: { fields: {}, sort: [{ field, desc }] } });
  let transformations = data.liqDefault;
  if (next) {
    // sortBy sorts stably and the last one applied leads: by the key, and
    // within a group (equal keys) by the row key, which puts the group's
    // head first.
    const rest = data.liqDefault.filter((step) => step.id !== 'sortBy');
    const at = rest.findIndex((step) => step.id === 'organize');
    transformations = [...rest.slice(0, at), sortBy(config.rowField, false), sortBy(key, next.desc), ...rest.slice(at)];
  }
  data.liqSort = next;
  data.setState({ transformations });
  data.reprocessTransformations();
  drawSortArrows(doc, panel);
};

// Unit tags: in a panel named in `unitTags`, a `column` cell gets the
// `text` of the first of its `tags` whose `values` hold its row's `match`
// cell, after its value (an onchain trade's size is in wrapped shares, a
// broker hedge's in underlying shares). On hover it shows the amount in the
// tag's `convert.unit`, through its row's `symbol` wrapper ratio on each
// chain (`wrapperRatios`, underlying shares per wrapped share): times the
// ratio, or divided by it. At today's ratio: the trade's own is not logged. Grafana redraws the table and
// reuses its row elements as it scrolls, so the tags follow every change to
// the panel's cells; tagging only sets attributes, which the observer does
// not watch, so it never feeds itself.
const formatShares = (value) => value.toLocaleString('en-US', { maximumFractionDigits: 3 });

// A number as Grafana's `locale` unit writes it, in the browser's own
// format: "1,020.5" in en-US, "1.020,5" in es-AR.
const parseLocaleNumber = (text) => {
  const parts = new Intl.NumberFormat().formatToParts(12345.6);
  const group = parts.find((part) => part.type === 'group')?.value ?? ',';
  const decimal = parts.find((part) => part.type === 'decimal')?.value ?? '.';
  return Number(text.trim().split(group).join('').split(decimal).join('.'));
};

// The hover text of a tagged `amount` of `symbol`.
const unitTip = (doc, tag, found, symbol, amount) => {
  const { unit, times } = found.convert;
  const ratios = Object.entries(doc.liqWrapperRatios?.[symbol] ?? {});
  if (!Number.isFinite(amount) || ratios.length === 0) {
    return `${found.tip} No wrapper ratio published for ${symbol} yet.`;
  }
  const converted = ratios.map(([chain, ratio]) => {
    const value = times ? amount * ratio : amount / ratio;
    const where = ratios.length > 1 ? ` on ${tag.chains?.[chain] ?? chain}` : '';
    return `${formatShares(value)} ${unit}${where}`;
  });
  return `${formatShares(amount)} ${found.text} ≈ ${converted.join(', ')} at today's wrapper ratio`;
};

const tagUnits = (doc, panel, unitTags) => {
  for (const observer of doc.liqUnitObservers ?? []) observer.disconnect();
  doc.liqUnitObservers = [];
  const Observer = doc.defaultView?.MutationObserver ?? globalThis.MutationObserver;
  for (const [title, tag] of Object.entries(unitTags)) {
    const section = doc.querySelector(panel(title));
    if (!section) continue;
    const apply = () => {
      const headers = [...section.querySelectorAll('[role="columnheader"]')];
      const index = (name) => headers.find((header) => header.textContent.trim() === name)?.getAttribute('aria-colindex');
      const from = index(tag.match);
      const to = index(tag.column);
      const symbolAt = index(tag.symbol);
      for (const row of section.querySelectorAll('[role="row"]')) {
        const cell = to ? row.querySelector(`[role="gridcell"][aria-colindex="${to}"]`) : null;
        if (!cell) continue;
        const source = from ? row.querySelector(`[role="gridcell"][aria-colindex="${from}"]`) : null;
        const found = source ? tag.tags.find(({ values }) => values.includes(source.textContent.trim())) : null;
        if (found) {
          const symbol = symbolAt
            ? (row.querySelector(`[role="gridcell"][aria-colindex="${symbolAt}"]`)?.textContent.trim() ?? '')
            : '';
          const amount = parseLocaleNumber(cell.textContent);
          cell.dataset.liqUnit = found.text;
          cell.dataset.liqTip = found.convert ? unitTip(doc, tag, found, symbol, amount) : found.tip;
        } else if (cell.dataset.liqUnit) {
          delete cell.dataset.liqUnit;
          delete cell.dataset.liqTip;
        }
      }
    };
    apply();
    if (!Observer) continue;
    const observer = new Observer(apply);
    observer.observe(section, { childList: true, subtree: true, characterData: true });
    doc.liqUnitObservers.push(observer);
  }
};

const installBoardTables = (
  doc,
  {
    lockedSortPanels,
    alignedPanels,
    rowTints = {},
    headerTips = {},
    treeSorts = {},
    rowTips = {},
    unitTags = {},
    wrapperRatios = {},
    sceneOf = () => doc.defaultView?.__grafanaSceneContext,
    theme,
  }
) => {
  doc.liqSceneOf = sceneOf;
  doc.liqWrapperRatios = wrapperRatios;
  const panel = (title) => `section[data-testid="${PANEL_TESTID}${title}"]`;
  doc.liqHeaderTips = headerTips;
  doc.liqTreeSorts = treeSorts;
  doc.liqRowTips = rowTips;
  doc.liqPanelSelector = panel;
  const locked = lockedSortPanels.map(panel);
  doc.liqLockedTables = locked;
  const aligned = alignedPanels.map((title) => `${panel(title)} [role="grid"]`);
  let style = doc.getElementById('liq-board-tables');
  if (!style) {
    style = doc.createElement('style');
    style.id = 'liq-board-tables';
    doc.head.appendChild(style);
  }
  style.textContent =
    '.liq-tip{position:fixed;z-index:10000;pointer-events:none;padding:4px 8px;' +
    `border-radius:4px;font-size:12px;white-space:nowrap;background:${theme.colors.background.secondary};` +
    `color:${theme.colors.text.primary};border:1px solid ${theme.colors.border.weak};` +
    'box-shadow:0 2px 8px rgba(0,0,0,.35)}' +
    '.liq-tip-title{font-weight:600;margin-bottom:2px}' +
    '.liq-tip table{border-collapse:collapse}' +
    '.liq-tip th,.liq-tip td{padding:1px 10px 1px 0;text-align:left;font-weight:normal}' +
    `.liq-tip tr:first-child th{color:${theme.colors.text.secondary ?? theme.colors.text.primary};font-size:11px}` +
    `${TOOLTIP_CARET}{top:0!important;left:0!important;right:0!important;bottom:0!important;` +
    'width:auto!important;height:auto!important;opacity:0!important}' +
    locked.map((table) => `${table} [role="columnheader"] button:not([aria-label])`).join(',') +
    (locked.length ? '{cursor:default}' : '') +
    aligned.join(',') +
    (aligned.length ? '{scrollbar-gutter:stable}' : '') +
    Object.entries(rowTints)
      .map(
        ([name, tint]) =>
          `[role="row"]:has(img[alt^="${name}"]) [role="gridcell"]{background-color:${tint}}`
      )
      .join('') +
    // A fixed slot, so the numbers line up whichever tag follows them.
    `[role="gridcell"][data-liq-unit]::after{content:attr(data-liq-unit);display:inline-block;width:1.6em;` +
    `margin-left:3px;text-align:left;font-size:11px;color:${theme.colors.text.secondary ?? theme.colors.text.primary}}`;
  drawSortArrows(doc, panel);
  tagUnits(doc, panel, unitTags);

  for (const [type, listener, capture] of doc.liqBoardTablesListeners ?? []) {
    doc.removeEventListener(type, listener, capture);
  }
  let tip = doc.querySelector('.liq-tip');
  if (!tip) {
    tip = doc.createElement('div');
    tip.className = 'liq-tip';
    tip.hidden = true;
    doc.body.appendChild(tip);
  }
  const hideTip = () => {
    tip.hidden = true;
  };

  // The tip for a header named in headerTips, under its panel's title.
  const headerTip = (target) => {
    const header = target.closest?.('[role="columnheader"]');
    const section = header?.closest('section[data-testid^="data-testid Panel header "]');
    if (!section) return null;
    const title = section.getAttribute('data-testid').slice(PANEL_TESTID.length);
    return doc.liqHeaderTips[title]?.[header.textContent.trim()] ? header : null;
  };
  const showTip = (anchor, text, html = false) => {
    if (html) tip.innerHTML = text;
    else tip.textContent = text;
    tip.hidden = false;
    const box = anchor.getBoundingClientRect();
    const view = doc.documentElement.clientWidth;
    const left = Math.min(Math.max(4, box.left + box.width / 2 - tip.offsetWidth / 2), view - tip.offsetWidth - 4);
    tip.style.left = `${left}px`;
    tip.style.top = `${Math.max(4, box.top - tip.offsetHeight - 6)}px`;
  };

  const onOver = (event) => {
    const header = headerTip(event.target);
    if (header) {
      const title = header.closest('section').getAttribute('data-testid').slice(PANEL_TESTID.length);
      showTip(header, doc.liqHeaderTips[title][header.textContent.trim()]);
      return;
    }
    const light = event.target.closest?.(TIP_CELL);
    if (!light) return;
    // The browser shows `title` on its own too, so the text moves out of it.
    if (light.hasAttribute('title')) {
      light.dataset.liqTip = light.getAttribute('title');
      light.removeAttribute('title');
    }
    const text = light.dataset.liqTip;
    if (!text) return;
    if (text.startsWith(ROW_TIP)) {
      const section = light.closest('section[data-testid^="data-testid Panel header "]');
      const title = section?.getAttribute('data-testid').slice(PANEL_TESTID.length);
      if (doc.liqRowTips[title]) showTip(light, rowTipHtml(doc, title, text.slice(ROW_TIP.length)), true);
      return;
    }
    showTip(light, text);
  };
  const onOut = (event) => {
    if (event.target.closest?.(TIP_CELL) || headerTip(event.target)) hideTip();
  };
  // Capture on the document runs before Grafana's own handlers, so the
  // click never reaches the table.
  const onClick = (event) => {
    const name = event.target.closest?.('[role="columnheader"] button');
    if (!name || name.hasAttribute('aria-label')) return;
    if (!doc.liqLockedTables.some((table) => name.closest(table))) return;
    event.stopPropagation();
    event.preventDefault();
    const section = name.closest('section[data-testid^="data-testid Panel header "]');
    const title = section.getAttribute('data-testid').slice(PANEL_TESTID.length);
    const header = name.closest('[role="columnheader"]');
    treeSort(doc, doc.liqPanelSelector, title, name.textContent.trim(), header.getAttribute('aria-colindex'));
  };
  doc.liqBoardTablesListeners = [
    ['mouseover', onOver, false],
    ['mouseout', onOut, false],
    ['scroll', hideTip, true],
    ['click', onClick, true],
  ];
  for (const [type, listener, capture] of doc.liqBoardTablesListeners) {
    doc.addEventListener(type, listener, capture);
  }
};

export { installBoardTables };
