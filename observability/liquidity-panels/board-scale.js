// Scales the inventory tables down on a narrow window. The generator
// prepends this file to header.js, which calls it on every render;
// observability/tests/board-scale.test.ts imports it as a module through the
// export line, which the generator drops.
//
// Each table's columns have fixed widths that add up to `fixedPx`, then the
// ratio bar takes the rest. On a window too narrow for them and `ratioMinPx`
// of bar, the table would scroll sideways and squeeze the bar. Instead every
// fixed width shrinks by the same factor, and the text with it, so the table
// fits its panel and the bar keeps its room. The tables share the factor, so
// their bars still line up. Grafana sizes a table's columns from the panel's
// own measure, so CSS zoom cannot do this: the widths change in each panel's
// field config in the dashboard scene (window.__grafanaSceneContext), and the
// font size in a CSS rule. Without the scene the tables keep their widths.
//
// A table's width changes without a window resize too: Grafana often draws
// the tables after the header first runs this, the layout moves the panels,
// and the sidebar opens and closes. So each table's panel is
// watched for its own size (ResizeObserver), and for a new table element
// inside it (MutationObserver), which is when Grafana first draws the
// table or draws it again; a scroll only changes cells, so it runs
// nothing.
//
// Each group of tables (`id`) has its own factor, rule, observer and
// listener: the left column's tables share one so their bars line up, and
// the trade and transfer tables each have their own.
//
// Grafana stretches a table's last column with no width over the panel's
// whole width, the scrollbar's place included, so that table scrolls
// sideways by the scrollbar's width at any size. A group that names its
// `stretch` column gets it pinned to what the other columns leave of the
// table's visible width, at least `stretchMinPx`.

// Grafana draws no column narrower than this, whatever its width says.
const MIN_COLUMN_PX = 50;

// A table that would need its text under COMPACT_BELOW to fit every column
// (about 10px), and every table on a phone (below Grafana's md breakpoint,
// one column of full-width panels), is compact: its group hides
// the columns `compactHide` names ({panel title: [column]}), changes the
// properties `compactColumns` names ({column: {property id: value}}, a
// shorter time format and its narrower width, say) and fits the rest,
// `compactFixedPx` wide, with `compactRatioMinPx` of stretching column.
// A 16-inch MacBook with Grafana's menu docked (about 0.74) keeps every
// column, smaller; a 1440px laptop (about 0.6) goes compact at full size.
const PHONE_MAX_PX = 768;
const COMPACT_BELOW = 0.72;

// `fieldConfig` with each column's properties changed to `changes`
// ({column: {property id: value}}) where its overrides set them.
const changedFieldConfig = (fieldConfig, changes) => ({
  ...fieldConfig,
  overrides: fieldConfig.overrides.map((override) => {
    const values = changes[override.matcher.options];
    if (!values) return override;
    return {
      ...override,
      properties: override.properties.map((property) =>
        property.id in values ? { ...property, value: values[property.id] } : property
      ),
    };
  }),
});

// `fieldConfig` with the columns `names` hidden.
const hiddenFieldConfig = (fieldConfig, names) => {
  const hide = { id: 'custom.hideFrom.viz', value: true };
  const listed = new Set(fieldConfig.overrides.map((override) => override.matcher.options));
  return {
    ...fieldConfig,
    overrides: [
      ...fieldConfig.overrides.map((override) =>
        names.includes(override.matcher.options) ? { ...override, properties: [...override.properties, hide] } : override
      ),
      ...names.filter((name) => !listed.has(name)).map((name) => ({ matcher: { id: 'byName', options: name }, properties: [hide] })),
    ],
  };
};
const hidden = (override) =>
  override.properties.some((property) => property.id === 'custom.hideFrom.viz' && property.value === true);

// Below this the text gets too small to read; the table scrolls instead.
const MIN_TABLE_SCALE = 0.6;
// The cells' font size at full scale (Grafana's table body).
const TABLE_FONT_PX = 14;

// The factor for a table `width` px wide: 1 when the columns and the bar
// fit, else smaller, in steps of 0.02 so a resize does not redraw for every
// pixel.
const tableScale = (width, fixedPx, ratioMinPx) => {
  if (!(width > 0)) return 1;
  const fit = width / (fixedPx + ratioMinPx);
  if (fit >= 1) return 1;
  return Math.max(MIN_TABLE_SCALE, Math.floor(fit * 50) / 50);
};

// `fieldConfig` with every fixed width times `scale`, each also its column's
// minimum (Grafana's default minimum, 50px, would stop it shrinking).
const scaledFieldConfig = (fieldConfig, scale) => ({
  ...fieldConfig,
  overrides: fieldConfig.overrides.map((override) => {
    const width = override.properties.find((property) => property.id === 'custom.width');
    if (!width) return override;
    const px = Math.round(width.value * scale);
    return {
      ...override,
      properties: [
        ...override.properties.filter((property) => property.id !== 'custom.width' && property.id !== 'custom.minWidth'),
        { id: 'custom.width', value: px },
        { id: 'custom.minWidth', value: px },
      ],
    };
  }),
});

// `fieldConfig` with the column `name` exactly `px` wide.
const pinnedFieldConfig = (fieldConfig, name, px) => {
  const pin = { id: 'custom.width', value: px };
  const pinned = fieldConfig.overrides.some((override) => override.matcher.options === name);
  return {
    ...fieldConfig,
    overrides: pinned
      ? fieldConfig.overrides.map((override) =>
          override.matcher.options === name
            ? { ...override, properties: [...override.properties.filter((property) => property.id !== 'custom.width'), pin] }
            : override
        )
      : [...fieldConfig.overrides, { matcher: { id: 'byName', options: name }, properties: [pin] }],
  };
};

// What the columns of `fieldConfig` other than `stretch` leave of a table
// `width` px wide, each counted at Grafana's minimum at least, and never
// under `minPx`.
const stretchLeft = (fieldConfig, width, stretch, minPx) => {
  const others = fieldConfig.overrides
    .filter((override) => override.matcher.options !== stretch && !hidden(override))
    .flatMap((override) => override.properties.filter((property) => property.id === 'custom.width'))
    .reduce((sum, property) => sum + Math.max(MIN_COLUMN_PX, property.value), 0);
  return Math.max(minPx, Math.floor(width - others));
};

const scaleTables = (
  doc,
  scene,
  {
    id = '',
    panels,
    fixedPx,
    ratioMinPx,
    stretch,
    stretchMinPx = 0,
    cellFontPx,
    compactHide = {},
    compactColumns = {},
    compactFixedPx = fixedPx,
    compactRatioMinPx = ratioMinPx,
  },
  win
) => {
  const grid = scene?.state?.body?.state?.grid;
  if (!grid?.state?.children || panels.length === 0) return;
  const vizPanel = (title) => grid.state.children.find((child) => child.state.body?.state?.title === title)?.state.body;
  const section = (title) => `section[data-testid="data-testid Panel header ${title}"]`;

  const styleId = id ? `liq-board-scale-${id}` : 'liq-board-scale';
  let style = doc.getElementById(styleId);
  if (!style) {
    style = doc.createElement('style');
    style.id = styleId;
    doc.head.appendChild(style);
  }

  const apply = () => {
    // The tables share one width (one column), so the first one present
    // sets the factor for all.
    const table = panels.map((title) => doc.querySelector(`${section(title)} [role="grid"]`)).find(Boolean);
    const width = table?.clientWidth;
    const compact =
      (win.innerWidth > 0 && win.innerWidth <= PHONE_MAX_PX) ||
      (width > 0 && width / (fixedPx + ratioMinPx) < COMPACT_BELOW);
    const scale = compact
      ? tableScale(width, compactFixedPx, compactRatioMinPx)
      : tableScale(width, fixedPx, ratioMinPx);
    // The headers at Grafana's size, scaled; the cells at the group's own
    // size when it sets one.
    style.textContent =
      (scale < 1
        ? panels.map((title) => `${section(title)} [role="grid"]`).join(',') +
          `{font-size:${(TABLE_FONT_PX * scale).toFixed(1)}px}`
        : '') +
      (cellFontPx
        ? panels.map((title) => `${section(title)} [role="gridcell"]`).join(',') +
          `{font-size:${(cellFontPx * scale).toFixed(1)}px}`
        : '');
    for (const title of panels) {
      const panel = vizPanel(title);
      if (!panel?.onFieldConfigChange) continue;
      panel.liqFieldConfig = panel.liqFieldConfig || panel.state.fieldConfig;
      const base = compact
        ? changedFieldConfig(hiddenFieldConfig(panel.liqFieldConfig, compactHide[title] ?? []), compactColumns)
        : panel.liqFieldConfig;
      let next = scale < 1 ? scaledFieldConfig(base, scale) : base;
      let applied = `${compact ? 'compact' : 'wide'}|${scale}`;
      const own = doc.querySelector(`${section(title)} [role="grid"]`);
      if (stretch && own?.clientWidth > 0) {
        // Its text shrinks with the others, so its minimum does too.
        const px = stretchLeft(next, own.clientWidth, stretch, Math.round(stretchMinPx * scale));
        next = pinnedFieldConfig(next, stretch, px);
        applied += `|${px}`;
      }
      if (panel.liqScale === applied) continue;
      panel.liqScale = applied;
      panel.onFieldConfigChange(next);
    }
  };
  // Each render watches the panels present now; Grafana can replace a
  // panel's element, so the last render's observer goes.
  const watch = () => {
    doc.liqScaleObservers = doc.liqScaleObservers || {};
    for (const observer of doc.liqScaleObservers[id] ?? []) observer.disconnect();
    const observers = [];
    const sections = panels.map((title) => doc.querySelector(section(title))).filter(Boolean);
    if (win.ResizeObserver) {
      const observer = new win.ResizeObserver(() => win.setTimeout(apply, 0));
      for (const found of sections) observer.observe(found);
      observers.push(observer);
    }
    if (win.MutationObserver) {
      for (const found of sections) {
        let drawn = found.querySelector('[role="grid"]');
        const observer = new win.MutationObserver(() => {
          const table = found.querySelector('[role="grid"]');
          if (table === drawn) return;
          drawn = table;
          win.setTimeout(apply, 0);
        });
        observer.observe(found, { childList: true, subtree: true });
        observers.push(observer);
      }
    }
    doc.liqScaleObservers[id] = observers;
    apply();
  };
  watch();

  grid.liqScaleTables = grid.liqScaleTables || {};
  if (grid.liqScaleTables[id]) return;
  grid.liqScaleTables[id] = true;
  for (const title of panels) vizPanel(title)?.state?.$data?.subscribeToState?.(() => win.setTimeout(watch, 0));
  // One listener per window and group, as in board-layout.js: a new scene
  // replaces the last one's.
  win.liqScaleTablesResize = win.liqScaleTablesResize || {};
  if (win.liqScaleTablesResize[id]) win.removeEventListener?.('resize', win.liqScaleTablesResize[id]);
  win.liqScaleTablesResize[id] = () => win.setTimeout(apply, 0);
  win.addEventListener('resize', win.liqScaleTablesResize[id]);
};

export { scaleTables, tableScale, scaledFieldConfig, stretchLeft, PHONE_MAX_PX };
