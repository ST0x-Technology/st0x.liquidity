// Lays out the Dashboard tab's two columns to fit the window. The generator
// prepends this file to header.js, which calls it on every render;
// observability/tests/board-layout.test.ts imports it as a module through the
// export line, which the generator drops.
//
// This ends both columns at the window's bottom, again on every resize: on
// the last whole grid unit that fits, and the two bottom panels then
// stretch over the pixels left (a CSS rule on their grid items, since the
// grid only moves in whole units). In the left column each `fit` table
// takes the grid units its rows need and `fill` the rest; the `split`
// panels of the right column share it equally. Without the page's panels
// to measure, both columns end on the lower of the generator's two
// bottoms. The header takes Grafana's autofit off this board: it would
// squeeze every panel into a short window instead of letting it scroll.
//
// It reaches the layout through window.__grafanaSceneContext, the dashboard
// scene Grafana exposes for debugging. Without it, or without the panels,
// the board keeps the generator's heights.

// A grid unit is a 30px cell and an 8px margin. A table panel needs its
// title bar, padding and header row (91px) plus the margin, and a row per
// table row at the "md" cell height.
const GRID_UNIT_PX = 38;
const GRID_MARGIN_PX = 8;
// The page's own margin under the last panels.
const PAGE_BOTTOM_PX = 8;
const TABLE_CHROME_PX = 99;
const TABLE_ROW_PX = 42;
// Below Grafana's md breakpoint the dashboard is one column of full-width
// panels, a phone's. Grafana stacks them in grid order, in the page's flow,
// which puts Trades between the USD tables; this orders the left column's
// tables first and the right column's after them instead (CSS rules on the
// grid and its items, as for the stretch). A USD table takes the height its
// rows need, and the long tables a share of the screen under Grafana's top
// bar, so they scroll inside and the page still shows the next panel's
// title.
const PHONE_LAYOUT_MAX_PX = 768;
const PHONE_TOP_BAR_PX = 48;
const PHONE_SCROLL_SHARE = 0.75;
const MIN_PHONE_TABLE_PX = 300;
// A phone's table is a pixel taller than the desktop's sum, so a USD table
// sized to its rows would scroll by that pixel.
const PHONE_TABLE_SLACK_PX = 2;
// Grafana pins its dashboard controls (time range, variables and links)
// over a phone's board, a quarter of the screen. On a phone the controls
// scroll away with the board instead: the page scrolls, not the board's
// own container. A panel's time range ("Last 5 minutes") hides too: on a
// phone it cuts off the title's total. The controls themselves wrap to
// five rows on a phone; there they are one row that scrolls sideways: the
// environment, the tab links, then the time range and refresh, without the
// buttons a phone does not need (moving and zooming the time range,
// sharing and editing). Only on a page with this board's Equities panel,
// since the style stays in the page when Grafana opens another dashboard.
const PHONE_SPLITTER = '[data-testid="data-testid DashboardEditPaneSplitter primary body"]';
const PHONE_BODY = '[data-testid="data-testid DashboardEditPaneSplitter body container"]';
const PHONE_CONTROLS = '[data-testid="data-testid dashboard controls"]';
const PHONE_HIDDEN_CONTROLS = [
  '[data-testid*="timepicker-move"]',
  '[data-testid*="timepicker-zoom-out"]',
  ':has(> [data-testid="data-testid Edit dashboard button"])',
];

// A window too short to give the fill table this much (three rows) gets it
// anyway, and the page scrolls: a squeezed Equities of one or two rows is
// worse than a page that scrolls by a few.
const MIN_FILL_UNITS = 6;

const unitsFor = (rows) => Math.ceil((TABLE_CHROME_PX + TABLE_ROW_PX * rows) / GRID_UNIT_PX);
const bottomOf = (column) => column[0].state.y + column.reduce((sum, found) => sum + found.state.height, 0);
const panelOf = (found) => `section[data-testid="data-testid Panel header ${found.state.body.state.title}"]`;
const sectionOf = (doc, found) => doc?.querySelector(panelOf(found));

// The page scroll, controls and time range rules, for the board whose panel
// `board` names. They scope to the page through `body:has(...)`: a :has()
// inside another is not valid CSS, and the browser drops the whole rule.
const phoneScroll = (board) => {
  const page = `body:has(${board})`;
  const splitter = `${page} ${PHONE_SPLITTER}`;
  const controls = `${page} ${PHONE_CONTROLS}`;
  const links = `${controls} > [class*="dashboard-links"]`;
  const time = `${controls} > :has([data-testid="data-testid TimePicker Open Button"])`;
  return (
    `@media (max-width:${PHONE_LAYOUT_MAX_PX}px){` +
    `${page} :has(> ${PHONE_SPLITTER}){overflow-y:auto!important}` +
    `${splitter}{overflow:visible!important;flex:none!important;height:auto!important}` +
    `${splitter} ${PHONE_BODY}{overflow:visible!important;flex:none!important;height:auto!important}` +
    `${splitter} [data-testid="title-items-container"] > :has([data-testid="icon-clock-nine"]){display:none!important}` +
    `${controls}{display:flex!important;flex-flow:row nowrap!important;align-items:center!important;gap:8px!important;` +
    `overflow-x:auto!important;scrollbar-width:none!important;padding:8px 16px!important}` +
    `${controls} > *{flex:none!important;margin:0!important;width:auto!important}` +
    `${controls} [data-testid="data-testid template variable"]{order:1}` +
    `${links}{order:2;display:flex!important;flex-wrap:nowrap!important;gap:8px!important}` +
    `${links} > *{flex:none!important;margin:0!important}` +
    `${time}{order:3;display:flex!important;flex-direction:row!important;gap:8px!important}` +
    PHONE_HIDDEN_CONTROLS.map((hidden) => `${controls} ${hidden}`).join(',') +
    `{display:none!important}}`
  );
};

// The phone stack's rules: the header (the grid's first item) first, then
// `column`'s panels in that order, `heights` px each. Any other item is the
// row detail's (detail.js): empty, with its margin a gap under the header.
// Its dialog opens in the page's top layer, so that item takes no room.
const phoneStack = (column, heights) => {
  const grid = `.react-grid-layout:has(> .react-grid-item ${panelOf(column[0])})`;
  const others = column.map((found) => `:not(:has(${panelOf(found)}))`).join('');
  return (
    `${grid}{display:flex!important;flex-direction:column!important;height:auto!important}` +
    `${grid} > .react-grid-item:not(:first-child)${others}{height:0!important;margin:0!important;overflow:hidden!important}` +
    column
      .map((found, index) => {
        const panel = panelOf(found);
        return (
          `.react-grid-item:has(${panel}){order:${index + 1}!important;height:${heights[index]}px!important}` +
          `${panel}{height:${heights[index]}px!important}`
        );
      })
      .join('')
  );
};

// How far the boxes around `element` have scrolled it up.
const scrolledAbove = (element) => {
  let scrolled = 0;
  for (let node = element.parentElement; node; node = node.parentElement) scrolled += node.scrollTop || 0;
  return scrolled;
};

// Where the window ends, in grid units and the pixels past the last whole
// unit, from where `first` sits on the page; null without it. The page
// position, not the scrolled one: a short window scrolls, and a reflow
// after a scroll must not grow the tables by what was scrolled.
const windowBottom = (win, first) => {
  const section = sectionOf(win.document, first);
  if (!section || !(win.innerHeight > 0)) return null;
  const top = section.getBoundingClientRect().top + scrolledAbove(section);
  const origin = top - first.state.y * GRID_UNIT_PX;
  // An item `n` units down ends at origin + n units, less one margin.
  const room = win.innerHeight - PAGE_BOTTOM_PX - origin + GRID_MARGIN_PX;
  const units = Math.floor(room / GRID_UNIT_PX);
  return { units, extraPx: Math.floor(room - units * GRID_UNIT_PX) };
};

const fitColumns = (scene, { fit, fill, split }, win) => {
  const grid = scene?.state?.body?.state?.grid;
  if (!grid?.state?.children || grid.liqFitColumns) return;

  const item = (title) => grid.state.children.find((child) => child.state.body?.state?.title === title);
  const left = [...fit, fill].map(item);
  const right = split.map(item);
  if ([...left, ...right].some((found) => !found)) return;
  grid.liqFitColumns = true;

  // The rows each table draws: its query's frame after the panel's
  // transformations, one row per venue. None until the data arrives.
  const rows = (found) => found.state.body.state.$data?.state?.data?.series?.[0]?.length;

  // The page's style for the board: `css` after the phone's scroll rule and
  // a dark, thin scrollbar (the system's light one is the brightest thing on
  // a dark board).
  const restyle = (css) => {
    const doc = win.document;
    if (!doc?.head) return;
    let style = doc.getElementById('liq-board-stretch');
    if (!style) {
      style = doc.createElement('style');
      style.id = 'liq-board-stretch';
      doc.head.appendChild(style);
    }
    const board = panelOf(left[left.length - 1]);
    style.textContent =
      phoneScroll(board) +
      `html:has(${board}),body:has(${board}) *{scrollbar-color:#3a3d45 transparent;scrollbar-width:thin}` +
      css;
  };

  // The bottom panels `extraPx` taller than their grid units, so the columns
  // end at the window's bottom; `bottoms` are [item, its height in units].
  const stretch = (extraPx, bottoms) =>
    restyle(
      extraPx > 0
        ? bottoms
            .map(([found, units]) => {
              const panel = panelOf(found);
              const px = units * GRID_UNIT_PX - GRID_MARGIN_PX + extraPx;
              return `.react-grid-item:has(${panel}),${panel}{height:${px}px!important}`;
            })
            .join('')
        : '',
    );

  // A phone's table `rows` long in px.
  const tablePx = (rows) => TABLE_CHROME_PX - GRID_MARGIN_PX + TABLE_ROW_PX * rows + PHONE_TABLE_SLACK_PX;

  const phone = () => {
    const scrolling = Math.max(
      MIN_PHONE_TABLE_PX,
      Math.round((win.innerHeight - PHONE_TOP_BAR_PX) * PHONE_SCROLL_SHARE),
    );
    // The long tables scroll inside; a USD table keeps its height until its
    // data arrives.
    const heights = [...left, ...right].map((found, index) => {
      const count = rows(found);
      if (index >= left.length - 1) return count ? Math.min(tablePx(count), scrolling) : scrolling;
      return count ? tablePx(count) : found.state.height * GRID_UNIT_PX - GRID_MARGIN_PX;
    });
    restyle(phoneStack([...left, ...right], heights));
  };

  const reflow = () => {
    if (win.innerWidth > 0 && win.innerWidth <= PHONE_LAYOUT_MAX_PX) {
      phone();
      return;
    }
    const fits = windowBottom(win, left[0]);
    const fitted = left.slice(0, -1).map((found) => {
      const count = rows(found);
      return count ? unitsFor(count) : found.state.height;
    });
    const above = left[0].state.y + fitted.reduce((sum, height) => sum + height, 0);
    const room = (fits ? fits.units : Math.max(bottomOf(left), bottomOf(right))) - above;
    const short = room < MIN_FILL_UNITS;
    const rest = short ? MIN_FILL_UNITS : room;
    const bottom = above + rest;

    const rightTotal = bottom - right[0].state.y;
    const share = Math.floor(rightTotal / right.length);
    const shares = right.map((_, index) => (index === right.length - 1 ? rightTotal - share * index : share));

    let changed = false;
    const place = (column, heights) => {
      let y = column[0].state.y;
      heights.forEach((height, index) => {
        const found = column[index];
        // Only real changes, so a reflow that its own update triggers stops.
        if (found.state.y !== y || found.state.height !== height) {
          found.setState({ y, height });
          changed = true;
        }
        y += height;
      });
    };
    place(left, [...fitted, rest]);
    place(right, shares);
    // A short window's page scrolls, so there is no bottom to stretch to.
    stretch(fits && !short ? fits.extraPx : 0, [
      [left[left.length - 1], rest],
      [right[right.length - 1], shares[shares.length - 1]],
    ]);
    // The grid draws its items' new places only when told to.
    if (changed) grid.forceRender?.();
  };

  reflow();
  left.forEach((found) => found.state.body.state.$data?.subscribeToState?.(reflow));
  // The window's bottom moves when it changes size. One
  // listener per window: a new dashboard scene replaces the last one's,
  // which would keep the old scene alive and reflow it on every resize.
  if (win.liqFitColumnsResize) win.removeEventListener?.('resize', win.liqFitColumnsResize);
  win.liqFitColumnsResize = () => win.setTimeout(reflow, 0);
  win.addEventListener('resize', win.liqFitColumnsResize);
};

export { fitColumns };
