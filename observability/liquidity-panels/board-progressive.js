// Loads the Trades and Rebalances tables in two steps. The generator
// prepends this file to header.js, which calls it on every render;
// observability/tests/board-progressive.test.ts imports it as a module
// through the export line, which the generator drops.
//
// A log table's panel asks for only the newest entries that fill a screen
// (its maxDataPoints, the row limit of the Cloud Logging plugin), so the
// board shows its first rows as soon as the other panels. Once that first
// answer is in, the table asks again for its whole history, `full` entries,
// and Grafana keeps the first rows on screen while the rest loads. A
// refresh then reads the whole history, as the table already shows it.
//
// It reaches the panels through window.__grafanaSceneContext, the dashboard
// scene Grafana exposes for debugging. Without it the tables keep their
// first rows only.

// The query runner under a panel: its data, under any transformations.
const queryRunner = (panel) => {
  let data = panel?.state?.$data;
  while (data && !data.state?.queries && data.state?.$data) data = data.state.$data;
  return data?.runQueries ? data : null;
};

// `panels`: {panel title: entries in its whole history}.
const loadInFull = (scene, panels) => {
  const grid = scene?.state?.body?.state?.grid;
  if (!grid?.state?.children) return;
  for (const [title, full] of Object.entries(panels)) {
    const panel = grid.state.children.find((child) => child.state.body?.state?.title === title)?.state.body;
    const runner = queryRunner(panel);
    // Once per runner: a new dashboard scene brings new runners.
    if (!runner || runner.liqFullLoad) continue;
    runner.liqFullLoad = true;
    const widen = () => {
      if (runner.state.data?.state !== 'Done' || !(runner.state.maxDataPoints < full)) return false;
      runner.setState({ maxDataPoints: full });
      runner.runQueries();
      return true;
    };
    if (widen()) continue;
    const subscription = runner.subscribeToState(() => {
      if (widen() || runner.state.maxDataPoints >= full) subscription?.unsubscribe?.();
    });
  }
};

export { loadInFull };
