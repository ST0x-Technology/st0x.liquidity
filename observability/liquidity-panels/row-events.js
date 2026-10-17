// One row's event lines, fetched when its dialog opens. The board loads
// only the newest 500 liq_event lines, which a busy bot writes within the
// hour, so an older row's events are not among them. The dialog then asks
// Cloud Logging for that row's lines alone, over its own span: a query
// filtered on one id is quick over minutes, where the board's 30 days take
// about 13s. The generator prepends this file to detail.js;
// observability/tests/row-events.test.ts imports it as a module through the
// export line, which the generator drops. lineEntries is prepended from
// log-lines.js.

// Margin around the row's first and newest status lines: an event can be
// logged a little before the status line it leads to, or after.
const ROW_EVENTS_MARGIN_MS = 10 * 60_000;

// How far back the dialog reads a row whose first status line is not its
// start: the board's own range. The table keeps only the newest status
// lines, so a long operation's earlier ones can be gone from it.
const ROW_EVENTS_LOOKBACK_MS = 30 * 24 * 60 * 60_000;

// The most lines one row query returns. Cloud Logging returns 100 by
// default, and a long USDC bridge logs more than that.
const ROW_EVENTS_LIMIT = 1000;

// The Cloud Logging filter for the row's lines: the board's event filter
// and the id, quoted.
const rowEventsFilter = (eventsFilter, id) =>
  `${eventsFilter} jsonPayload.id="${String(id).replace(/["\\]/g, (char) => `\\${char}`)}"`;

// The body of Grafana's /api/ds/query for the row's lines.
const rowEventsRequest = ({ datasource, eventsFilter, project, id, first, newest }) => ({
  queries: [
    {
      refId: 'row-events',
      datasource,
      projectId: project,
      queryType: 'logs',
      queryText: rowEventsFilter(eventsFilter, id),
      maxDataPoints: ROW_EVENTS_LIMIT,
    },
  ],
  from: String(first - ROW_EVENTS_MARGIN_MS),
  to: String(newest + ROW_EVENTS_MARGIN_MS),
});

// A /api/ds/query response's frame for `refId` in the shape lineEntries
// reads (fields with a name and values), or an error with its message.
const responseFrame = (response, refId) => {
  const result = response?.results?.[refId];
  if (!result) throw new Error('no result for the row query');
  if (result.error) throw new Error(result.error);
  const frame = (result.frames || [])[0];
  if (!frame) return { fields: [] };
  return {
    fields: frame.schema.fields.map((field, index) => ({
      name: field.name,
      values: frame.data?.values?.[index] || [],
    })),
  };
};

// The row's entries from Cloud Logging, through Grafana. `fetcher` is the
// browser's fetch.
const fetchRowEvents = async (fetcher, request, id, project) => {
  const response = await fetcher('/api/ds/query', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(request),
  });
  const body = await response.json().catch(() => null);
  if (!response.ok) throw new Error(body?.message || `HTTP ${response.status}`);
  return lineEntries(responseFrame(body, 'row-events'), id, project);
};

// The board's lines and the fetched ones, each event once.
const mergeEntries = (board, fetched) => {
  const seen = new Set();
  return [...board, ...fetched].filter((entry) => {
    if (!entry.event_id) return true;
    if (seen.has(entry.event_id)) return false;
    seen.add(entry.event_id);
    return true;
  });
};

export { ROW_EVENTS_LOOKBACK_MS, rowEventsFilter, rowEventsRequest, responseFrame, fetchRowEvents, mergeEntries };
