// The detail dialog's reading of Cloud Logging frames: a row's entries, and
// the bot's event timeline. The generator prepends this file to detail.js;
// dashboard/src/lib/transfer-board.test.ts imports it as a module through
// the export line, which the generator drops.

// A JSON object, or null for anything else (no value, bad JSON, a string).
const parseObject = (value) => {
  if (value !== null && typeof value === 'object') return value;
  try {
    const parsed = JSON.parse(value);
    return parsed !== null && typeof parsed === 'object' ? parsed : null;
  } catch (error) {
    return null;
  }
};

// The entries of `id` in one frame of the plugin's log-lines shape, newest
// first like the frame: each line's time and payload. The exporter's body
// is its JSON payload. A bot line's body is its message, so its payload
// comes from the flattened `jsonPayload.<key>` labels, which the exporter's
// lines carry too. A line from another project than `project` belongs to
// the environment the board showed before a switch and is dropped. A line
// whose `event_id` came before (the bot's lines, shipped twice) is dropped.
const lineEntries = (frame, id, project) => {
  if (!frame) return [];
  const column = (name) => frame.fields.find((field) => field.name === name);
  const times = column('timestamp');
  const bodies = column('body');
  const labels = column('labels');
  const count = times ? times.values.length : 0;
  const seen = new Set();
  const entries = [];
  for (let index = 0; index < count; index++) {
    const flat = (labels && parseObject(labels.values[index])) || {};
    const lineProject = flat['resource.labels.project_id'];
    if (lineProject && lineProject !== project) continue;
    const fromLabels = {};
    for (const [key, value] of Object.entries(flat)) {
      if (key.startsWith('jsonPayload.')) fromLabels[key.slice('jsonPayload.'.length)] = value;
    }
    const payload = (bodies && parseObject(bodies.values[index])) || fromLabels;
    if (!payload.id || String(payload.id) !== id) continue;
    if (payload.event_id) {
      if (seen.has(payload.event_id)) continue;
      seen.add(payload.event_id);
    }
    entries.push({ time: Number(times.values[index]), ...payload });
  }
  return entries;
};

// A row's event timeline from its liq_event entries, oldest first: the
// events of a trade, or of a transfer of `kind` (each transfer kind is its
// own aggregate). A trade's events are not matched on venue, because a
// venue correction changes it. Each payload is parsed from its JSON string.
const eventTimeline = (entries, parent, kind) =>
  entries
    .filter((entry) => entry.parent === parent && (parent !== 'transfer' || entry.kind === kind))
    .map((entry) => ({ ...entry, payload: parseObject(entry.payload) || {} }))
    .sort((left, right) => Number(left.sequence) - Number(right.sequence));

// Whether a row's event timeline misses events: the event store numbers an
// aggregate's events from 1 without gaps, so a timeline that does not start
// at 1, skips a number, or ends before the event of the row's latest status
// line (`rowEventId`, `<aggregate>:<id>:<sequence>`) lacks lines: lines not
// loaded yet (the events scan ran before the row's), the newest-500 window,
// or a line the bot never wrote.
const timelineIncomplete = (events, rowEventId) => {
  const match = /:(\d+)$/.exec(rowEventId || '');
  const last = events.length > 0 ? Number(events[events.length - 1].sequence) : 0;
  return (
    events.some((event, index) => Number(event.sequence) !== index + 1) ||
    (match !== null && Number(match[1]) > last)
  );
};

// The panel errors that concern the source shown. A failed query of the
// other source, or of the event timeline, leaves the rows usable, so it does
// not block them. An error without a refId blocks: it cannot be placed.
const blockingErrors = (errors, source) => {
  const other = source === 'bot' ? 'exporter-' : 'bot-';
  return errors.filter(
    (error) => !error.refId || (error.refId !== 'bot-events' && !error.refId.startsWith(other))
  );
};

// What the panel's last refresh says about the queries the source shown
// reads. Grafana's Mixed datasource keeps only the last failing query's
// errors (runRequest's processResponsePacket overwrites error and errors per
// packet), so an Error state whose listed errors do not block can still hide
// a failed table query, and its frames can be the ones the failed refresh
// kept. `failed`: a listed error blocks, or the state is Error with none
// listed. `unsure`: the state is Error and only errors that do not block are
// listed, so a row shown can be out of date. `eventsFailed`: the event query
// is among the listed errors, whether Grafana gave a list or one error.
const queryState = (panelData, source) => {
  const listed = panelData?.errors || (panelData?.error ? [panelData.error] : []);
  const blocking = blockingErrors(listed, source);
  const isError = panelData?.state === 'Error';
  return {
    listed,
    blocking,
    failed: isError && (listed.length === 0 || blocking.length > 0),
    unsure: isError && listed.length > 0 && blocking.length === 0,
    eventsFailed: listed.some((error) => error.refId === 'bot-events'),
  };
};

export { lineEntries, eventTimeline, timelineIncomplete, blockingErrors, queryState };
