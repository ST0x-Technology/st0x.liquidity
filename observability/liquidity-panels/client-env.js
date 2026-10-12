// The operations client a board command runs with: the client form of the
// SPA's LIQUIDITY_CLIENT and forClientEnv (dashboard/src/lib/transfer.ts).
// The generator prepends this file to header.js and detail.js;
// dashboard/src/lib/transfer-board.test.ts imports it as a module through the
// export line, which the generator drops, and checks it against the SPA.

const LIQUIDITY_CLIENT = 'st0x-liquidity-client --env production';

// The board always knows its environment: the selector's text is production
// or staging. The SPA shows a placeholder when its environment is unknown.
const boardEnv = (envText) => (envText === 'staging' ? 'staging' : 'production');

const forClientEnv = (command, env) => command.replace('--env production', `--env ${env}`);

const clientFor = (envText) => forClientEnv(LIQUIDITY_CLIENT, boardEnv(envText));

export { LIQUIDITY_CLIENT, boardEnv, forClientEnv, clientFor };
