// The operations client a board command runs with. The generator prepends
// this file to header.js and detail.js; observability/tests/recovery.test.ts
// imports it as a module through the export line, which the generator drops.

const LIQUIDITY_CLIENT = 'st0x-liquidity-client --env production';

// The board always knows its environment: the selector's text is production
// or staging.
const boardEnv = (envText) => (envText === 'staging' ? 'staging' : 'production');

const forClientEnv = (command, env) => command.replace('--env production', `--env ${env}`);

const clientFor = (envText) => forClientEnv(LIQUIDITY_CLIENT, boardEnv(envText));

export { LIQUIDITY_CLIENT, boardEnv, forClientEnv, clientFor };
