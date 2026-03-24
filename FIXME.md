# FIXME.md

## wtf are we sending to alpaca in terms of quantity

-- be sure to see changes that recently landed on master, especially ones
related to rain float switch

1. alpaca order request should probably still use fixed decimal since they only
   support max 9 decimal points
2. Broker rejected order -- shouldn't have
3. `{"code":40310000,"message":"no day trades permitted based on your previous day account equity being under $25,000"}`
   -- wtf is up with this?

## why are our logs so bad?

1. placing order for more than 9 decimals but the fact that that gets
   transformed isn't made clear
2. can we make the formatting so that all the messages fit on the screen and
   then additional properties go after them into infinity? keeping the stuff
   that's before the message currently still before it but maybe in a more
   compact way
3. "Place succeeded" but "Broker rejected order", another
4. `{"code":40310000,"message":"no day trades permitted based on your previous day account equity being under $25,000"}`
   -- can we not put the code into log properties and put the log message in the
   log message?

## relevant logs

```
2026-03-24T17:07:47.941293Z  INFO check_and_execute_accumulated_positions: st0x_hedge::conductor: Found 1 accumulated positions ready for execution
2026-03-24T17:07:47.941308Z  INFO check_and_execute_accumulated_positions: st0x_hedge::conductor: Executing accumulated position symbol=RKLB shares=0.996350331351928059 direction=Sell offchain_order_id=2e996ac6-e869-4daf-acd3-9f18b43f7f43
2026-03-24T17:07:47.951521Z  INFO check_and_execute_accumulated_positions: st0x_hedge::conductor: Position::PlaceOffChainOrder succeeded offchain_order_id=2e996ac6-e869-4daf-acd3-9f18b43f7f43 symbol=RKLB
2026-03-24T17:07:47.952030Z DEBUG check_and_execute_accumulated_positions: st0x_execution::alpaca_broker_api::order: Placing Alpaca Broker API market order: SELL 0.996350331351928059 shares of RKLB (time_in_force: Day)
2026-03-24T17:07:47.952360Z DEBUG check_and_execute_accumulated_positions: st0x_execution::alpaca_broker_api::client: Placing order at https://broker-api.alpaca.markets/v1/trading/accounts/c0069a7b-7894-3ea4-b0a0-5d7c2dab0da7/orders: OrderRequest { symbol: Symbol("RKLB"), quantity: Positive(FractionalShares(Float(0xffffffee00000000000000000000000000000000000000000dd3bf58f4130cfb))), side: Sell, order_type: "market", time_in_force: "day", extended_hours: false }
2026-03-24T17:07:47.980280Z  INFO check_and_execute_accumulated_positions: st0x_hedge::conductor: OffchainOrder::Place succeeded offchain_order_id=2e996ac6-e869-4daf-acd3-9f18b43f7f43 symbol=RKLB
2026-03-24T17:07:47.981049Z  WARN check_and_execute_accumulated_positions: st0x_hedge::conductor: Broker rejected order, clearing position pending state offchain_order_id=2e996ac6-e869-4daf-acd3-9f18b43f7f43 symbol=RKLB error=API error (403 Forbidden): {"code":40310000,"message":"no day trades permitted based on your previous day account equity being under $25,000"}
```

## Full Logs

```
2026-03-24T17:00:48.004273Z DEBUG st0x_hedge::inventory::polling: No Base wallet configured, skipping Base cash polling
2026-03-24T17:00:48.004281Z DEBUG st0x_hedge::inventory::polling: No Base wallet configured, skipping Base unwrapped equity polling
2026-03-24T17:00:48.004290Z DEBUG st0x_hedge::inventory::polling: No Base wallet configured, skipping Base wrapped equity polling
2026-03-24T17:00:48.004298Z DEBUG st0x_hedge::inventory::polling: No Alpaca wallet configured, skipping Alpaca wallet cash polling
2026-03-24T17:00:48.004315Z DEBUG st0x_execution::alpaca_broker_api::positions: Listing positions from https://broker-api.alpaca.markets/v1/trading/accounts/c0069a7b-7894-3ea4-b0a0-5d7c2dab0da7/positions
2026-03-24T17:00:48.019710Z DEBUG st0x_execution::alpaca_broker_api::positions: Fetching account details from https://broker-api.alpaca.markets/v1/trading/accounts/c0069a7b-7894-3ea4-b0a0-5d7c2dab0da7/account
2026-03-24T17:01:02.928586Z DEBUG poll_pending_orders: st0x_hedge::offchain::order_poller: Starting polling cycle for submitted orders
2026-03-24T17:01:02.929753Z DEBUG poll_pending_orders: st0x_hedge::offchain::order_poller: No submitted orders to poll
2026-03-24T17:01:17.929219Z DEBUG poll_pending_orders: st0x_hedge::offchain::order_poller: Starting polling cycle for submitted orders
2026-03-24T17:01:17.931052Z DEBUG poll_pending_orders: st0x_hedge::offchain::order_poller: No submitted orders to poll
2026-03-24T17:01:32.928548Z DEBUG poll_pending_orders: st0x_hedge::offchain::order_poller: Starting polling cycle for submitted orders
2026-03-24T17:01:32.929416Z DEBUG poll_pending_orders: st0x_hedge::offchain::order_poller: No submitted orders to poll
2026-03-24T17:01:47.928696Z DEBUG st0x_hedge::conductor: Running inventory poll
2026-03-24T17:01:47.929998Z DEBUG poll_pending_orders: st0x_hedge::offchain::order_poller: Starting polling cycle for submitted orders
2026-03-24T17:01:47.930155Z DEBUG st0x_hedge::conductor: Running periodic accumulated position check
2026-03-24T17:01:47.930890Z DEBUG check_and_execute_accumulated_positions:check_all_positions{assets=AssetsConfig { equities: EquitiesConfig { operational_limit: None, symbols: {Symbol("IAU"): EquityAssetConfig { tokenized_equity: 0x9a507314ea2a6c5686c0d07bfecb764dcf324dff, tokenized_
2026-03-24T17:01:47.930957Z DEBUG check_and_execute_accumulated_positions: st0x_hedge::conductor: No accumulated positions ready for execution
2026-03-24T17:01:47.931319Z DEBUG poll_pending_orders: st0x_hedge::offchain::order_poller: No submitted orders to poll
2026-03-24T17:01:48.013899Z DEBUG st0x_hedge::inventory::polling: No Ethereum wallet configured, skipping Ethereum cash polling
2026-03-24T17:01:48.013939Z DEBUG st0x_hedge::inventory::polling: No Base wallet configured, skipping Base cash polling
2026-03-24T17:01:48.013949Z DEBUG st0x_hedge::inventory::polling: No Base wallet configured, skipping Base unwrapped equity polling
2026-03-24T17:01:48.013959Z DEBUG st0x_hedge::inventory::polling: No Base wallet configured, skipping Base wrapped equity polling
2026-03-24T17:01:48.013969Z DEBUG st0x_hedge::inventory::polling: No Alpaca wallet configured, skipping Alpaca wallet cash polling
2026-03-24T17:01:48.013984Z DEBUG st0x_execution::alpaca_broker_api::positions: Listing positions from https://broker-api.alpaca.markets/v1/trading/accounts/c0069a7b-7894-3ea4-b0a0-5d7c2dab0da7/positions
2026-03-24T17:01:48.029237Z DEBUG st0x_execution::alpaca_broker_api::positions: Fetching account details from https://broker-api.alpaca.markets/v1/trading/accounts/c0069a7b-7894-3ea4-b0a0-5d7c2dab0da7/account
2026-03-24T17:02:02.929498Z DEBUG poll_pending_orders: st0x_hedge::offchain::order_poller: Starting polling cycle for submitted orders
2026-03-24T17:02:02.930798Z DEBUG poll_pending_orders: st0x_hedge::offchain::order_poller: No submitted orders to poll
2026-03-24T17:02:17.928421Z DEBUG poll_pending_orders: st0x_hedge::offchain::order_poller: Starting polling cycle for submitted orders
2026-03-24T17:02:17.929997Z DEBUG poll_pending_orders: st0x_hedge::offchain::order_poller: No submitted orders to poll
2026-03-24T17:02:32.928971Z DEBUG poll_pending_orders: st0x_hedge::offchain::order_poller: Starting polling cycle for submitted orders
2026-03-24T17:02:32.930093Z DEBUG poll_pending_orders: st0x_hedge::offchain::order_poller: No submitted orders to poll
2026-03-24T17:02:47.928774Z DEBUG poll_pending_orders: st0x_hedge::offchain::order_poller: Starting polling cycle for submitted orders
2026-03-24T17:02:47.928825Z DEBUG st0x_hedge::conductor: Running inventory poll
2026-03-24T17:02:47.930054Z DEBUG st0x_hedge::conductor: Running periodic accumulated position check
2026-03-24T17:02:47.930563Z DEBUG poll_pending_orders: st0x_hedge::offchain::order_poller: No submitted orders to poll
2026-03-24T17:02:47.931698Z DEBUG check_and_execute_accumulated_positions:check_all_positions{assets=AssetsConfig { equities: EquitiesConfig { operational_limit: None, symbols: {Symbol("IAU"): EquityAssetConfig { tokenized_equity: 0x9a507314ea2a6c5686c0d07bfecb764dcf324dff, tokenized_
2026-03-24T17:02:47.932312Z DEBUG check_and_execute_accumulated_positions: st0x_hedge::conductor: No accumulated positions ready for execution
2026-03-24T17:02:48.010274Z DEBUG st0x_hedge::inventory::polling: No Ethereum wallet configured, skipping Ethereum cash polling
2026-03-24T17:02:48.010311Z DEBUG st0x_hedge::inventory::polling: No Base wallet configured, skipping Base cash polling
2026-03-24T17:02:48.010319Z DEBUG st0x_hedge::inventory::polling: No Base wallet configured, skipping Base unwrapped equity polling
2026-03-24T17:02:48.010327Z DEBUG st0x_hedge::inventory::polling: No Base wallet configured, skipping Base wrapped equity polling
2026-03-24T17:02:48.010337Z DEBUG st0x_hedge::inventory::polling: No Alpaca wallet configured, skipping Alpaca wallet cash polling
2026-03-24T17:02:48.010366Z DEBUG st0x_execution::alpaca_broker_api::positions: Listing positions from https://broker-api.alpaca.markets/v1/trading/accounts/c0069a7b-7894-3ea4-b0a0-5d7c2dab0da7/positions
2026-03-24T17:02:48.024644Z DEBUG st0x_execution::alpaca_broker_api::positions: Fetching account details from https://broker-api.alpaca.markets/v1/trading/accounts/c0069a7b-7894-3ea4-b0a0-5d7c2dab0da7/account
2026-03-24T17:03:02.928842Z DEBUG poll_pending_orders: st0x_hedge::offchain::order_poller: Starting polling cycle for submitted orders
2026-03-24T17:03:02.931075Z DEBUG poll_pending_orders: st0x_hedge::offchain::order_poller: No submitted orders to poll
2026-03-24T17:03:17.928742Z DEBUG poll_pending_orders: st0x_hedge::offchain::order_poller: Starting polling cycle for submitted orders
2026-03-24T17:03:17.929875Z DEBUG poll_pending_orders: st0x_hedge::offchain::order_poller: No submitted orders to poll
2026-03-24T17:03:32.928355Z DEBUG poll_pending_orders: st0x_hedge::offchain::order_poller: Starting polling cycle for submitted orders
2026-03-24T17:03:32.930158Z DEBUG poll_pending_orders: st0x_hedge::offchain::order_poller: No submitted orders to poll
2026-03-24T17:03:47.928975Z DEBUG poll_pending_orders: st0x_hedge::offchain::order_poller: Starting polling cycle for submitted orders
2026-03-24T17:03:47.929048Z DEBUG st0x_hedge::conductor: Running inventory poll
2026-03-24T17:03:47.929240Z DEBUG st0x_hedge::conductor: Running periodic accumulated position check
2026-03-24T17:03:47.929979Z DEBUG check_and_execute_accumulated_positions:check_all_positions{assets=AssetsConfig { equities: EquitiesConfig { operational_limit: None, symbols: {Symbol("IAU"): EquityAssetConfig { tokenized_equity: 0x9a507314ea2a6c5686c0d07bfecb764dcf324dff, tokenized_
2026-03-24T17:03:47.930073Z DEBUG check_and_execute_accumulated_positions: st0x_hedge::conductor: No accumulated positions ready for execution
2026-03-24T17:03:47.930537Z DEBUG poll_pending_orders: st0x_hedge::offchain::order_poller: No submitted orders to poll
2026-03-24T17:03:48.153496Z DEBUG st0x_hedge::inventory::polling: No Ethereum wallet configured, skipping Ethereum cash polling
2026-03-24T17:03:48.153632Z DEBUG st0x_hedge::inventory::polling: No Base wallet configured, skipping Base cash polling
2026-03-24T17:03:48.153641Z DEBUG st0x_hedge::inventory::polling: No Base wallet configured, skipping Base unwrapped equity polling
2026-03-24T17:03:48.153648Z DEBUG st0x_hedge::inventory::polling: No Base wallet configured, skipping Base wrapped equity polling
2026-03-24T17:03:48.153654Z DEBUG st0x_hedge::inventory::polling: No Alpaca wallet configured, skipping Alpaca wallet cash polling
2026-03-24T17:03:48.153671Z DEBUG st0x_execution::alpaca_broker_api::positions: Listing positions from https://broker-api.alpaca.markets/v1/trading/accounts/c0069a7b-7894-3ea4-b0a0-5d7c2dab0da7/positions
2026-03-24T17:03:48.167873Z DEBUG st0x_execution::alpaca_broker_api::positions: Fetching account details from https://broker-api.alpaca.markets/v1/trading/accounts/c0069a7b-7894-3ea4-b0a0-5d7c2dab0da7/account
2026-03-24T17:04:02.928639Z DEBUG poll_pending_orders: st0x_hedge::offchain::order_poller: Starting polling cycle for submitted orders
2026-03-24T17:04:02.931474Z DEBUG poll_pending_orders: st0x_hedge::offchain::order_poller: No submitted orders to poll
2026-03-24T17:04:17.928829Z DEBUG poll_pending_orders: st0x_hedge::offchain::order_poller: Starting polling cycle for submitted orders
2026-03-24T17:04:17.930565Z DEBUG poll_pending_orders: st0x_hedge::offchain::order_poller: No submitted orders to poll
2026-03-24T17:04:32.928368Z DEBUG poll_pending_orders: st0x_hedge::offchain::order_poller: Starting polling cycle for submitted orders
2026-03-24T17:04:32.929312Z DEBUG poll_pending_orders: st0x_hedge::offchain::order_poller: No submitted orders to poll
2026-03-24T17:04:47.928335Z DEBUG poll_pending_orders: st0x_hedge::offchain::order_poller: Starting polling cycle for submitted orders
2026-03-24T17:04:47.928380Z DEBUG st0x_hedge::conductor: Running inventory poll
2026-03-24T17:04:47.929238Z DEBUG st0x_hedge::conductor: Running periodic accumulated position check
2026-03-24T17:04:47.929854Z DEBUG poll_pending_orders: st0x_hedge::offchain::order_poller: No submitted orders to poll
2026-03-24T17:04:47.930084Z DEBUG check_and_execute_accumulated_positions:check_all_positions{assets=AssetsConfig { equities: EquitiesConfig { operational_limit: None, symbols: {Symbol("IAU"): EquityAssetConfig { tokenized_equity: 0x9a507314ea2a6c5686c0d07bfecb764dcf324dff, tokenized_
2026-03-24T17:04:47.930136Z DEBUG check_and_execute_accumulated_positions: st0x_hedge::conductor: No accumulated positions ready for execution
2026-03-24T17:04:48.017496Z DEBUG st0x_hedge::inventory::polling: No Ethereum wallet configured, skipping Ethereum cash polling
2026-03-24T17:04:48.017539Z DEBUG st0x_hedge::inventory::polling: No Base wallet configured, skipping Base cash polling
2026-03-24T17:04:48.017547Z DEBUG st0x_hedge::inventory::polling: No Base wallet configured, skipping Base unwrapped equity polling
2026-03-24T17:04:48.017555Z DEBUG st0x_hedge::inventory::polling: No Base wallet configured, skipping Base wrapped equity polling
2026-03-24T17:04:48.017563Z DEBUG st0x_hedge::inventory::polling: No Alpaca wallet configured, skipping Alpaca wallet cash polling
2026-03-24T17:04:48.017587Z DEBUG st0x_execution::alpaca_broker_api::positions: Listing positions from https://broker-api.alpaca.markets/v1/trading/accounts/c0069a7b-7894-3ea4-b0a0-5d7c2dab0da7/positions
2026-03-24T17:04:48.031561Z DEBUG st0x_execution::alpaca_broker_api::positions: Fetching account details from https://broker-api.alpaca.markets/v1/trading/accounts/c0069a7b-7894-3ea4-b0a0-5d7c2dab0da7/account
2026-03-24T17:05:02.928604Z DEBUG poll_pending_orders: st0x_hedge::offchain::order_poller: Starting polling cycle for submitted orders
2026-03-24T17:05:02.930438Z DEBUG poll_pending_orders: st0x_hedge::offchain::order_poller: No submitted orders to poll
2026-03-24T17:05:17.928654Z DEBUG poll_pending_orders: st0x_hedge::offchain::order_poller: Starting polling cycle for submitted orders
2026-03-24T17:05:17.929995Z DEBUG poll_pending_orders: st0x_hedge::offchain::order_poller: No submitted orders to poll
2026-03-24T17:05:32.928750Z DEBUG poll_pending_orders: st0x_hedge::offchain::order_poller: Starting polling cycle for submitted orders
2026-03-24T17:05:32.930907Z DEBUG poll_pending_orders: st0x_hedge::offchain::order_poller: No submitted orders to poll
2026-03-24T17:05:47.929339Z DEBUG st0x_hedge::conductor: Running periodic accumulated position check
2026-03-24T17:05:47.929407Z DEBUG st0x_hedge::conductor: Running inventory poll
2026-03-24T17:05:47.929518Z DEBUG poll_pending_orders: st0x_hedge::offchain::order_poller: Starting polling cycle for submitted orders
2026-03-24T17:05:47.930651Z DEBUG check_and_execute_accumulated_positions:check_all_positions{assets=AssetsConfig { equities: EquitiesConfig { operational_limit: None, symbols: {Symbol("IAU"): EquityAssetConfig { tokenized_equity: 0x9a507314ea2a6c5686c0d07bfecb764dcf324dff, tokenized_
2026-03-24T17:05:47.930712Z DEBUG check_and_execute_accumulated_positions: st0x_hedge::conductor: No accumulated positions ready for execution
2026-03-24T17:05:47.930949Z DEBUG poll_pending_orders: st0x_hedge::offchain::order_poller: No submitted orders to poll
2026-03-24T17:05:48.155965Z DEBUG st0x_hedge::inventory::polling: No Ethereum wallet configured, skipping Ethereum cash polling
2026-03-24T17:05:48.156030Z DEBUG st0x_hedge::inventory::polling: No Base wallet configured, skipping Base cash polling
2026-03-24T17:05:48.156038Z DEBUG st0x_hedge::inventory::polling: No Base wallet configured, skipping Base unwrapped equity polling
2026-03-24T17:05:48.156045Z DEBUG st0x_hedge::inventory::polling: No Base wallet configured, skipping Base wrapped equity polling
2026-03-24T17:05:48.156055Z DEBUG st0x_hedge::inventory::polling: No Alpaca wallet configured, skipping Alpaca wallet cash polling
2026-03-24T17:05:48.156075Z DEBUG st0x_execution::alpaca_broker_api::positions: Listing positions from https://broker-api.alpaca.markets/v1/trading/accounts/c0069a7b-7894-3ea4-b0a0-5d7c2dab0da7/positions
2026-03-24T17:05:48.171073Z DEBUG st0x_execution::alpaca_broker_api::positions: Fetching account details from https://broker-api.alpaca.markets/v1/trading/accounts/c0069a7b-7894-3ea4-b0a0-5d7c2dab0da7/account
2026-03-24T17:06:02.928984Z DEBUG poll_pending_orders: st0x_hedge::offchain::order_poller: Starting polling cycle for submitted orders
2026-03-24T17:06:02.930547Z DEBUG poll_pending_orders: st0x_hedge::offchain::order_poller: No submitted orders to poll
2026-03-24T17:06:17.928688Z DEBUG poll_pending_orders: st0x_hedge::offchain::order_poller: Starting polling cycle for submitted orders
2026-03-24T17:06:17.930414Z DEBUG poll_pending_orders: st0x_hedge::offchain::order_poller: No submitted orders to poll
2026-03-24T17:06:32.929238Z DEBUG poll_pending_orders: st0x_hedge::offchain::order_poller: Starting polling cycle for submitted orders
2026-03-24T17:06:32.930302Z DEBUG poll_pending_orders: st0x_hedge::offchain::order_poller: No submitted orders to poll
2026-03-24T17:06:47.929055Z DEBUG poll_pending_orders: st0x_hedge::offchain::order_poller: Starting polling cycle for submitted orders
2026-03-24T17:06:47.929082Z DEBUG st0x_hedge::conductor: Running inventory poll
2026-03-24T17:06:47.930276Z DEBUG st0x_hedge::conductor: Running periodic accumulated position check
2026-03-24T17:06:47.931350Z DEBUG check_and_execute_accumulated_positions:check_all_positions{assets=AssetsConfig { equities: EquitiesConfig { operational_limit: None, symbols: {Symbol("IAU"): EquityAssetConfig { tokenized_equity: 0x9a507314ea2a6c5686c0d07bfecb764dcf324dff, tokenized_
2026-03-24T17:06:47.931426Z DEBUG check_and_execute_accumulated_positions: st0x_hedge::conductor: No accumulated positions ready for execution
2026-03-24T17:06:47.931900Z DEBUG poll_pending_orders: st0x_hedge::offchain::order_poller: No submitted orders to poll
2026-03-24T17:06:48.149354Z DEBUG st0x_hedge::inventory::polling: No Ethereum wallet configured, skipping Ethereum cash polling
2026-03-24T17:06:48.149390Z DEBUG st0x_hedge::inventory::polling: No Base wallet configured, skipping Base cash polling
2026-03-24T17:06:48.149395Z DEBUG st0x_hedge::inventory::polling: No Base wallet configured, skipping Base unwrapped equity polling
2026-03-24T17:06:48.149400Z DEBUG st0x_hedge::inventory::polling: No Base wallet configured, skipping Base wrapped equity polling
2026-03-24T17:06:48.149407Z DEBUG st0x_hedge::inventory::polling: No Alpaca wallet configured, skipping Alpaca wallet cash polling
2026-03-24T17:06:48.149421Z DEBUG st0x_execution::alpaca_broker_api::positions: Listing positions from https://broker-api.alpaca.markets/v1/trading/accounts/c0069a7b-7894-3ea4-b0a0-5d7c2dab0da7/positions
2026-03-24T17:06:48.164416Z DEBUG st0x_execution::alpaca_broker_api::positions: Fetching account details from https://broker-api.alpaca.markets/v1/trading/accounts/c0069a7b-7894-3ea4-b0a0-5d7c2dab0da7/account
2026-03-24T17:07:02.928819Z DEBUG poll_pending_orders: st0x_hedge::offchain::order_poller: Starting polling cycle for submitted orders
2026-03-24T17:07:02.930138Z DEBUG poll_pending_orders: st0x_hedge::offchain::order_poller: No submitted orders to poll
2026-03-24T17:07:17.929072Z DEBUG poll_pending_orders: st0x_hedge::offchain::order_poller: Starting polling cycle for submitted orders
2026-03-24T17:07:17.929836Z DEBUG poll_pending_orders: st0x_hedge::offchain::order_poller: No submitted orders to poll
2026-03-24T17:07:32.928866Z DEBUG poll_pending_orders: st0x_hedge::offchain::order_poller: Starting polling cycle for submitted orders
2026-03-24T17:07:32.929711Z DEBUG poll_pending_orders: st0x_hedge::offchain::order_poller: No submitted orders to poll
2026-03-24T17:07:35.557905Z  INFO st0x_hedge::conductor: Enqueuing TakeOrderV3 event: tx_hash=Some(0x039f24b270999f29728b8a8892a9e2dfa21421c2d250a74605d41e4a88d903c8), log_index=Some(580)
2026-03-24T17:07:35.639481Z DEBUG process_next_queued_event:convert_event_to_trade:try_from_take_order_if_target_owner{tx_hash=Some(0x039f24b270999f29728b8a8892a9e2dfa21421c2d250a74605d41e4a88d903c8) log_index=Some(580)}: st0x_hedge::onchain::pyth: Fetching trace for tx 0x039f24b27099
2026-03-24T17:07:35.726295Z DEBUG process_next_queued_event:convert_event_to_trade:try_from_take_order_if_target_owner{tx_hash=Some(0x039f24b270999f29728b8a8892a9e2dfa21421c2d250a74605d41e4a88d903c8) log_index=Some(580)}: st0x_hedge::onchain::pyth: Parsing trace for Pyth oracle calls
2026-03-24T17:07:35.726351Z DEBUG process_next_queued_event:convert_event_to_trade:try_from_take_order_if_target_owner{tx_hash=Some(0x039f24b270999f29728b8a8892a9e2dfa21421c2d250a74605d41e4a88d903c8) log_index=Some(580)}: st0x_hedge::onchain::pyth: Found 1 Pyth call(s) in trace
2026-03-24T17:07:35.726371Z DEBUG process_next_queued_event:convert_event_to_trade:try_from_take_order_if_target_owner{tx_hash=Some(0x039f24b270999f29728b8a8892a9e2dfa21421c2d250a74605d41e4a88d903c8) log_index=Some(580)}: st0x_hedge::onchain::pyth: Found cached feed ID for RKLB: 0x405
2026-03-24T17:07:35.726387Z DEBUG process_next_queued_event:convert_event_to_trade:try_from_take_order_if_target_owner{tx_hash=Some(0x039f24b270999f29728b8a8892a9e2dfa21421c2d250a74605d41e4a88d903c8) log_index=Some(580)}: st0x_hedge::onchain::pyth: Using Pyth call at depth 3 with feed
2026-03-24T17:07:35.726405Z  INFO process_next_queued_event:convert_event_to_trade:try_from_take_order_if_target_owner{tx_hash=Some(0x039f24b270999f29728b8a8892a9e2dfa21421c2d250a74605d41e4a88d903c8) log_index=Some(580)}: st0x_hedge::onchain::pyth: Extracted Pyth price for RKLB (feed
2026-03-24T17:07:35.726494Z  INFO process_next_queued_event: st0x_hedge::conductor: Event successfully converted to trade: event_type="TakeOrderV3", tx_hash=0x039f24b270999f29728b8a8892a9e2dfa21421c2d250a74605d41e4a88d903c8, log_index=580, symbol=wtRKLB, amount=0.996350331351928059
2026-03-24T17:07:35.750638Z  INFO process_next_queued_event: st0x_hedge::conductor: Successfully executed Position::AcknowledgeOnChainFill command: tx_hash=0x039f24b270999f29728b8a8892a9e2dfa21421c2d250a74605d41e4a88d903c8, log_index=580, symbol=wtRKLB
2026-03-24T17:07:35.753695Z  INFO process_next_queued_event: st0x_hedge::conductor: Successfully marked event as processed: event_id=457, tx_hash=0x039f24b270999f29728b8a8892a9e2dfa21421c2d250a74605d41e4a88d903c8, log_index=580
2026-03-24T17:07:35.757687Z  INFO process_next_queued_event: st0x_hedge::conductor: Successfully executed OnChainTrade::Witness command: tx_hash=0x039f24b270999f29728b8a8892a9e2dfa21421c2d250a74605d41e4a88d903c8, log_index=580
2026-03-24T17:07:35.761596Z  INFO process_next_queued_event: st0x_hedge::conductor: Successfully executed OnChainTrade::Enrich command: tx_hash=0x039f24b270999f29728b8a8892a9e2dfa21421c2d250a74605d41e4a88d903c8, log_index=580
2026-03-24T17:07:35.762582Z DEBUG process_next_queued_event: st0x_execution::alpaca_broker_api::market_hours: Fetching market calendar from https://broker-api.alpaca.markets/v1/calendar?start=2026-03-24&end=2026-03-24
2026-03-24T17:07:35.772948Z DEBUG process_next_queued_event: st0x_execution::alpaca_broker_api::market_hours: Checked market hours open=09:30:00 close=16:00:00 now=13:07:35.762568154 is_open=true
2026-03-24T17:07:35.773088Z  INFO process_next_queued_event: st0x_hedge::onchain::accumulator: Position ready for execution symbol=RKLB shares=0.996350331351928059 direction=Sell
2026-03-24T17:07:35.784990Z  INFO process_next_queued_event: st0x_hedge::conductor: Position::PlaceOffChainOrder succeeded offchain_order_id=6bf9d553-4dd2-4dbb-aced-23d06e3e321a symbol=RKLB
2026-03-24T17:07:35.785433Z DEBUG process_next_queued_event: st0x_execution::alpaca_broker_api::order: Placing Alpaca Broker API market order: SELL 0.996350331351928059 shares of RKLB (time_in_force: Day)
2026-03-24T17:07:35.785649Z DEBUG process_next_queued_event: st0x_execution::alpaca_broker_api::client: Placing order at https://broker-api.alpaca.markets/v1/trading/accounts/c0069a7b-7894-3ea4-b0a0-5d7c2dab0da7/orders: OrderRequest { symbol: Symbol("RKLB"), quantity: Positive(Fractio
2026-03-24T17:07:35.810231Z  INFO process_next_queued_event: st0x_hedge::conductor: OffchainOrder::Place succeeded offchain_order_id=6bf9d553-4dd2-4dbb-aced-23d06e3e321a symbol=RKLB
2026-03-24T17:07:35.810991Z  WARN process_next_queued_event: st0x_hedge::conductor: Broker rejected order, clearing position pending state offchain_order_id=6bf9d553-4dd2-4dbb-aced-23d06e3e321a symbol=RKLB error=API error (403 Forbidden): {"code":40310000,"message":"no day trades perm
2026-03-24T17:07:35.821300Z  INFO process_next_queued_event: st0x_hedge::conductor: Position::FailOffChainOrder succeeded offchain_order_id=6bf9d553-4dd2-4dbb-aced-23d06e3e321a symbol=RKLB
2026-03-24T17:07:35.821381Z  INFO st0x_hedge::conductor: Offchain order placed successfully offchain_order_id=6bf9d553-4dd2-4dbb-aced-23d06e3e321a
2026-03-24T17:07:47.929087Z DEBUG poll_pending_orders: st0x_hedge::offchain::order_poller: Starting polling cycle for submitted orders
2026-03-24T17:07:47.929141Z DEBUG st0x_hedge::conductor: Running inventory poll
2026-03-24T17:07:47.929203Z DEBUG st0x_hedge::conductor: Running periodic accumulated position check
2026-03-24T17:07:47.930743Z DEBUG poll_pending_orders: st0x_hedge::offchain::order_poller: No submitted orders to poll
2026-03-24T17:07:47.930841Z DEBUG check_and_execute_accumulated_positions:check_all_positions{assets=AssetsConfig { equities: EquitiesConfig { operational_limit: None, symbols: {Symbol("IAU"): EquityAssetConfig { tokenized_equity: 0x9a507314ea2a6c5686c0d07bfecb764dcf324dff, tokenized_zed_equity_derivative: 0x31c2c14134e6e3b7ef9478297f199331133fc2d8, vault_id: None, trading: Disabled, rebalancing: Disabled, operational_limit: None }} }, cash: Some(CashAssetConfig { vault_id: Some(0x0000000000000000000000000000000000000000000000000000000000000fab), rebalancing: Disabled, operational_limit: None }) } executor_type=alpaca-broker-api}: st0x_execution::alpaca_broker_api::market_hours: Fetching market calendar from https://broker-api.alpaca.markets/v1/calendar?start=2026-03-24&end=2026-03-24
2026-03-24T17:07:47.941047Z DEBUG check_and_execute_accumulated_positions:check_all_positions{assets=AssetsConfig { equities: EquitiesConfig { operational_limit: None, symbols: {Symbol("IAU"): EquityAssetConfig { tokenized_equity: 0x9a507314ea2a6c5686c0d07bfecb764dcf324dff, tokenized_equity_derivative: 0x1e46d7efef64a833afb1cd49299a7ad5b439f4d8, vault_id: None, trading: Disabled, rebalancing: Disabled, operational_limit: None }, Symbol("QSEP"): EquityAssetConfig { tokenized_equity: 0x4a9a9fc94a507559481270d0bff3315ab92fcefa, tokenized_equity_derivative: 0x849e389982209f901b7b9ffca57ae4c9eeb11c0d, vault_id: None, trading: Disabled, rebalancing: Disabled, operational_limit: None }, Symbol("MSTR"): EquityAssetConfig { tokenized_equity: 0x013b782f402d61aa1004cca95b9f5bb402c9d5fe, tokenized_equity_derivative: 0xff05e1bd696900dc6a52ca35ca61bb1024eda8e2, vault_id: None, trading: Disabled, rebalancing: Disabled, operational_limit: None }, Symbol("PPLT"): EquityAssetConfig { tokenized_equity: 0x1f17523b147ccc2a2328c0f014f6d49c479ea063, tokenized_equity_derivative: 0x82f5baee1076334357a34a19e04f7c282d51ce47, vault_id: None, trading: Disabled, rebalancing: Disabled, operational_limit: None }, Symbol("CRCL"): EquityAssetConfig { tokenized_equity: 0x38eb797892ed71da69bdc27a456a7c83ff813b52, tokenized_equity_derivative: 0x8afba81dec38de0a18e2df5e1967a7493651eebf, vault_id: None, trading: Disabled, rebalancing: Disabled, operational_limit: None }, Symbol("RKLB"): EquityAssetConfig { tokenized_equity: 0xf6744fd94e27c2f58f6110aa9fdc77a87e41766b, tokenized_equity_derivative: 0xf4f8c66085910d583c01f3b4e44bf731d4e2c565, vault_id: Some(0x0000000000000000000000000000000000000000000000000000000000000fab), trading: Enabled, rebalancing: Disabled, operational_limit: None }, Symbol("COIN"): EquityAssetConfig { tokenized_equity: 0x626757e6f50675d17fcad312e82f989ae7a23d38, tokenized_equity_derivative: 0x5cda0e1ca4ce2af96315f7f8963c85399c172204, vault_id: None, trading: Disabled, rebalancing: Disabled, operational_limit: None }, Symbol("AMZN"): EquityAssetConfig { tokenized_equity: 0x466cb2e46fa1afc0ab5e22274b34d0391db18efd, tokenized_equity_derivative: 0x997bae3ec193a249596d3708c3fab7c501bb8a53, vault_id: None, trading: Disabled, rebalancing: Disabled, operational_limit: None }, Symbol("SIVR"): EquityAssetConfig { tokenized_equity: 0x58ce5024b89b4f73c27814c0f0abbea331c99be8, tokenized_equity_derivative: 0xeb7f3e4093c9d68253b6104fbbff561f3ec0442f, vault_id: None, trading: Disabled, rebalancing: Disabled, operational_limit: None }, Symbol("TSLA"): EquityAssetConfig { tokenized_equity: 0x4e169cd2ab4f82640a8c65c68fed55863866fdb0, tokenized_equity_derivative: 0x219a8d384a10bf19b9f24cb5cc53f79dd0e5a03d, vault_id: None, trading: Disabled, rebalancing: Disabled, operational_limit: None }, Symbol("BMNR"): EquityAssetConfig { tokenized_equity: 0xfbde45df60249203b12148452fc77c3b5f811eb2, tokenized_equity_derivative: 0x02512ec661f0ba89c275ea105e31bad6fcfcf319, vault_id: None, trading: Disabled, rebalancing: Disabled, operational_limit: None }, Symbol("NVDA"): EquityAssetConfig { tokenized_equity: 0x7271a3c91bb6070ed09333b84a815949d4f16d14, tokenized_equity_derivative: 0xfb5b41acdba20a3230f84be995173cfb98b8d6e7, vault_id: None, trading: Disabled, rebalancing: Disabled, operational_limit: None }, Symbol("SPYM"): EquityAssetConfig { tokenized_equity: 0x8fdf41116f755771bfe0747d5f8c3711d5debfbb, tokenized_equity_derivative: 0x31c2c14134e6e3b7ef9478297f199331133fc2d8, vault_id: None, trading: Disabled, rebalancing: Disabled, operational_limit: None }} }, cash: Some(CashAssetConfig { vault_id: Some(0x0000000000000000000000000000000000000000000000000000000000000fab), rebalancing: Disabled, operational_limit: None }) } executor_type=alpaca-broker-api}: st0x_execution::alpaca_broker_api::market_hours: Checked market hours open=09:30:00 close=16:00:00 now=13:07:47.930827395 is_open=true
2026-03-24T17:07:47.941168Z  INFO check_and_execute_accumulated_positions:check_all_positions{assets=AssetsConfig { equities: EquitiesConfig { operational_limit: None, symbols: {Symbol("IAU"): EquityAssetConfig { tokenized_equity: 0x9a507314ea2a6c5686c0d07bfecb764dcf324dff, tokenized_equity_derivative: 0x1e46d7efef64a833afb1cd49299a7ad5b439f4d8, vault_id: None, trading: Disabled, rebalancing: Disabled, operational_limit: None }, Symbol("QSEP"): EquityAssetConfig { tokenized_equity: 0x4a9a9fc94a507559481270d0bff3315ab92fcefa, tokenized_equity_derivative: 0x849e389982209f901b7b9ffca57ae4c9eeb11c0d, vault_id: None, trading: Disabled, rebalancing: Disabled, operational_limit: None }, Symbol("MSTR"): EquityAssetConfig { tokenized_equity: 0x013b782f402d61aa1004cca95b9f5bb402c9d5fe, tokenized_equity_derivative: 0xff05e1bd696900dc6a52ca35ca61bb1024eda8e2, vault_id: None, trading: Disabled, rebalancing: Disabled, operational_limit: None }, Symbol("PPLT"): EquityAssetConfig { tokenized_equity: 0x1f17523b147ccc2a2328c0f014f6d49c479ea063, tokenized_equity_derivative: 0x82f5baee1076334357a34a19e04f7c282d51ce47, vault_id: None, trading: Disabled, rebalancing: Disabled, operational_limit: None }, Symbol("CRCL"): EquityAssetConfig { tokenized_equity: 0x38eb797892ed71da69bdc27a456a7c83ff813b52, tokenized_equity_derivative: 0x8afba81dec38de0a18e2df5e1967a7493651eebf, vault_id: None, trading: Disabled, rebalancing: Disabled, operational_limit: None }, Symbol("RKLB"): EquityAssetConfig { tokenized_equity: 0xf6744fd94e27c2f58f6110aa9fdc77a87e41766b, tokenized_equity_derivative: 0xf4f8c66085910d583c01f3b4e44bf731d4e2c565, vault_id: Some(0x0000000000000000000000000000000000000000000000000000000000000fab), trading: Enabled, rebalancing: Disabled, operational_limit: None }, Symbol("COIN"): EquityAssetConfig { tokenized_equity: 0x626757e6f50675d17fcad312e82f989ae7a23d38, tokenized_equity_derivative: 0x5cda0e1ca4ce2af96315f7f8963c85399c172204, vault_id: None, trading: Disabled, rebalancing: Disabled, operational_limit: None }, Symbol("AMZN"): EquityAssetConfig { tokenized_equity: 0x466cb2e46fa1afc0ab5e22274b34d0391db18efd, tokenized_equity_derivative: 0x997bae3ec193a249596d3708c3fab7c501bb8a53, vault_id: None, trading: Disabled, rebalancing: Disabled, operational_limit: None }, Symbol("SIVR"): EquityAssetConfig { tokenized_equity: 0x58ce5024b89b4f73c27814c0f0abbea331c99be8, tokenized_equity_derivative: 0xeb7f3e4093c9d68253b6104fbbff561f3ec0442f, vault_id: None, trading: Disabled, rebalancing: Disabled, operational_limit: None }, Symbol("TSLA"): EquityAssetConfig { tokenized_equity: 0x4e169cd2ab4f82640a8c65c68fed55863866fdb0, tokenized_equity_derivative: 0x219a8d384a10bf19b9f24cb5cc53f79dd0e5a03d, vault_id: None, trading: Disabled, rebalancing: Disabled, operational_limit: None }, Symbol("BMNR"): EquityAssetConfig { tokenized_equity: 0xfbde45df60249203b12148452fc77c3b5f811eb2, tokenized_equity_derivative: 0x02512ec661f0ba89c275ea105e31bad6fcfcf319, vault_id: None, trading: Disabled, rebalancing: Disabled, operational_limit: None }, Symbol("NVDA"): EquityAssetConfig { tokenized_equity: 0x7271a3c91bb6070ed09333b84a815949d4f16d14, tokenized_equity_derivative: 0xfb5b41acdba20a3230f84be995173cfb98b8d6e7, vault_id: None, trading: Disabled, rebalancing: Disabled, operational_limit: None }, Symbol("SPYM"): EquityAssetConfig { tokenized_equity: 0x8fdf41116f755771bfe0747d5f8c3711d5debfbb, tokenized_equity_derivative: 0x31c2c14134e6e3b7ef9478297f199331133fc2d8, vault_id: None, trading: Disabled, rebalancing: Disabled, operational_limit: None }} }, cash: Some(CashAssetConfig { vault_id: Some(0x0000000000000000000000000000000000000000000000000000000000000fab), rebalancing: Disabled, operational_limit: None }) } executor_type=alpaca-broker-api}: st0x_hedge::onchain::accumulator: Position ready for execution symbol=RKLB shares=0.996350331351928059 direction=Sell
2026-03-24T17:07:47.941252Z  INFO check_and_execute_accumulated_positions:check_all_positions{assets=AssetsConfig { equities: EquitiesConfig { operational_limit: None, symbols: {Symbol("IAU"): EquityAssetConfig { tokenized_equity: 0x9a507314ea2a6c5686c0d07bfecb764dcf324dff, tokenized_equity_derivative: 0x1e46d7efef64a833afb1cd49299a7ad5b439f4d8, vault_id: None, trading: Disabled, rebalancing: Disabled, operational_limit: None }, Symbol("QSEP"): EquityAssetConfig { tokenized_equity: 0x4a9a9fc94a507559481270d0bff3315ab92fcefa, tokenized_equity_derivative: 0x849e389982209f901b7b9ffca57ae4c9eeb11c0d, vault_id: None, trading: Disabled, rebalancing: Disabled, operational_limit: None }, Symbol("MSTR"): EquityAssetConfig { tokenized_equity: 0x013b782f402d61aa1004cca95b9f5bb402c9d5fe, tokenized_equity_derivative: 0xff05e1bd696900dc6a52ca35ca61bb1024eda8e2, vault_id: None, trading: Disabled, rebalancing: Disabled, operational_limit: None }, Symbol("PPLT"): EquityAssetConfig { tokenized_equity: 0x1f17523b147ccc2a2328c0f014f6d49c479ea063, tokenized_equity_derivative: 0x82f5baee1076334357a34a19e04f7c282d51ce47, vault_id: None, trading: Disabled, rebalancing: Disabled, operational_limit: None }, Symbol("CRCL"): EquityAssetConfig { tokenized_equity: 0x38eb797892ed71da69bdc27a456a7c83ff813b52, tokenized_equity_derivative: 0x8afba81dec38de0a18e2df5e1967a7493651eebf, vault_id: None, trading: Disabled, rebalancing: Disabled, operational_limit: None }, Symbol("RKLB"): EquityAssetConfig { tokenized_equity: 0xf6744fd94e27c2f58f6110aa9fdc77a87e41766b, tokenized_equity_derivative: 0xf4f8c66085910d583c01f3b4e44bf731d4e2c565, vault_id: Some(0x0000000000000000000000000000000000000000000000000000000000000fab), trading: Enabled, rebalancing: Disabled, operational_limit: None }, Symbol("COIN"): EquityAssetConfig { tokenized_equity: 0x626757e6f50675d17fcad312e82f989ae7a23d38, tokenized_equity_derivative: 0x5cda0e1ca4ce2af96315f7f8963c85399c172204, vault_id: None, trading: Disabled, rebalancing: Disabled, operational_limit: None }, Symbol("AMZN"): EquityAssetConfig { tokenized_equity: 0x466cb2e46fa1afc0ab5e22274b34d0391db18efd, tokenized_equity_derivative: 0x997bae3ec193a249596d3708c3fab7c501bb8a53, vault_id: None, trading: Disabled, rebalancing: Disabled, operational_limit: None }, Symbol("SIVR"): EquityAssetConfig { tokenized_equity: 0x58ce5024b89b4f73c27814c0f0abbea331c99be8, tokenized_equity_derivative: 0xeb7f3e4093c9d68253b6104fbbff561f3ec0442f, vault_id: None, trading: Disabled, rebalancing: Disabled, operational_limit: None }, Symbol("TSLA"): EquityAssetConfig { tokenized_equity: 0x4e169cd2ab4f82640a8c65c68fed55863866fdb0, tokenized_equity_derivative: 0x219a8d384a10bf19b9f24cb5cc53f79dd0e5a03d, vault_id: None, trading: Disabled, rebalancing: Disabled, operational_limit: None }, Symbol("BMNR"): EquityAssetConfig { tokenized_equity: 0xfbde45df60249203b12148452fc77c3b5f811eb2, tokenized_equity_derivative: 0x02512ec661f0ba89c275ea105e31bad6fcfcf319, vault_id: None, trading: Disabled, rebalancing: Disabled, operational_limit: None }, Symbol("NVDA"): EquityAssetConfig { tokenized_equity: 0x7271a3c91bb6070ed09333b84a815949d4f16d14, tokenized_equity_derivative: 0xfb5b41acdba20a3230f84be995173cfb98b8d6e7, vault_id: None, trading: Disabled, rebalancing: Disabled, operational_limit: None }, Symbol("SPYM"): EquityAssetConfig { tokenized_equity: 0x8fdf41116f755771bfe0747d5f8c3711d5debfbb, tokenized_equity_derivative: 0x31c2c14134e6e3b7ef9478297f199331133fc2d8, vault_id: None, trading: Disabled, rebalancing: Disabled, operational_limit: None }} }, cash: Some(CashAssetConfig { vault_id: Some(0x0000000000000000000000000000000000000000000000000000000000000fab), rebalancing: Disabled, operational_limit: None }) } executor_type=alpaca-broker-api}: st0x_hedge::onchain::accumulator: Found 1 positions ready for execution
2026-03-24T17:07:47.941293Z  INFO check_and_execute_accumulated_positions: st0x_hedge::conductor: Found 1 accumulated positions ready for execution
2026-03-24T17:07:47.941308Z  INFO check_and_execute_accumulated_positions: st0x_hedge::conductor: Executing accumulated position symbol=RKLB shares=0.996350331351928059 direction=Sell offchain_order_id=2e996ac6-e869-4daf-acd3-9f18b43f7f43
2026-03-24T17:07:47.951521Z  INFO check_and_execute_accumulated_positions: st0x_hedge::conductor: Position::PlaceOffChainOrder succeeded offchain_order_id=2e996ac6-e869-4daf-acd3-9f18b43f7f43 symbol=RKLB
2026-03-24T17:07:47.952030Z DEBUG check_and_execute_accumulated_positions: st0x_execution::alpaca_broker_api::order: Placing Alpaca Broker API market order: SELL 0.996350331351928059 shares of RKLB (time_in_force: Day)
2026-03-24T17:07:47.952360Z DEBUG check_and_execute_accumulated_positions: st0x_execution::alpaca_broker_api::client: Placing order at https://broker-api.alpaca.markets/v1/trading/accounts/c0069a7b-7894-3ea4-b0a0-5d7c2dab0da7/orders: OrderRequest { symbol: Symbol("RKLB"), quantity: Positive(FractionalShares(Float(0xffffffee00000000000000000000000000000000000000000dd3bf58f4130cfb))), side: Sell, order_type: "market", time_in_force: "day", extended_hours: false }
2026-03-24T17:07:47.980280Z  INFO check_and_execute_accumulated_positions: st0x_hedge::conductor: OffchainOrder::Place succeeded offchain_order_id=2e996ac6-e869-4daf-acd3-9f18b43f7f43 symbol=RKLB
2026-03-24T17:07:47.981049Z  WARN check_and_execute_accumulated_positions: st0x_hedge::conductor: Broker rejected order, clearing position pending state offchain_order_id=2e996ac6-e869-4daf-acd3-9f18b43f7f43 symbol=RKLB error=API error (403 Forbidden): {"code":40310000,"message":"no day trades permitted based on your previous day account equity being under $25,000"}
2026-03-24T17:07:47.989734Z  INFO check_and_execute_accumulated_positions: st0x_hedge::conductor: Position::FailOffChainOrder succeeded offchain_order_id=2e996ac6-e869-4daf-acd3-9f18b43f7f43 symbol=RKLB
2026-03-24T17:07:48.009050Z DEBUG st0x_hedge::inventory::polling: No Ethereum wallet configured, skipping Ethereum cash polling
2026-03-24T17:07:48.009330Z DEBUG st0x_hedge::inventory::polling: No Base wallet configured, skipping Base cash polling
2026-03-24T17:07:48.009348Z DEBUG st0x_hedge::inventory::polling: No Base wallet configured, skipping Base unwrapped equity polling
2026-03-24T17:07:48.009354Z DEBUG st0x_hedge::inventory::polling: No Base wallet configured, skipping Base wrapped equity polling
2026-03-24T17:07:48.009359Z DEBUG st0x_hedge::inventory::polling: No Alpaca wallet configured, skipping Alpaca wallet cash polling
2026-03-24T17:07:48.009380Z DEBUG st0x_execution::alpaca_broker_api::positions: Listing positions from https://broker-api.alpaca.markets/v1/trading/accounts/c0069a7b-7894-3ea4-b0a0-5d7c2dab0da7/positions
2026-03-24T17:07:48.022768Z DEBUG st0x_execution::alpaca_broker_api::positions: Fetching account details from https://broker-api.alpaca.markets/v1/trading/accounts/c0069a7b-7894-3ea4-b0a0-5d7c2dab0da7/account
2026-03-24T17:07:49.581678Z  INFO st0x_hedge::conductor: Enqueuing TakeOrderV3 event: tx_hash=Some(0x20329208da25bbece9e1247e060a3c46e8c018746c0dcb00566584bc6450de63), log_index=Some(363)
2026-03-24T17:07:49.639682Z  INFO process_next_queued_event: st0x_hedge::conductor: Event filtered out (no matching owner): event_type="TakeOrderV3", tx_hash=0x20329208da25bbece9e1247e060a3c46e8c018746c0dcb00566584bc6450de63, log_index=363
2026-03-24T17:08:02.928517Z DEBUG poll_pending_orders: st0x_hedge::offchain::order_poller: Starting polling cycle for submitted orders
2026-03-24T17:08:02.929781Z DEBUG poll_pending_orders: st0x_hedge::offchain::order_poller: No submitted orders to poll
                                                                                                                                     2026-03-24T17:08:17.928728Z DEBUG poll_pending_orders: st0x_hedge::offchain::order_poller: Starting polling cycle for submitted orders
2026-03-24T17:08:17.929426Z DEBUG poll_pending_orders: st0x_hedge::offchain::order_poller: No submitted orders to poll
Warning: Received SIGTERM. Requesting shutdown.
Stopping st0x server (st0x-hedge)...
2026-03-24T17:08:20.643094Z  INFO launch: st0x_hedge: Server completed successfully
2026-03-24T17:08:20.643158Z  INFO launch: st0x_hedge: Aborting bot task
2026-03-24T17:08:20.643183Z  INFO launch: st0x_hedge: Shutdown complete
st0x-hedge.service: Deactivated successfully.
Stopped st0x server (st0x-hedge).
st0x-hedge.service: Consumed 19.385s CPU time over 40min 38.416s wall clock time, 21.8M memory peak, 624K written to disk, 736.5K incoming IP traffic, 14.6M outgoing IP traffic.
```
