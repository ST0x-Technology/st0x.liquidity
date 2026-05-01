/// Mock st0x REST API for simulate mode.
/// Returns fake Raindex order data so the Orders dashboard tab can be tested.

const MOCK_ORDERS = {
  orders: [
    {
      orderHash: "0xc24100a122c701de0627f745799829fdfc1cb85cab49d33dd06eb4be46852f94",
      owner: "0xcccccccccccccccccccccccccccccccccccccccc",
      inputToken: { address: "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913", symbol: "USDC", decimals: 6 },
      outputToken: { address: "0xFF05E1bD696900dc6A52CA35Ca61Bb1024eDa8e2", symbol: "wtMSTR", decimals: 18 },
      outputVaultBalance: "500000000000000000000",
      ioRatio: "0.0027",
      createdAt: 1718452800,
      orderbookId: "0xe522cB4a5fCb2eb31a52Ff41a4653d85A4fd7C9D",
    },
    {
      orderHash: "0xba760b0150cd36aad23755b230a47cc2a44975c83d1dc9f82645248fe1a32678",
      owner: "0xcccccccccccccccccccccccccccccccccccccccc",
      inputToken: { address: "0xFF05E1bD696900dc6A52CA35Ca61Bb1024eDa8e2", symbol: "wtMSTR", decimals: 18 },
      outputToken: { address: "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913", symbol: "USDC", decimals: 6 },
      outputVaultBalance: "15000000000",
      ioRatio: "370.50",
      createdAt: 1718452800,
      orderbookId: "0xe522cB4a5fCb2eb31a52Ff41a4653d85A4fd7C9D",
    },
    {
      orderHash: "0x97e83c4fbd18e6dd917385aabe5d051226dc62643bd689b6995668f124728296",
      owner: "0xcccccccccccccccccccccccccccccccccccccccc",
      inputToken: { address: "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913", symbol: "USDC", decimals: 6 },
      outputToken: { address: "0xFb5B41acdbA20a3230F84BE995173CFb98b8D6E7", symbol: "wtNVDA", decimals: 18 },
      outputVaultBalance: "3200000000000000000",
      ioRatio: "0.0075",
      createdAt: 1718539200,
      orderbookId: "0xe522cB4a5fCb2eb31a52Ff41a4653d85A4fd7C9D",
    },
    {
      orderHash: "0x572302e9377b43d44e897f1ba848066522952cea0a863017a9f5d6ce715b8e2d",
      owner: "0xcccccccccccccccccccccccccccccccccccccccc",
      inputToken: { address: "0xFb5B41acdbA20a3230F84BE995173CFb98b8D6E7", symbol: "wtNVDA", decimals: 18 },
      outputToken: { address: "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913", symbol: "USDC", decimals: 6 },
      outputVaultBalance: "8500000000",
      ioRatio: "133.20",
      createdAt: 1718539200,
      orderbookId: "0xe522cB4a5fCb2eb31a52Ff41a4653d85A4fd7C9D",
    },
    {
      orderHash: "0xabd966a604c6e67543cd27f79cb789e4cd057c02a779e496dace404a34a3c61b",
      owner: "0xcccccccccccccccccccccccccccccccccccccccc",
      inputToken: { address: "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913", symbol: "USDC", decimals: 6 },
      outputToken: { address: "0x997baE3EC193a249596d3708C3fAB7C501Bb8a53", symbol: "wtAMZN", decimals: 18 },
      outputVaultBalance: "1800000000000000000",
      ioRatio: "0.0050",
      createdAt: 1718625600,
      orderbookId: "0xe522cB4a5fCb2eb31a52Ff41a4653d85A4fd7C9D",
    },
    {
      orderHash: "0x54475d13b2783ba092b38dca9b09a4c96ade1d10aa94019c290aec7a3ca7376a",
      owner: "0xcccccccccccccccccccccccccccccccccccccccc",
      inputToken: { address: "0x997baE3EC193a249596d3708C3fAB7C501Bb8a53", symbol: "wtAMZN", decimals: 18 },
      outputToken: { address: "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913", symbol: "USDC", decimals: 6 },
      outputVaultBalance: "5200000000",
      ioRatio: "198.75",
      createdAt: 1718625600,
      orderbookId: "0xe522cB4a5fCb2eb31a52Ff41a4653d85A4fd7C9D",
    },
  ],
  pagination: {
    page: 1,
    pageSize: 20,
    totalOrders: 6,
    totalPages: 1,
    hasMore: false,
  },
};

const server = Bun.serve({
  port: 8099,
  fetch(request: Request) {
    const url = new URL(request.url);

    if (url.pathname.startsWith("/v1/orders/owner/")) {
      return Response.json(MOCK_ORDERS);
    }

    return new Response("Not found", { status: 404 });
  },
});

console.log(`Mock REST API listening on http://localhost:${server.port}`);
