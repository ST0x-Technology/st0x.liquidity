# Single source of truth for all st0x services
{
  server-schwab = {
    enabled = false;
    bin = "server";
  };
  server-alpaca = {
    enabled = true;
    bin = "server";
  };
  reporter-schwab = {
    enabled = false;
    bin = "reporter";
  };
  reporter-alpaca = {
    enabled = false;
    bin = "reporter";
  };
}
