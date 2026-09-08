"""Keep deployed settings within the shared 15-minute quoting/hedging budget."""

import sys
import tomllib
from pathlib import Path


def validate_config(config):
    flatten = config.get("broker", {}).get("extended_hours_close_flatten_window_secs")
    if type(flatten) is not int or not 0 < flatten <= 900:
        raise ValueError(
            "broker.extended_hours_close_flatten_window_secs must be an integer in 1..900 seconds"
        )


if __name__ == "__main__":
    root = Path(__file__).resolve().parents[1]
    candidates = [Path(argument) for argument in sys.argv[1:]] or [
        root / "config/staging/st0x-hedge.toml",
        root / "config/prod/st0x-hedge.toml",
    ]
    for candidate in candidates:
        with candidate.open("rb") as source:
            validate_config(tomllib.load(source))
        print(f"{candidate}: close-window budget OK")

