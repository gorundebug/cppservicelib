"""Exercise process termination without killing the parent test runner."""

import subprocess
import sys
import time


def main():
    binary = sys.argv[1]
    cases = {
        "unarmed": 7,
        "clean": 7,
        "blocked-handler": 0,
        "startup-failure": 1,
        "startup-failure-after-stop": 1,
        "rearm-later": 0,
        "rearm-earlier": 0,
        "destructor": 0,
        "expired-before-release": 0,
    }
    for scenario, expected in cases.items():
        started = time.monotonic()
        result = subprocess.run(
            [binary, scenario], capture_output=True, text=True, timeout=5
        )
        elapsed = time.monotonic() - started
        if result.returncode != expected or elapsed >= 1.5:
            raise AssertionError(
                f"{scenario}: exit={result.returncode}, elapsed={elapsed:.3f}s, "
                f"stdout={result.stdout!r}, stderr={result.stderr!r}"
            )
        assert "probe entered" in result.stdout, scenario
        released = "function released" in result.stdout
        assert released == (scenario in ("unarmed", "clean")), scenario
        if scenario == "destructor":
            assert "destructor entered" in result.stdout, scenario
        print(f"PASS {scenario}: exit={result.returncode}, {elapsed:.3f}s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
