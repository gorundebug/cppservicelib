"""Verify userver's fatal non-standard exception boundary in child processes."""

import os
import resource
import signal
import subprocess
import sys


def no_core_dump():
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))


def main():
    taskpool, pools, operators = sys.argv[1:]
    cases = (
        (taskpool, "TaskPool.UnhandledFailureTerminatesProcess"),
        (pools, "PriorityTaskPool.UnhandledFailureTerminatesProcess"),
        (pools, "DelayPool.UnhandledFailureTerminatesProcess"),
        (operators, "GraphParityReview.UnhandledFailureTerminatesProcess"),
    )
    for binary, test in cases:
        result = subprocess.run(
            [binary, "--gtest_filter=" + test],
            env={**os.environ, "SERVICELIB_FATAL_CALLBACK_CHILD": "nonstd"},
            capture_output=True, text=True, timeout=15,
            preexec_fn=no_core_dump,
        )
        assert result.returncode == -signal.SIGABRT, (
            test, result.returncode, result.stdout, result.stderr
        )
        assert "not derived from std::exception" in result.stderr, result.stderr
        assert "Dynamic exception type: int" in result.stderr, result.stderr
        print(f"PASS {test}: unhandled int terminates via SIGABRT")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
