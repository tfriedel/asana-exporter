#!/usr/bin/env python3
"""Update script for asana-exporter: installs CLI tool."""

import subprocess
import sys


def run_update() -> int:
    """Run the update workflow.

    Returns:
        int: Exit code (0 for success, non-zero for failure).
    """
    print("📦 Installing asana-exporter tool...")
    result = subprocess.run(
        ["uv", "tool", "install", ".", "--reinstall", "--force"],
        capture_output=False,
    )
    if result.returncode != 0:
        print("❌ Failed to install tool")
        return 1

    print("✅ Update complete!")
    return 0


if __name__ == "__main__":
    sys.exit(run_update())
