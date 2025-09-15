#!/usr/bin/env bash

set -euo pipefail
shopt -s inherit_errexit

src/dev set-up-environment
src/dev bandit
src/dev black --check
src/dev test