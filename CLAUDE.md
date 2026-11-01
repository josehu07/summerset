# Guidelines

* Prefer a compact and concise style of coding; no excessive docs
* Never permanently delete anything without approval -- ask first
* Put temporary/experimental files under `scratch/`, organized by subfolders
* Do not write any prose `.md` doc files unless explicitly requested

## Rust

* Do not add `#[allow()]` rules without approval -- ask first
* Format and lint the codebase before presenting changes

## Python

* Use `uv` to manage and run any Python scripts
* Use `argparse` for command line arguments management
* Add complete type hints in signatures with full coverage
* Format the scripts with `uv run ruff` before presenting changes

## Git

* Never sign your name in commit messages, keep them pure and one-line
* Never run non-read-only git commands without approval -- ask first
