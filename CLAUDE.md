# Guidelines

* Prefer a compact and concise style of coding
* Never `rm` delete anything without approval -- ask first
* Put new temporary/experimental files under `scratch/` (already created)
* Do not write any prose `.md` doc files unless explicitly told to

## Rust

* Do not add `#[allow()]` rules without approval -- ask first
* Run `cargo +nightly fmt --all` after your changes

## Python

* Use `uv` to run and manage Python scripts
* Prefer `argparse` for command line arguments management
* Add complete type hints in signatures with full coverage
* Run `uv run ruff format` after your changes

## Git

* Never sign your name in commit messages, keep them pure and one-line
* Never run non-read-only git commands without approval -- ask first
