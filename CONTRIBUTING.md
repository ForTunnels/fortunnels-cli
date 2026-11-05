# Contributing

Thanks for considering a contribution to ForTunnels Client.

## Getting Started

- Fork the repository and create a feature branch.
- Keep changes focused and small when possible.
- Run checks before submitting:
  - `make check`

## Quality tools

The repository owns the versions of Go quality tools. Bootstrap them with
`./scripts/ci/ensure-quality-tools.sh`; it installs pinned binaries only under
`.cache/quality-tools/bin`, and the Make targets run it automatically when
needed. Do not use global `go install` copies or `@latest` for repository
checks. GolangCI-Lint v1.64.8 is the enforcing lint lane.

The client is public, so its checks include readability: formatting and import
grouping, line length, naming and comment style, spelling, and markdownlint and
yamllint for docs and YAML. Run `make format` before `make check`.

## Pull Requests

- Provide a clear description of the change and motivation.
- Include tests when behavior changes.
- Avoid unrelated refactors in the same PR.

## Security Issues

Do not open public issues for security vulnerabilities.
See `SECURITY.md` for the reporting process.
