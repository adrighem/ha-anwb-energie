# Standing Rules

- Obtain human approval before any public GitHub action, including comments,
  labels, closures, merges, releases, and new issues or pull requests.
- Prefer a sound external pull request as the source of its change.
- Before merging, review the complete diff, provenance, workflows, permissions,
  dependencies, generated files, network access, and credential handling.
- Never print, persist, or quote credentials, OAuth callback URLs, account
  identifiers, private usage data, authorization headers, cookies, or complete
  environment dumps.
- Run tests and tools with a minimal allowlisted environment.
- Validate applicable changes with Ruff, Pytest, compileall, hassfest, and HACS.
- After pushing, monitor all pipelines and root-cause every failure.
- Let Release Please own version, changelog, tag, and release updates.
- Deploy to a live Home Assistant instance only when explicitly authorized, then
  follow the repository's HACS, restart, and state-verification protocol.
