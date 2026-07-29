# Maintainer Backlog

## Current

1. Fix the diagnostics runtime-data mismatch and add account-data redaction
   coverage. Status: complete locally.
2. Remove credential-bearing OAuth callback URLs from probe command arguments,
   sanitize upstream errors, and require callback state validation. Status:
   complete locally.
3. Bind the integration's manual OAuth callback to generated state and the
   expected callback endpoint. Status: complete locally.
4. Answer ISSUE:17, add the `question` label, and close it as answered. Status:
   awaiting approval.

## Next

5. Add a default-branch ruleset that requires current CI checks while preserving
   a deliberate solo-maintainer and Release Please bypass path.
6. Pin reviewed GitHub Actions to immutable commit SHAs and automate action
   updates.
7. Add a minimum-supported Home Assistant test job for setup, unload, OAuth,
   registry behavior, and diagnostics.
8. Narrow pricing cache fallback to known transient failures and define an
   explicit freshness limit.
9. Add removal and retained-statistics guidance, or correct the claimed
   `docs-removal-instructions` quality-scale status.
