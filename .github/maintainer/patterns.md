# Maintenance Patterns

- The ANWB API is undocumented and partly inferred. Record observations and
  label hypotheses clearly.
- Timezone, DST, and local calendar boundaries are recurring correctness risks.
- Cache fallbacks must balance availability against stale-period data.
- Incomplete usage or tariff coverage should produce an unavailable estimate,
  not a plausible but incorrect value.
- Entity migrations must preserve user-managed compatibility aliases.
- Conventional commits feed Release Please and should describe user-visible
  fixes or features accurately.
- Mobile bundle property names can confirm that an API field reaches the
  official client, but runtime observations are stronger evidence of semantics.
