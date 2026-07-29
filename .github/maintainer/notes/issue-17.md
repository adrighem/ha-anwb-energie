# ISSUE:17 - HasGap semantics

Status: Open

Type: User question

Actionability: High

Confidence: High on observed behavior, moderate on interpretation

## Intent

The reporter understands the documented uncertainty and asks what ANWB's
undocumented `hasGap` field most likely represents.

## Evidence

- `hasGap` is a boolean on each account-cache usage row.
- Authenticated probes observed `hasGap: true` on rows with usable, non-zero
  usage.
- The integration does not discard or gate usage based on this field.
- ANWB explains that missing smart-meter readings can arrive later or be
  distributed over the missing period using a standard usage profile.
- No official field definition or independent public implementation was found.
- No duplicate or related repository issue was found.

## Interpretation

The strongest defensible inference is that `hasGap` is an advisory completeness
or provenance flag. It probably means that one or more underlying meter
intervals were missing, delayed, reconstructed, or profile-estimated while the
aggregate row still contains usable usage.

Unknowns include the exact threshold, whether the flag distinguishes partial,
estimated, or backfilled data, and how it propagates across HOUR, DAY, and MONTH.

## Recommendation

Add the `question` label, answer with the verified limits and the best inference,
then close as answered. Public action requires maintainer approval.

## Public Draft

Hi Eddict, good question. I could not find an official definition, so this is
still an informed interpretation.

`hasGap` most likely means that the hour, day, or month contains a gap in the
underlying meter readings, possibly filled later or estimated from a usage
profile. That fits [ANWB's explanation](https://www.anwb.nl/energie/slimme-meter)
of how missing smart-meter data is handled.

It does not appear to mean "ignore this row": we observed `hasGap: true` on rows
with non-zero usage. The integration therefore treats it as an advisory
data-quality flag. What remains unclear is whether it means partial, estimated,
backfilled, or simply that a gap was detected, and there is no evidence yet
that it explains the zero cost fields.

So the short answer is: probably a completeness warning, but ANWB has not
documented the exact semantics. Thanks for asking!
