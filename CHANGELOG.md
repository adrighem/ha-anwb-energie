# Changelog

## [1.3.0](https://github.com/adrighem/ha-anwb-energie/compare/v1.2.0...v1.3.0) (2026-07-23)


### Features

* streamline cost entities and add persistent tariff caching ([50374a9](https://github.com/adrighem/ha-anwb-energie/commit/50374a9d2ab304afeb42a76aeb09185b55d24f37))

## [1.2.0](https://github.com/adrighem/ha-anwb-energie/compare/v1.1.0...v1.2.0) (2026-07-08)


### Features

* expose electricity market price ([af904b3](https://github.com/adrighem/ha-anwb-energie/commit/af904b34a18b647176a8a642fec1daaec7758492)), closes [#11](https://github.com/adrighem/ha-anwb-energie/issues/11)

## [1.1.0](https://github.com/adrighem/ha-anwb-energie/compare/v1.0.2...v1.1.0) (2026-06-17)


### Features

* add canonical energy entities ([c4fe3c1](https://github.com/adrighem/ha-anwb-energie/commit/c4fe3c1d159d372930682fc9e5b58f60c018d042))


### Bug Fixes

* limit consumption cache fallback to DNS grace period ([4d631db](https://github.com/adrighem/ha-anwb-energie/commit/4d631db327d6ea1083c873e525633a429cb42d8f))
* limit hassfest workflow permissions ([2947a82](https://github.com/adrighem/ha-anwb-energie/commit/2947a824f7e7858f11f2d60f41c3be1cefb1b059))

## [1.0.2](https://github.com/adrighem/ha-anwb-energie/compare/v1.0.1...v1.0.2) (2026-05-13)


### Bug Fixes

* change electricity price fetch time check to 13:00 UTC and handle gas gracefully ([5148372](https://github.com/adrighem/ha-anwb-energie/commit/51483723a1cb1cb22be8800443fca90550295de2))
* fallback to cached data when API update fails to prevent unnecessary unavailable state ([c99d194](https://github.com/adrighem/ha-anwb-energie/commit/c99d194c39935b8aa144d3ee3ad12e4f297eec57))

## [1.0.1](https://github.com/adrighem/ha-anwb-energie/compare/v1.0.0...v1.0.1) (2026-04-24)


### Bug Fixes

* correct timezone parsing and format standardization for API timestamps ([29bae66](https://github.com/adrighem/ha-anwb-energie/commit/29bae6675753c913ed959c72f87631ad735521f6))
* remove hardcoded release version from release-please config ([ea31a0f](https://github.com/adrighem/ha-anwb-energie/commit/ea31a0f381705cea5529c5987067e1645d471063))
* remove hardcoded release version from release-please config ([97b60fb](https://github.com/adrighem/ha-anwb-energie/commit/97b60fbdf595220d0f44e0f8998029da9446ace1))

## [1.0.0](https://github.com/adrighem/ha-anwb-energie/compare/v0.9.1...v1.0.0) (2026-04-21)


### Features

* split coordinators and optimize price fetching ([da798b4](https://github.com/adrighem/ha-anwb-energie/commit/da798b4606b63f121a963edae07d473f3b67d6f7))


### Bug Fixes

* correctly format and sort manifest.json keys for hassfest validation ([5e851c1](https://github.com/adrighem/ha-anwb-energie/commit/5e851c16c95677f92b067c9806f335f12036daf9))
* update manifest with documentation URL and issue tracker for validation ([de238e8](https://github.com/adrighem/ha-anwb-energie/commit/de238e83065e8a7bf682ff4fcd94535673165bc8))

## [0.9.1](https://github.com/adrighem/ha-anwb-energie/compare/v0.9.0...v0.9.1) (2026-04-20)


### Bug Fixes

* correct API endpoint for dynamic gas prices ([e66eab6](https://github.com/adrighem/ha-anwb-energie/commit/e66eab6feac7e0a34b7afb9b86df96bc8255b9a9))

## [0.9.0](https://github.com/adrighem/ha-anwb-energie/compare/v0.8.2...v0.9.0) (2026-04-20)


### Features

* add support for gas subscriptions ([c55e149](https://github.com/adrighem/ha-anwb-energie/commit/c55e1497e577e013fe23e0ba62040f4ce6a66210))

## [0.8.2](https://github.com/adrighem/ha-anwb-energie/compare/v0.8.1...v0.8.2) (2026-04-20)


### Bug Fixes

* handle Kraken token expiry and retry transparently ([8f1ab2c](https://github.com/adrighem/ha-anwb-energie/commit/8f1ab2c05366ff85273469ccfb86376b5102c1ad))

## [0.8.1](https://github.com/adrighem/ha-anwb-energie/compare/v0.8.0...v0.8.1) (2026-04-20)


### Bug Fixes

* only trigger reauth on actual HTTP 4xx token errors ([97dafc6](https://github.com/adrighem/ha-anwb-energie/commit/97dafc66f32a2099abd3b86e6429e590d7c72da1))

## [0.8.0](https://github.com/adrighem/ha-anwb-energie/releases/tag/v0.8.0) (2026-04-20)

* Initial release of the ANWB Energie Account integration for Home Assistant.
