# Changelog

## [1.5.1](https://github.com/adrighem/ha-anwb-energie/compare/v1.5.0...v1.5.1) (2026-10-24)


### Bug Fixes

* refresh Kraken token on HTTP 401 or GraphQL auth errors (fixes [#35](https://github.com/adrighem/ha-anwb-energie/issues/35)) ([#40](https://github.com/adrighem/ha-anwb-energie/issues/40)) ([b9b833d](https://github.com/adrighem/ha-anwb-energie/commit/b9b833d15c53092197f318860cef623196d621ac))
* retry incomplete hourly electricity market tariffs ([#32](https://github.com/adrighem/ha-anwb-energie/issues/32)) ([80a7f9f](https://github.com/adrighem/ha-anwb-energie/commit/80a7f9f1c9e6ade27fe9bf82c0c4ee3df5b42c05))

## [1.5.0](https://github.com/adrighem/ha-anwb-energie/compare/v1.4.1...v1.5.0) (2026-10-23)


### Features

* add export market value sensors and split export credit breakdown ([#38](https://github.com/adrighem/ha-anwb-energie/issues/38)) ([4fa196c](https://github.com/adrighem/ha-anwb-energie/commit/4fa196c200733276e8310218b5af5ae801acce07)), closes [#29](https://github.com/adrighem/ha-anwb-energie/issues/29)
* **probe:** add current-month window and Kraken statistics command ([#34](https://github.com/adrighem/ha-anwb-energie/issues/34)) ([398aa2b](https://github.com/adrighem/ha-anwb-energie/commit/398aa2b1bee0f078c8596ef1c6464a00acd235d3))


### Bug Fixes

* split daily tariff requests below the 200-price limit ([#31](https://github.com/adrighem/ha-anwb-energie/issues/31)) ([4e1cdee](https://github.com/adrighem/ha-anwb-energie/commit/4e1cdeee806b8f7b9cdb48824170ee6adfa7c362)) (@Remc0000)
* sum hourly account-cache fixed charges (fixes [#30](https://github.com/adrighem/ha-anwb-energie/issues/30)) ([10d384c](https://github.com/adrighem/ha-anwb-energie/commit/10d384ccf4ef6644775aeae0036ac3d460aae5ad))

## [1.4.1](https://github.com/adrighem/ha-anwb-energie/compare/v1.4.0...v1.4.1) (2026-10-16)


### Bug Fixes

* allow setup when initial gas cache request fails (fixes [#26](https://github.com/adrighem/ha-anwb-energie/issues/26)) ([38eac23](https://github.com/adrighem/ha-anwb-energie/commit/38eac235fd3517ebf0a601075798167c9ba01c6e))

## [1.4.0](https://github.com/adrighem/ha-anwb-energie/compare/v1.3.5...v1.4.0) (2026-10-04)


### Features

* use ANWB's daily cost statistics from Kraken for electricity costs (closes [#23](https://github.com/adrighem/ha-anwb-energie/issues/23)) ([#24](https://github.com/adrighem/ha-anwb-energie/issues/24)) ([bd6106f](https://github.com/adrighem/ha-anwb-energie/commit/bd6106f933ea93ef2ca270e6b59322cfacf3fe07)) (@imc67)

## [1.3.5](https://github.com/adrighem/ha-anwb-energie/compare/v1.3.4...v1.3.5) (2026-10-01)


### Bug Fixes

* address review points for post-2027 export pricing (fixes [#21](https://github.com/adrighem/ha-anwb-energie/issues/21)) ([c807d64](https://github.com/adrighem/ha-anwb-energie/commit/c807d64cc2128a235270324ce511e021b088b6b4))
* switch electricity export pricing to market price after net metering ends (fixes [#21](https://github.com/adrighem/ha-anwb-energie/issues/21)) ([629234e](https://github.com/adrighem/ha-anwb-energie/commit/629234ea87b8829862079b11abf74bdbea912566))

## [1.3.4](https://github.com/adrighem/ha-anwb-energie/compare/v1.3.3...v1.3.4) (2026-09-04)


### Bug Fixes

* extend hourly tariff fetch range to prevent null boundary gaps (refs [#19](https://github.com/adrighem/ha-anwb-energie/issues/19)) ([76613f8](https://github.com/adrighem/ha-anwb-energie/commit/76613f87c61ea3fe3577c5c813cb29d78535701d))

## [1.3.3](https://github.com/adrighem/ha-anwb-energie/compare/v1.3.2...v1.3.3) (2026-07-30)


### Bug Fixes

* harden authentication and diagnostics ([b9594de](https://github.com/adrighem/ha-anwb-energie/commit/b9594debea6ce6a86652a294c744c7d9c5d1021d))

## [1.3.2](https://github.com/adrighem/ha-anwb-energie/compare/v1.3.1...v1.3.2) (2026-07-28)


### Bug Fixes

* correct Home Assistant brand icon sizes ([8117dcc](https://github.com/adrighem/ha-anwb-energie/commit/8117dcc25a324960f36b9eaca4faba5311b67cc4))
* finish local brand asset cleanup ([3e70c6a](https://github.com/adrighem/ha-anwb-energie/commit/3e70c6af0c466b66efdfe92d1bb789fb8fdeab05))

## [1.3.1](https://github.com/adrighem/ha-anwb-energie/compare/v1.3.0...v1.3.1) (2026-07-24)


### Bug Fixes

* use local periods for account cache queries ([901358a](https://github.com/adrighem/ha-anwb-energie/commit/901358a35cbddf24ee2f2d3c9cb611e83b8c9ea0)), closes [#14](https://github.com/adrighem/ha-anwb-energie/issues/14)

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
