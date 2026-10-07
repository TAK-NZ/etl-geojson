# CHANGELOG

## Emoji Cheatsheet
- :pencil2: doc updates
- :bug: when fixing a bug
- :rocket: when making general improvements
- :white_check_mark: when adding tests
- :arrow_up: when upgrading dependencies
- :tada: when adding new features

## Version History

### v2.11.0
- :tada: Add `capabilities.json` manifest (`feature:*` required, schedule default `rate(1 minute)`) validated against `StaticCapabilitiesSchema`, embedded as the `com.cloudtak.capabilities` OCI annotation on the pushed image by a `docker buildx build` step in the deploy workflow (TAK-NZ/CloudTAK#166)
- :arrow_up: Update `@tak-ps/etl` from pinned `10.8.0` to `^10.22.2` and move all dependencies to caret ranges
- :white_check_mark: Add basic test suite (`tsx --test`) including capabilities manifest validation
- :rocket: Keep the manual buildx build/push instead of the `cloudtak-etl` CLI because its `bin/build.ts` hardcodes ECR repo `tak-vpc-<Environment>-cloudtak-tasks` while base-infra creates `<stackname>-etltasks`
### v2.6.0

- :arrow_up: Update Core Dependencies

### v2.5.0

- :arrow_up: Update Core Deps

### v2.4.0

- :arrow_up: Update Core Deps

### v2.3.0

- :arrow_up: Update Core Deps
 
### v2.2.0

- :tada: Handle MultiGeometries

### v2.1.0

- :tada: Add Invalid Geometry Filtering

### v2.0.0

- :tada: Update to `CloudTAK@v6`

### v1.4.0

- :tada: Add Capabilities API

### v1.3.0

- :rocket: Add ability to remove ID

### v1.2.0

- :rocket: Add Fallback ID

### v1.1.0

- :rocket: Make Query Params and Headers Optional

### v1.0.1

- :bug: Fix Build error

### v1.0.0

- :tada: Initial Commit
