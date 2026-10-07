# CHANGELOG

## Emoji Cheatsheet
- :pencil2: doc updates
- :bug: when fixing a bug
- :rocket: when making general improvements
- :white_check_mark: when adding tests
- :arrow_up: when upgrading dependencies
- :tada: when adding new features

## Version History

### v2.6.0

- :arrow_up: Update Core Dependencies
- :arrow_up: Update GitHub Actions to releases that run on Node.js 24, clearing the Node.js 20 deprecation warnings: `actions/checkout` v7, `actions/setup-node` v7 and `aws-actions/configure-aws-credentials` v6. `aws-actions/amazon-ecr-login` v2 already runs on Node.js 24. Not yet run in CI on these versions
- :rocket: Pin the workflow runners to `ubuntu-24.04` instead of `ubuntu-latest`, so the `ubuntu-latest` migration to Ubuntu 26 (starting October 19, 2026) does not change the build environment unannounced

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
