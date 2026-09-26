# Verification notes

Documentation review: September 25, 2026.

Environment: Linux, Rust 1.92.0. The committed Cargo.lock records the dependency resolution used for the build.

Passed:

- `cargo build`: compiled the CLI in the development profile.
- The resulting executable's `--help`: verified the option names and defaults documented in the README.

No Evergreen database was connected and no records were exported. XML validity, completeness, behavior during database changes, production throughput and the release-profile build were not tested. The README's limitations were checked against the CLI, database query and writer source.
