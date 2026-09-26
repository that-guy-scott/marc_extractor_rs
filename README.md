# MARC Extractor RS

Export MARC XML records from an Evergreen library database into a MARC collection file. This Rust command-line tool reads PostgreSQL in chunks, fetches chunks concurrently and sends records to a single XML writer.

It is a focused data-operations utility: extract catalog records without building a separate application around the database. The repository contains the CLI, database reader and XML writer; it does not include a database or a published container image.

## Data flow

```text
Evergreen PostgreSQL (biblio.record_entry)
    -> chunk queries through a connection pool
    -> bounded record channel
    -> MARC XML collection file
```

The reader uses `id`, `marc` and `deleted` from `biblio.record_entry`. Empty or null MARC values are skipped. Deleted records are excluded unless requested.

## Build

Requirements: a current stable Rust toolchain and, for extraction, access to an Evergreen PostgreSQL database with read permissions on the source table.

```bash
git clone https://github.com/that-guy-scott/marc_extractor_rs.git
cd marc_extractor_rs
cargo build --locked --release
./target/release/marc_extractor_rs --help
```

## Extract records

Set `DATABASE_URL` in your shell using your database credentials. The following value is an example, not a working credential:

```bash
export DATABASE_URL='postgresql://reader:replace-me@localhost/evergreen'
./target/release/marc_extractor_rs \
  --output catalog.xml \
  --workers 4 \
  --chunk-size 1000 \
  --verbose
```

Start on a test copy or read replica and validate the output before using it downstream. `--output` creates or replaces the named file. Prefer it over shell redirection because logging can share stdout with XML output.

| Option | Behavior |
| --- | --- |
| `--db-url` | Database URL; alternatively set `DATABASE_URL`. |
| `-o`, `--output` | Output file; otherwise writes XML to stdout. |
| `-w`, `--workers` | Maximum pool connections; default 10. |
| `-c`, `--chunk-size` | Records requested per chunk; default 1000. |
| `-d`, `--include-deleted` | Include rows marked deleted. |
| `-v`, `--verbose` | Enable informational logging. |
| `--limit` | Limit scheduling by record count; see the chunk-boundary limitation below. |

Use positive values for workers and chunk size. The current CLI does not validate all numeric combinations.

## Output

The writer wraps stored MARC XML in a collection:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<collection xmlns="http://www.loc.gov/MARC21/slim">
  <!-- Record XML from the source database -->
</collection>
```

This is a structural illustration, not a sample export or a MARC validation result.

## Limits and tradeoffs

- Queries use `LIMIT`/`OFFSET`; a concurrently changing database can produce an inconsistent export. Use a stable source for reproducible results.
- Chunks can complete out of order, so output order is not guaranteed.
- `--limit` controls chunk scheduling, but the last query still requests a full chunk. It is not a strict exported-record cap.
- Wrapper cleanup is string-based and does not validate or repair MARC XML. Validate the resulting file with your downstream tools.
- Fetch failures are counted; write errors and worker failures are logged but are not all reflected in the final error count. Inspect logs as well as the exit status.
- Concurrency is a tuning control, not a promised speedup. There is no reproducible benchmark dataset or harness in this repository; previous numerical speed comparisons have been removed.

## Code and verification

- [CLI and orchestration](src/main.rs)
- [Database queries](src/db.rs)
- [XML output](src/writer.rs)

`cargo build` and `--help` verify compilation and CLI availability without connecting to a database. An actual export requires an Evergreen database and separate data-integrity checks. See [verification notes](docs/verification.md).
