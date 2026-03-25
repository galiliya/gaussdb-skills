---
name: gauss-db-local
description: Use this repository-local CLI to inspect or query a configured Gauss database without storing real credentials in git.
---

# Gauss DB Local

Use the bundled CLI at `tools/gauss-db`.

Default config path:

`.codex-local/gauss-db.properties`

Before first use:

1. Copy `.codex-local/gauss-db.properties.example` to `.codex-local/gauss-db.properties`
2. Fill `db.host`, `jdbc.username`, and `jdbc.password`
3. Run `tools/gauss-db tables`

## Commands

- List visible tables:
  `tools/gauss-db tables`
- Describe a table:
  `tools/gauss-db describe --table schema.table`
- Run SQL text:
  `tools/gauss-db sql --sql "select * from schema.table fetch first 10 rows only"`
- Run SQL file:
  `tools/gauss-db sql --file /absolute/path/query.sql`

## Rules

- Use this tool only for the database you are authorized to access.
- Keep real credentials in the local `.codex-local/gauss-db.properties` file only.
- For exploration, prefer `tables`, then `describe`, then targeted `select`.
- If the user did not explicitly ask for destructive changes, do not run insert, update, delete, truncate, or DDL statements.
- The CLI returns JSON. Summarize the important parts instead of dumping large raw output.

