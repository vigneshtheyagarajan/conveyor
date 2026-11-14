# Contributing

Thanks for helping improve Conveyor.

## Setup

```bash
npm install
npm run typecheck
npm test
```

Node 22.13 or newer is required (the SQLite store uses the built-in `node:sqlite` module).

## Guidelines

- Keep the runtime dependency-free. Dev dependencies are fine.
- New behaviour needs tests. Time-dependent code should take a `Clock` so tests can use `ManualClock`.
- Schema changes go in a **new** entry at the end of `src/storage/migrations.ts`. Never edit a released migration.
- Anything added to the `Store` interface must be implemented by every store.
- Run `npm run typecheck && npm test` before opening a pull request.
