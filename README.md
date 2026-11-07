# @smaltdb/io

<a href="https://smaltworks.dev/stability"><picture><source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/stenhoor/smalt-brand/main/badges/work-in-progress-on-dark.svg"><img alt="Work in progress. Come build with us." src="https://raw.githubusercontent.com/stenhoor/smalt-brand/main/badges/work-in-progress.svg" height="56"></picture></a>

[Writing plugins](https://smaltdb.dev/reference/api/plugins) · [smaltworks.dev](https://smaltworks.dev/products/plugins)

CSV, TSV and JSON for [smaltdb](https://github.com/stenhoor/smaltdb): the `csv`, `tsv` and `json` (`ndjson`, `jsonl`)
formats for external tables, `read_csv()` and `read_json()`, and CSV / JSON import and export. No dependencies.

```sh
npm install smaltdb @smaltdb/io
```

In the browser, install it in your engine worker entry and open the database with that worker:

```ts
// db-worker.ts
import { serve } from 'smaltdb/worker-runtime';
import io from '@smaltdb/io';

serve({ plugins: [io] });
```

```ts
import { open } from 'smaltdb';
import { exportCSV, importCSV } from '@smaltdb/io/client';

const db = await open('app', {
  worker: () => new Worker(new URL('./db-worker.ts', import.meta.url), { type: 'module' }),
});
await importCSV(db, 'people', file, { create: true }); // a string, Uint8Array, ArrayBuffer or Blob
const csv = await exportCSV(db, 'SELECT * FROM people WHERE age > ?', [30]);
```

`importJSON` and `exportJSON` work the same way. Import and export need no language: import writes through plans, and
export also takes a plan statement (`PlanStatementInput` from `smaltdb/engine`) in place of the SQL query. Reading files with `read_csv()` / `read_json()` also needs a source,
such as `@smaltdb/external`.

With the in-process engine (Node): `Database.openMemory({ plugins: [io] })` from `smaltdb/engine`; the helpers take
that `Database` too.

See the [plugins guide](https://github.com/stenhoor/smaltdb/blob/main/docs/guide/plugins.md). MIT licence.

## Development

```sh
npm ci && npm run deps   # deps: the pinned smaltdb build from packed.json (until 0.9.0 is on npm)
npm run check            # type-check and test
```

See [CLAUDE.md](CLAUDE.md) for how the pins work.

<a href="https://smaltworks.dev"><picture><source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/stenhoor/smalt-brand/main/badges/a-smaltworks-project-on-dark.svg"><img alt="A smaltworks project" src="https://raw.githubusercontent.com/stenhoor/smalt-brand/main/badges/a-smaltworks-project.svg" height="28"></picture></a>

MIT License.
