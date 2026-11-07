/**
 * CSV and JSON / NDJSON import and export. Runs inside the engine (worker), so rows never cross the
 * message boundary one by one: imports go through one prepared write in a single transaction (all
 * constraints, conversions and foreign keys apply), exports format query results.
 *
 * Imports are plans, not SQL text (smaltdb ADR 0017): the table, the writes and the transaction are plan statements,
 * so they work with no language registered. Exports run a query given as SQL text or as a plan statement.
 */
import type { Database, MethodContext, PlanStatementInput, SqlValue } from 'smaltdb/engine';
import type { CsvExportOptions, CsvImportOptions, ExportParams, ImportSource, JsonExportOptions, JsonImportOptions } from './options.js';
import { type Field, IoError, csvRecords, inferType, jsonValue, numericOrText, parseJson } from './text.js';

/** Run a statement given as plan data. */
const runPlan = (db: Database, stmt: PlanStatementInput, params?: SqlValue[]): void => void db.preparePlan(stmt).run(params);

/** The savepoint an import inside the caller's transaction runs in. */
const SAVEPOINT = 'smaltdb_import';

/** An import source as text (bytes and blobs are UTF-8). */
export async function sourceText(source: ImportSource): Promise<string> {
  if (typeof source === 'string') return source;
  if (source instanceof Uint8Array) return new TextDecoder().decode(source);
  if (source instanceof ArrayBuffer) return new TextDecoder().decode(new Uint8Array(source));
  if (source && typeof (source as Blob).arrayBuffer === 'function') return new TextDecoder().decode(new Uint8Array(await (source as Blob).arrayBuffer()));
  throw new IoError('SMALT_MISUSE', 'the import source must be a string, Uint8Array, ArrayBuffer or Blob');
}

/**
 * The import source as text, read before the import starts. Reading a Blob waits, and meanwhile the
 * call may be cancelled, or its tab close and its transaction be rolled back: the import must then
 * not go ahead (on its own, it would commit what was meant for the transaction).
 */
export async function importText(ctx: MethodContext, source: ImportSource): Promise<string> {
  const inTransaction = ctx.db.inTransaction;
  const text = await sourceText(source);
  if (ctx.signal?.aborted) throw new IoError('SMALT_INTERRUPT', 'interrupted');
  if (inTransaction && !ctx.db.inTransaction) throw new IoError('SMALT_INTERRUPT', 'the transaction ended while the import source was read');
  return text;
}

/** `e` with `${where}: ` in front of its message, keeping its class and code (an engine error stays one). */
function at(where: string, e: unknown): Error {
  if (e instanceof Error && typeof (e as { code?: unknown }).code === 'string') {
    e.message = `${where}: ${e.message}`;
    return e;
  }
  return new IoError('SMALT_ERROR', `${where}: ${e instanceof Error ? e.message : String(e)}`);
}

type Row = { values: SqlValue[]; where: string; present?: boolean[] };

/**
 * Insert rows (values per target column) into `table` in one transaction, or in a savepoint of the
 * caller's open transaction: on failure nothing is left behind, including a table it created.
 */
function load(db: Database, table: string, columns: string[], rows: Iterable<Row>, create: boolean, sample: SqlValue[][]): number {
  const own = !db.inTransaction;
  runPlan(db, own ? { kind: 'transaction', action: 'begin', mode: 'deferred' } : { kind: 'transaction', action: 'savepoint', name: SAVEPOINT });
  try {
    const n = loadRows(db, table, columns, rows, create, sample);
    runPlan(db, own ? { kind: 'transaction', action: 'commit' } : { kind: 'transaction', action: 'release', name: SAVEPOINT });
    return n;
  } catch (e) {
    if (own) {
      if (db.inTransaction) runPlan(db, { kind: 'transaction', action: 'rollback' });
    } else {
      runPlan(db, { kind: 'transaction', action: 'rollback', savepoint: SAVEPOINT });
      runPlan(db, { kind: 'transaction', action: 'release', name: SAVEPOINT });
    }
    throw e;
  }
}

function loadRows(db: Database, table: string, columns: string[], rows: Iterable<Row>, create: boolean, sample: SqlValue[][]): number {
  let t = db.getTable(table);
  if (!t) {
    if (!create) throw new IoError('SMALT_ERROR', `no such table: ${table}`);
    // A table with the inferred column types, defined without source text: it belongs to no language.
    const defs = columns.map((c, i) => {
      const type = inferType(sample.map((r) => r[i] ?? null)) as 'INTEGER' | 'REAL' | 'TEXT' | 'BLOB' | 'ANY' | 'NUMERIC';
      return { name: c, declaredType: type, type, notNull: false, collation: 'binary' as const };
    });
    runPlan(db, { kind: 'schema', op: { op: 'createTable', table: { name: table, columns: defs, keys: [], checks: [], foreignKeys: [], storage: 'row' }, temp: false, ifNotExists: false } });
    t = db.getTable(table)!;
  }
  const idx = columns.map((c) => {
    const i = t!.columns.findIndex((x) => x.name.toLowerCase() === c.toLowerCase());
    if (i < 0) throw new IoError('SMALT_ERROR', `table ${t!.name} has no column named ${c}`);
    return i;
  });
  // ANY / NUMERIC columns get numbers for numeric-looking text (typed columns convert on their own).
  const loose = idx.map((i) => t!.columns[i].type === 'ANY' || t!.columns[i].type === 'NUMERIC');
  // One prepared write per set of present columns (absent JSON keys take the column DEFAULT, not NULL): a row of
  // parameters appended to those columns, or a row of defaults.
  const target = t.temp ? 'temp' : 'main';
  const statements = new Map<string, ReturnType<Database['preparePlan']>>();
  const stmtFor = (present: boolean[] | undefined): { st: ReturnType<Database['preparePlan']>; pick: number[] } => {
    const pick = present ? idx.flatMap((_, k) => (present[k] ? [k] : [])) : idx.map((_, k) => k);
    const key = pick.join(',');
    let st = statements.get(key);
    if (!st) {
      const row = pick.map((_, i) => ({ k: 'param' as const, index: i, type: 'any' as const }));
      const input = pick.length ? { op: 'values' as const, rows: [row], types: pick.map(() => 'any' as const) } : null;
      st = db.preparePlan({ kind: 'write', plan: { op: 'append', table: t!.name, db: target, input, columns: pick.map((k) => idx[k]), upserts: [] } });
      statements.set(key, st);
    }
    return { st, pick };
  };
  let n = 0;
  for (const r of rows) {
    const vals = r.values.map((v, i) => (loose[i] && typeof v === 'string' ? numericOrText(v) : v));
    const { st, pick } = stmtFor(r.present);
    try {
      st.run(pick.map((k) => vals[k]));
    } catch (e) {
      throw at(r.where, e);
    }
    n++;
  }
  return n;
}

export function importCSV(db: Database, table: string, text: string, opts: CsvImportOptions = {}): { rows: number } {
  const delim = opts.delimiter ?? ',';
  const quote = opts.quote ?? '"';
  if (delim.length !== 1 || quote.length !== 1) throw new IoError('SMALT_RANGE', 'CSV delimiter and quote must be single characters');
  const emptyAsNull = opts.emptyAsNull ?? true;
  const it = csvRecords(text, delim, quote);
  let columns = opts.columns;
  const first = it.next();
  if (first.done) return { rows: 0 };
  const records: { fields: Field[]; line: number }[] = [];
  if (opts.header ?? true) {
    columns ??= first.value.fields.map((f) => f.text.trim());
  } else records.push(first.value);
  if (!columns) {
    const t = db.getTable(table);
    if (!t) throw new IoError('SMALT_ERROR', `no such table: ${table} (give a header or columns to create it)`);
    columns = t.columns.map((c) => c.name);
  }
  const width = columns.length;
  const toValues = (r: { fields: Field[]; line: number }): Row => {
    if (r.fields.length !== width) throw new IoError('SMALT_ERROR', `CSV line ${r.line}: expected ${width} fields but found ${r.fields.length}`);
    return { values: r.fields.map((f) => (!f.quoted && f.text === '' && emptyAsNull ? null : f.text)), where: `CSV line ${r.line}` };
  };
  // Creating the table needs the values first (type inference); otherwise rows stream.
  const needSample = !db.getTable(table) && opts.create;
  if (needSample) {
    for (const r of it) records.push(r);
    const rows = records.map(toValues);
    return { rows: load(db, table, columns, rows, true, rows.map((r) => r.values)) };
  }
  function* all(): Generator<Row> {
    for (const r of records) yield toValues(r);
    for (const r of it) yield toValues(r);
  }
  return { rows: load(db, table, columns, all(), false, []) };
}

const kindOf = (v: unknown): string => (v === null ? 'null' : Array.isArray(v) ? 'an array' : typeof v === 'object' ? 'an object' : typeof v === 'string' ? 'a string' : typeof v === 'boolean' ? 'a boolean' : 'a number');

export function importJSON(db: Database, table: string, text: string, opts: JsonImportOptions = {}): { rows: number } {
  const body = text.charCodeAt(0) === 0xfeff ? text.slice(1) : text;
  const trimmed = body.trimStart();
  let items: { value: unknown; where: string }[];
  if (trimmed.startsWith('[')) {
    let arr: unknown;
    try {
      arr = parseJson(body);
    } catch (e) {
      throw new IoError('SMALT_ERROR', `invalid JSON: ${(e as Error).message}`);
    }
    if (!Array.isArray(arr)) throw new IoError('SMALT_ERROR', 'JSON import expects an array or NDJSON');
    items = arr.map((value, i) => ({ value, where: `JSON item ${i + 1}` }));
  } else {
    items = [];
    body.split(/\r?\n/).forEach((l, i) => {
      if (l.trim() === '') return;
      try {
        items.push({ value: parseJson(l), where: `NDJSON line ${i + 1}` });
      } catch (e) {
        throw new IoError('SMALT_ERROR', `NDJSON line ${i + 1}: ${(e as Error).message}`);
      }
    });
  }
  if (items.length === 0) return { rows: 0 };
  // All objects or all arrays: say which item breaks the pattern set by the first one.
  const firstKind = kindOf(items[0].value);
  for (const x of items) {
    const kind = kindOf(x.value);
    if (kind !== 'an object' && kind !== 'an array') throw new IoError('SMALT_ERROR', `${x.where}: expected an object or an array, found ${kind} (JSON import expects an array of objects or of arrays, or NDJSON lines of either)`);
    if (kind !== firstKind) throw new IoError('SMALT_ERROR', `${x.where}: expected ${firstKind} like ${items[0].where}, found ${kind} (all items must be objects or all arrays)`);
  }
  const objects = firstKind === 'an object';
  let columns: string[];
  if (objects) {
    // Union of keys in first-seen order; a row's missing keys take the column's DEFAULT.
    const seen = new Map<string, string>();
    for (const x of items) for (const k of Object.keys(x.value as object)) if (!seen.has(k.toLowerCase())) seen.set(k.toLowerCase(), k);
    columns = [...seen.values()];
  } else {
    const t = db.getTable(table);
    columns = opts.columns ?? t?.columns.map((c) => c.name) ?? [];
    if (!columns.length) throw new IoError('SMALT_ERROR', `no such table: ${table} (give columns to create it)`);
  }
  const rows = items.map((x): Row => {
    if (objects) {
      const o = x.value as Record<string, unknown>;
      const byLower = new Map(Object.entries(o).map(([k, v]) => [k.toLowerCase(), v]));
      return { values: columns.map((c) => jsonValue(byLower.get(c.toLowerCase()))), where: x.where, present: columns.map((c) => byLower.has(c.toLowerCase())) };
    }
    const a = x.value as unknown[];
    if (a.length !== columns.length) throw new IoError('SMALT_ERROR', `${x.where}: expected ${columns.length} values but found ${a.length}`);
    return { values: a.map(jsonValue), where: x.where };
  });
  return { rows: load(db, table, columns, rows, !!opts.create, rows.map((r) => r.values)) };
}

// ------------------------------------------------------------------ export

const hex = (b: Uint8Array): string => Array.from(b, (x) => x.toString(16).padStart(2, '0')).join('').toUpperCase();

/** A result with its columns' static types (REAL values print as reals, `2.0`). */
interface TypedResult {
  columns: string[];
  rows: SqlValue[][];
  real: boolean[];
}

/**
 * Run a query through the engine (sources of external data are fetched first): SQL text, which needs SQL registered,
 * or a statement given as plan data, which needs no language. A plan's parameters are positional.
 */
async function typedResult(ctx: MethodContext, query: string | PlanStatementInput, params?: ExportParams): Promise<TypedResult> {
  let r: Awaited<ReturnType<MethodContext['query']>>;
  if (typeof query === 'string') r = await ctx.query(query, params as Parameters<MethodContext['query']>[1]);
  else {
    if (!ctx.queryPlan) throw new IoError('SMALT_MISUSE', 'exporting a plan needs smaltdb 0.9.0 or later');
    if (params !== undefined && !Array.isArray(params)) throw new IoError('SMALT_RANGE', "a plan's parameters are positional: give them as an array");
    r = await ctx.queryPlan(query, params as SqlValue[] | undefined);
  }
  return { columns: r.columns, rows: r.rows, real: r.columnTypes.map((t) => t === 'real') };
}

export async function exportCSV(ctx: MethodContext, formatReal: (v: number) => string, query: string | PlanStatementInput, params?: ExportParams, opts: CsvExportOptions = {}): Promise<string> {
  return formatCSV(await typedResult(ctx, query, params), opts, formatReal);
}

export async function exportJSON(ctx: MethodContext, formatReal: (v: number) => string, query: string | PlanStatementInput, params?: ExportParams, opts: JsonExportOptions = {}): Promise<string> {
  return formatJSON(await typedResult(ctx, query, params), opts, formatReal);
}

function formatCSV(res: TypedResult, opts: CsvExportOptions, formatReal: (v: number) => string): string {
  const delim = opts.delimiter ?? ',';
  const nl = opts.crlf ? '\r\n' : '\n';
  const nullString = opts.nullString ?? '';
  const real = res.real;
  /** Text of a value; `real` marks a REAL-typed result column (integral values print as 2.0, like SQLite). */
  const textOf = (v: SqlValue, real = false): string => {
    if (typeof v === 'number') return Number.isInteger(v) && Math.abs(v) < 2 ** 53 && !real ? String(v) : formatReal(v);
    if (typeof v === 'bigint') return v.toString();
    if (v instanceof Uint8Array) return hex(v);
    return String(v);
  };
  const field = (s: string): string => (s.includes(delim) || s.includes('"') || s.includes('\n') || s.includes('\r') || /^\s|\s$/.test(s) ? '"' + s.replace(/"/g, '""') + '"' : s);
  const out: string[] = [];
  if (opts.header ?? true) out.push(res.columns.map(field).join(delim));
  for (const row of res.rows) out.push(row.map((v, i) => (v === null ? nullString : field(textOf(v, real[i])))).join(delim));
  return out.length ? out.join(nl) + nl : '';
}

function formatJSON(res: TypedResult, opts: JsonExportOptions, formatReal: (v: number) => string): string {
  const { rows, real } = res;
  const jsonOf = (v: SqlValue, real = false): string => {
    if (v === null) return 'null';
    if (typeof v === 'number') return Number.isFinite(v) ? (Number.isInteger(v) && Math.abs(v) < 2 ** 53 && !real ? String(v) : formatReal(v)) : 'null';
    if (typeof v === 'bigint') return v.toString(); // exact digits (JSON numbers have no size limit)
    if (v instanceof Uint8Array) return JSON.stringify(hex(v));
    return JSON.stringify(v);
  };
  const keys = res.columns.map((c) => JSON.stringify(c));
  const objects = rows.map((row) => '{' + row.map((v, i) => `${keys[i]}:${jsonOf(v, real[i])}`).join(',') + '}');
  if (opts.format === 'ndjson') return objects.length ? objects.join('\n') + '\n' : '';
  return '[' + objects.join(',') + ']';
}
