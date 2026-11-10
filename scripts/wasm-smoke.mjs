// Run the wasm ETL on the repository CSVs and check sql.js against bonus_query.sql.
import { createRequire } from "node:module";
import { execSync } from "node:child_process";
import { readFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const require = createRequire(import.meta.url);
const goroot = execSync("go env GOROOT", { encoding: "utf8" }).trim();
require(path.join(goroot, "misc/wasm/wasm_exec.js"));

const google = readFileSync(path.join(root, "google_clicks.csv"), "utf8");
const facebook = readFileSync(path.join(root, "facebook_clicks.csv"), "utf8");
const site = readFileSync(path.join(root, "site_events.csv"), "utf8");
const bonusSQL = readFileSync(path.join(root, "bonus_query.sql"), "utf8");

function assert(cond, message) {
  if (!cond) {
    console.error(message);
    process.exit(1);
  }
}

function nearly(got, want) {
  assert(Math.abs(got - want) < 1e-9, `got ${got}, want ${want}`);
}

const go = new Go();
const wasm = readFileSync(path.join(root, "site/etl.wasm"));
const { instance } = await WebAssembly.instantiate(wasm, go.importObject);
const pending = go.run(instance);
void pending;

assert(typeof globalThis.runMarketingETL === "function", "runMarketingETL was not registered");

const channels = new Map();
let last;
for (let i = 0; i < 20; i++) {
  last = JSON.parse(globalThis.runMarketingETL(google, facebook, site));
  assert(!last.error, last.error || "etl error");
  const summary = last.summary;
  assert(summary.users === 636, `users ${summary.users}`);
  assert(summary.events === 3311, `events ${summary.events}`);
  assert(summary.clicks === 907 && summary.views === 2191 && summary.conversions === 52, "event types");
  assert(summary.google_clicks === 537 && summary.facebook_clicks === 370 && summary.website_events === 2404, "sources");
  nearly(summary.google_cost, 474.7);
  nearly(summary.facebook_cost, 286.21);
  nearly(summary.revenue, 2463.3);
  assert(summary.multi_channel_converters === 17, "go multi");
  assert(summary.empty_website_metadata === 0 && summary.empty_user_ids === 0, "empty fields");
  assert(summary.skipped_rows === 0, "skipped");
  assert(summary.days.length === 26, `days ${summary.days.length}`);
  assert(Array.isArray(last.logs) && last.logs.length === 0, "unexpected logs");
  const signature = last.users
    .map((user) => `${user.session_key}|${user.utm_channel}|${user.earliest_time}`)
    .sort()
    .join("\n");
  channels.set(signature, (channels.get(signature) || 0) + 1);
}

const initSqlJs = require(path.join(root, "site/vendor/sql-wasm.js"));
const SQL = await initSqlJs({
  locateFile: (file) => path.join(root, "site/vendor", file),
});
const db = new SQL.Database();
db.run(`CREATE TABLE tracked_user (
  tracked_user_id INTEGER PRIMARY KEY,
  user_id TEXT,
  session_key TEXT,
  tracking_hash TEXT,
  utm_channel TEXT,
  attributes TEXT,
  earliest_time TEXT,
  latest_time TEXT
)`);
db.run(`CREATE TABLE event (
  event_id INTEGER PRIMARY KEY,
  tracked_user_id INTEGER,
  event_time TEXT,
  event_type TEXT,
  source TEXT,
  campaign TEXT,
  cost REAL,
  revenue REAL,
  metadata TEXT
)`);
const insertUser = db.prepare(`INSERT INTO tracked_user (
  tracked_user_id, user_id, session_key, tracking_hash, utm_channel, attributes, earliest_time, latest_time
) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`);
for (const user of last.users) {
  insertUser.run([
    user.tracked_user_id,
    user.user_id,
    user.session_key,
    user.tracking_hash,
    user.utm_channel,
    user.attributes,
    user.earliest_time,
    user.latest_time,
  ]);
}
insertUser.free();
const insertEvent = db.prepare(`INSERT INTO event (
  event_id, tracked_user_id, event_time, event_type, source, campaign, cost, revenue, metadata
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`);
for (const event of last.events) {
  insertEvent.run([
    event.event_id,
    event.tracked_user_id,
    event.event_time,
    event.event_type,
    event.source,
    event.campaign,
    event.cost,
    event.revenue,
    event.metadata,
  ]);
}
insertEvent.free();

const bonus = db.exec(bonusSQL);
assert(bonus.length === 1, `bonus result sets ${bonus.length}`);
assert(bonus[0].columns[0] === "multi_channel_converters", bonus[0].columns.join(","));
assert(bonus[0].values[0][0] === 17, `sql multi ${bonus[0].values[0][0]}`);
assert(channels.size === 1, `channel signatures ${channels.size}`);

console.log("wasm smoke ok. 20 runs, one channel signature, multi_channel_converters=17");
process.exit(0);
