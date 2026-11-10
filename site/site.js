const AGGREGATE = `
SELECT
  (SELECT COUNT(*) FROM tracked_user) AS users,
  (SELECT COUNT(*) FROM event) AS events,
  (SELECT COUNT(*) FROM event WHERE event_type = 'click') AS clicks,
  (SELECT COUNT(*) FROM event WHERE event_type = 'view') AS views,
  (SELECT COUNT(*) FROM event WHERE event_type = 'conversion') AS conversions,
  (SELECT COUNT(*) FROM event WHERE source = 'Google Ads' AND event_type = 'click') AS google_clicks,
  (SELECT COALESCE(SUM(cost), 0) FROM event WHERE source = 'Google Ads') AS google_cost,
  (SELECT COUNT(*) FROM event WHERE source = 'Facebook Ads' AND event_type = 'click') AS facebook_clicks,
  (SELECT COALESCE(SUM(cost), 0) FROM event WHERE source = 'Facebook Ads') AS facebook_cost,
  (SELECT COUNT(*) FROM event WHERE source = 'Website') AS website_events,
  (SELECT COALESCE(SUM(cost), 0) FROM event WHERE source = 'Website') AS website_cost,
  (SELECT COALESCE(SUM(revenue), 0) FROM event) AS revenue
`.trim();

const QUERIES = {
  totals: `
SELECT source, event_type, COUNT(*) AS events, SUM(cost) AS cost, SUM(revenue) AS revenue
FROM event
GROUP BY source, event_type
ORDER BY source, event_type
`.trim(),
  byUser: `
SELECT tu.tracked_user_id, tu.user_id, tu.session_key, tu.utm_channel,
       tu.earliest_time, tu.latest_time,
       SUM(CASE WHEN e.source = 'Google Ads' AND e.event_type = 'click' THEN 1 ELSE 0 END) AS google_clicks,
       SUM(CASE WHEN e.source = 'Facebook Ads' AND e.event_type = 'click' THEN 1 ELSE 0 END) AS facebook_clicks,
       SUM(CASE WHEN e.source = 'Website' THEN 1 ELSE 0 END) AS website_events,
       SUM(CASE WHEN e.event_type = 'conversion' THEN 1 ELSE 0 END) AS conversions,
       SUM(COALESCE(e.cost, 0)) AS cost,
       SUM(COALESCE(e.revenue, 0)) AS revenue
FROM tracked_user tu
LEFT JOIN event e ON e.tracked_user_id = tu.tracked_user_id
GROUP BY tu.tracked_user_id, tu.user_id, tu.session_key, tu.utm_channel, tu.earliest_time, tu.latest_time
ORDER BY tu.session_key, tu.tracked_user_id
`.trim(),
  both: `
SELECT tu.tracked_user_id, tu.session_key, tu.user_id,
       SUM(CASE WHEN e.source = 'Google Ads' AND e.event_type = 'click' THEN 1 ELSE 0 END) AS google_clicks,
       SUM(CASE WHEN e.source = 'Facebook Ads' AND e.event_type = 'click' THEN 1 ELSE 0 END) AS facebook_clicks,
       SUM(CASE WHEN e.event_type = 'conversion' THEN 1 ELSE 0 END) AS conversions
FROM tracked_user tu
JOIN event e ON e.tracked_user_id = tu.tracked_user_id
GROUP BY tu.tracked_user_id, tu.session_key, tu.user_id
HAVING SUM(CASE WHEN e.source = 'Google Ads' AND e.event_type = 'click' THEN 1 ELSE 0 END) > 0
   AND SUM(CASE WHEN e.source = 'Facebook Ads' AND e.event_type = 'click' THEN 1 ELSE 0 END) > 0
ORDER BY tu.session_key
`.trim(),
};

const LINEAGE = [
  ["tracked_user.tracking_hash", "session_key, device_type", "hash"],
  ["tracked_user.user_id", "user_id", "userId"],
  ["tracked_user.utm_channel", "utm_channel", "channel"],
  ["tracked_user.attributes", "constante / constant", "attributes"],
  ["tracked_user.earliest_time, latest_time", "timestamp", "window"],
  ["event.event_type", "click, action_type", "eventType"],
  ["event.source", "worker", "source"],
  ["event.campaign", "campaign", "campaign"],
  ["event.cost", "cost, spend", "cost"],
  ["event.revenue", "action_details.value", "revenue"],
  ["event.metadata (Google Ads, Facebook Ads)", "campaign, gclid, fbclid", "adMeta"],
  ["event.metadata (Website)", "action_details", "siteMeta"],
  ["device_type", "device_type", "device"],
];

const copy = {
  es: {
    skip: "Saltar al resultado",
    title: "Pipeline de marketing en el navegador",
    lede: "Esta página ejecuta el ETL de este repositorio, compilado a WebAssembly, sobre google_clicks.csv, facebook_clicks.csv y site_events.csv. Las cifras de abajo salen de esa ejecución.",
    runExample: "Correr el pipeline con datos de ejemplo",
    loading: "Cargando etl.wasm y los CSV del repositorio.",
    resultHeading: "Resultado de esta ejecución",
    ledgerCaption: "Los totales aparecen cuando terminan etl.wasm y sql.js.",
    measure: "Medida",
    value: "Valor",
    aggregateToggle: "Ver la consulta de los totales",
    usersHeading: "Usuarios unificados",
    usersNote: "Una fila por hash. El id sigue el orden del map de Go, el mismo criterio que el INSERT nativo, y cambia entre procesos.",
    usersCaption: "tracked_user producido por runPipeline.",
    eventsHeading: "Eventos",
    eventsNote: "Orden de esta tabla: event_time, luego source. El slice en memoria conserva el orden en que terminaron los workers.",
    eventsCaption: "event producido por runPipeline.",
    costHeading: "Costo por fuente",
    costNote: "Eje horizontal: suma de cost. El largo de cada barra es la suma de event.cost de esa source en esta ejecución. Los CSV son sintéticos.",
    sqlHeading: "Consulta de atribución",
    presetLabel: "Consulta",
    presetTotals: "Costo e ingreso por fuente y tipo",
    presetByUser: "Atribución por usuario",
    presetBoth: "Usuarios con clics en las dos plataformas",
    execSql: "Ejecutar consulta",
    queryLabel: "SQL sobre las tablas de esta carga",
    sqlHint: "Ejecutar consulta corre el texto de arriba. El número de conversores del resultado de esta ejecución sigue siendo bonus_query.sql del repositorio, aunque edites este cuadro. Ctrl+Enter también ejecuta.",
    logsHeading: "Filas omitidas",
    lineageHeading: "Linaje de columnas",
    lineageIntro: "Cada fila describe lo que hace el código de este repositorio. No es un modelo de atribución distinto.",
    lineageCaption: "Del CSV al campo guardado.",
    colColumn: "Columna",
    colFrom: "Viene de",
    colRule: "Regla",
    archHeading: "Arquitectura",
    archIntro: "Recorrido en el navegador. go run . llama a processGoogleClicks, processFacebookClicks y processSiteEvents, que a su vez llaman a estas mismas funciones de ingesta, y cierra con loadToDatabase sobre marketing_data.db. El diagrama nativo está en el README.",
    csvHeading: "CSV de esta ejecución",
    csvIntro: "El botón de arriba vuelve a leer los archivos del repositorio. Aquí puedes pegar otro texto o cargar un archivo y correr ese texto. Los CSV de ejemplo traen action_details con comillas escapadas: el JSON no parsea, metadata queda {} y revenue queda 0. Un objeto JSON válido en esa columna hace que revenue salga de value.",
    runEdited: "Correr el pipeline con este texto",
    restore: "Restaurar los CSV del repositorio",
    limitsHeading: "Qué cambia en el navegador",
    limit1: "modernc.org/sqlite v1.34.5 no entra en etl.wasm. Con GOOS=js GOARCH=wasm, modernc.org/libc excluye errno, pthread, signal, stdio, sys/types, time y unistd: sus archivos no declaran ese GOOS. go build de ese par lo rechaza. Por eso esta página no abre marketing_data.db. Las filas salen del mismo código de ingesta, compilado con la toolchain estándar de Go, y la consulta corre en sql.js 1.11.0, que es SQLite en WebAssembly.",
    limit2: "Cada corrida arma una base nueva. Un segundo go run . en la terminal añade otra copia al archivo. Aquí no hay archivo que conservar.",
    limit3: "Las goroutines y los mutex son los del programa. En el navegador el runtime de Go las reparte en un solo hilo de JavaScript, así que el utm_channel de sess-006 puede repetirse en cada recarga aunque en un proceso nativo cambie.",
    limit4: "tracked_user_id sale del orden de iteración del map. Es el mismo criterio que el INSERT nativo y no es estable entre procesos.",
    limit5: "Una fila de anuncios demasiado corta provoca un pánico de índice. Este binario lo anota en Filas omitidas y sigue. go run . aborta. Los CSV de ejemplo no disparan ese caso. Sus action_details tampoco son JSON válido, así que las cuatro filas del sitio quedan con metadata {} y revenue 0.",
    colophon: "Texto en IBM Plex Sans e IBM Plex Mono, de IBM, licencia SIL Open Font. IBM Plex tiene cifras tabulares, que es lo que usa una tabla de costos. El azul #1C4E89 es el del papel carbón: esta página es una copia del pipeline nativo, con el mismo código de transformación y la misma consulta.",
    hash: "hex(SHA-256(session_key + device_type)), sin separador. session_key ab con device_type c produce el mismo hash que session_key a con device_type bc.",
    userId: "Se escribe solo cuando el valor guardado sigue vacío.",
    channel: "Se queda el del primer worker que inserta el hash.",
    attributes: "El literal {}.",
    window: "Mínimo y máximo. La comparación es texto con forma YYYY-MM-DD HH:MM:SS.",
    eventType: "purchase y signup se guardan como conversion. Cualquier otra acción se guarda tal cual.",
    source: "Google Ads, Facebook Ads o Website, según el worker.",
    campaign: "La columna campaign de los clics. Vacío en filas del sitio.",
    cost: "0 si el número no parsea. 0 en filas del sitio.",
    revenue: "Solo en purchase y signup, desde un número JSON o un string numérico. Si no parsea, 0.",
    adMeta: "La clave ad_group o ad_set recibe la columna campaign (índice 4). La columna ad_group o ad_set (índice 5) no se lee. También se guardan gclid o fbclid.",
    siteMeta: "Si el texto no es JSON, el campo queda {}.",
    device: "Entra al hash y no tiene columna propia.",
    multi: "Conversores en las dos plataformas",
    users: "Usuarios",
    events: "Eventos",
    clicks: "Clics",
    views: "Vistas",
    conversions: "Conversiones",
    googleCost: "Costo Google Ads",
    facebookCost: "Costo Facebook Ads",
    websiteCost: "Costo del sitio",
    revenueLabel: "Ingreso",
    skippedNone: "Esta ejecución no omitió filas.",
    sqlEmpty: "sql.js no devolvió una tabla. Un SELECT sin filas también termina así. Si el texto era INSERT, UPDATE o DELETE, el cambio ya está en la base de esta carga.",
    sqlRows: "Resultado {n}, {rows} filas.",
    zeroRows: "0 filas.",
    ledgerSql: "Usuarios y eventos: etl.wasm. Totales y conversores: sql.js sobre esas filas. Conversores ejecuta bonus_query.sql del repositorio, no el texto editado.",
    ledgerGo: "sql.js no pudo calcular los totales. Las cifras son las del binario Go.",
    axis: "Suma de cost",
  },
  en: {
    skip: "Skip to the result",
    title: "Marketing pipeline in the browser",
    lede: "This page runs this repository's ETL, compiled to WebAssembly, on google_clicks.csv, facebook_clicks.csv, and site_events.csv. The figures below come from that run.",
    runExample: "Run the pipeline on the sample data",
    loading: "Loading etl.wasm and the repository CSVs.",
    resultHeading: "Result of this run",
    ledgerCaption: "Totals appear when etl.wasm and sql.js finish.",
    measure: "Measure",
    value: "Value",
    aggregateToggle: "Show the totals query",
    usersHeading: "Unified users",
    usersNote: "One row per hash. The id follows Go map order, the same rule as the native INSERT, and it changes across processes.",
    usersCaption: "tracked_user produced by runPipeline.",
    eventsHeading: "Events",
    eventsNote: "This table is ordered by event_time, then source. The memory slice keeps the order in which the workers finished.",
    eventsCaption: "event produced by runPipeline.",
    costHeading: "Cost by source",
    costNote: "Horizontal axis: sum of cost. Each bar is the sum of event.cost for that source in this run. The CSVs are synthetic.",
    sqlHeading: "Attribution query",
    presetLabel: "Query",
    presetTotals: "Cost and revenue by source and type",
    presetByUser: "Attribution by user",
    presetBoth: "Users with clicks on both platforms",
    execSql: "Run query",
    queryLabel: "SQL on the tables from this load",
    sqlHint: "Run query executes the text above. The converter count in this run's result stays bonus_query.sql from the repository, even if you edit this box. Ctrl+Enter also runs it.",
    logsHeading: "Skipped rows",
    lineageHeading: "Column lineage",
    lineageIntro: "Each row describes what this repository's code does. It is not a separate attribution model.",
    lineageCaption: "From the CSV field to the stored column.",
    colColumn: "Column",
    colFrom: "Taken from",
    colRule: "Rule",
    archHeading: "Architecture",
    archIntro: "Path in the browser. go run . calls processGoogleClicks, processFacebookClicks, and processSiteEvents, which call these same ingest functions, then loadToDatabase writes marketing_data.db. The native diagram is in the README.",
    csvHeading: "CSVs for this run",
    csvIntro: "The button at the top reads the repository files again. Here you can paste other text or load a file and run that text. The sample CSVs store action_details with escaped quotes, so the JSON does not parse, metadata stays {}, and revenue stays 0. A valid JSON object in that column makes revenue come from value.",
    runEdited: "Run the pipeline on this text",
    restore: "Restore the repository CSVs",
    limitsHeading: "What changes in the browser",
    limit1: "modernc.org/sqlite v1.34.5 is not linked into etl.wasm. With GOOS=js GOARCH=wasm, modernc.org/libc excludes errno, pthread, signal, stdio, sys/types, time, and unistd, because those files do not declare that GOOS. go build for that pair rejects the import. This page therefore does not open marketing_data.db. The rows come from the same ingest code, compiled with the standard Go toolchain, and the query runs in sql.js 1.11.0, which is SQLite compiled to WebAssembly.",
    limit2: "Each run builds a new database. A second go run . in the terminal appends another copy to the file. There is no file to keep here.",
    limit3: "The goroutines and mutexes are the program's. In the browser the Go runtime schedules them on one JavaScript thread, so sess-006's utm_channel can repeat on every reload even when a native process changes it.",
    limit4: "tracked_user_id comes from map iteration order. That is the same rule as the native INSERT, and it is not stable across processes.",
    limit5: "An ad row that is too short panics on an index. This binary records it under Skipped rows and continues. go run . aborts. The sample CSVs do not hit that case. Their action_details values are not valid JSON either, so the four website rows keep metadata {} and revenue 0.",
    colophon: "Text is set in IBM Plex Sans and IBM Plex Mono, by IBM, under the SIL Open Font License. IBM Plex has tabular figures, which a cost table needs. Blue #1C4E89 is the blue of carbon paper: this page is a copy of the native pipeline, with the same transform code and the same query.",
    hash: "hex(SHA-256(session_key + device_type)), with no separator. session_key ab with device_type c hashes the same as session_key a with device_type bc.",
    userId: "Written only while the stored value is still empty.",
    channel: "Kept from the first worker that inserts the hash.",
    attributes: "The literal {}.",
    window: "Minimum and maximum. The comparison is text in the shape YYYY-MM-DD HH:MM:SS.",
    eventType: "purchase and signup are stored as conversion. Any other action is stored unchanged.",
    source: "Google Ads, Facebook Ads, or Website, depending on the worker.",
    campaign: "The campaign column on clicks. Empty on website rows.",
    cost: "0 when the number does not parse. 0 on website rows.",
    revenue: "Only for purchase and signup, from a JSON number or a numeric string. Unparseable values contribute 0.",
    adMeta: "The ad_group or ad_set key receives the campaign column (index 4). The ad_group or ad_set column (index 5) is not read. gclid or fbclid is stored too.",
    siteMeta: "When the text is not JSON, the field stays {}.",
    device: "It feeds the hash and has no column of its own.",
    multi: "Converters on both platforms",
    users: "Users",
    events: "Events",
    clicks: "Clicks",
    views: "Views",
    conversions: "Conversions",
    googleCost: "Google Ads cost",
    facebookCost: "Facebook Ads cost",
    websiteCost: "Website cost",
    revenueLabel: "Revenue",
    skippedNone: "This run skipped no rows.",
    sqlEmpty: "sql.js returned no table. A SELECT with no rows ends the same way. An INSERT, UPDATE, or DELETE has already changed this load's database.",
    sqlRows: "Result {n}, {rows} rows.",
    zeroRows: "0 rows.",
    ledgerSql: "Users and events: etl.wasm. Totals and converters: sql.js on those rows. Converters runs bonus_query.sql from the repository, not the edited text.",
    ledgerGo: "sql.js could not compute the totals. The figures are from the Go binary.",
    axis: "Sum of cost",
  },
};

const state = {
  lang: "es",
  ready: false,
  busy: false,
  booting: false,
  bonusSQL: "",
  samples: null,
  result: null,
  totals: null,
  bonusValue: null,
  sqlFailed: false,
  db: null,
  SQL: null,
  wasmBytes: 0,
  wasmFetchMs: 0,
  wasmCompileMs: 0,
  runMs: 0,
  sqlMs: 0,
  agreement: [],
  lastSql: null,
  build: null,
};

function t(key) {
  return copy[state.lang][key];
}

function fmt(value) {
  const number = Number(value);
  if (!Number.isFinite(number)) return value == null ? "" : String(value);
  if (Object.is(number, -0)) return "0";
  if (Number.isInteger(number)) return String(number);
  return number.toFixed(2);
}

function setStatus(text) {
  document.getElementById("status").textContent = text;
}

function applyStatic() {
  document.documentElement.lang = state.lang;
  document.querySelectorAll("[data-i18n]").forEach((node) => {
    const key = node.getAttribute("data-i18n");
    if (copy[state.lang][key] != null) node.textContent = copy[state.lang][key];
  });
  document.querySelectorAll("[data-i18n-option]").forEach((node) => {
    const key = node.getAttribute("data-i18n-option");
    node.textContent = copy[state.lang][key];
  });
  document.getElementById("lang-es").setAttribute("aria-pressed", state.lang === "es" ? "true" : "false");
  document.getElementById("lang-en").setAttribute("aria-pressed", state.lang === "en" ? "true" : "false");
  const fileWord = state.lang === "es" ? "Cargar un archivo sobre" : "Load a file into";
  for (const [id, name] of [
    ["google", "google_clicks.csv"],
    ["facebook", "facebook_clicks.csv"],
    ["site", "site_events.csv"],
  ]) {
    document.getElementById("file-" + id).setAttribute("aria-label", fileWord + " " + name);
  }
  document.title = t("title");
}

function setLang(lang) {
  state.lang = lang;
  try {
    localStorage.setItem("etl-lang", lang);
  } catch (err) {
    /* private mode */
  }
  applyStatic();
  renderLineage();
  if (state.result) {
    state.agreement = compare(state.result, state.totals, state.bonusValue);
    renderResult();
    if (state.lastSql) renderSql(state.lastSql);
  }
}

function setBusy(busy) {
  state.busy = busy;
  const locked = busy || !state.ready;
  for (const id of ["run-edited", "restore", "exec-sql"]) {
    document.getElementById(id).disabled = locked;
  }
  document.getElementById("run-example").disabled = busy;
}

async function fetchText(url) {
  const response = await fetch(url);
  if (!response.ok) throw new Error(url + " " + response.status);
  return response.text();
}

function fillEditors(samples) {
  document.getElementById("csv-google").value = samples.google;
  document.getElementById("csv-facebook").value = samples.facebook;
  document.getElementById("csv-site").value = samples.site;
}

function editors() {
  return {
    google: document.getElementById("csv-google").value,
    facebook: document.getElementById("csv-facebook").value,
    site: document.getElementById("csv-site").value,
  };
}

async function loadSamples() {
  const [google, facebook, site] = await Promise.all([
    fetchText("data/google_clicks.csv"),
    fetchText("data/facebook_clicks.csv"),
    fetchText("data/site_events.csv"),
  ]);
  state.samples = { google, facebook, site };
  return state.samples;
}

function reloadDatabase(result) {
  if (state.db) state.db.close();
  const db = new state.SQL.Database();
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
  for (const user of result.users) {
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
  for (const event of result.events) {
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
  state.db = db;
}

function execRaw(sql) {
  const text = sql.trim();
  if (!text) return { ok: true, sets: [] };
  const semicolon = text.indexOf(";");
  const single = semicolon === -1 || semicolon === text.length - 1;
  if (single) {
    try {
      const stmt = state.db.prepare(text);
      const columns = stmt.getColumnNames();
      const values = [];
      while (stmt.step()) values.push(stmt.get());
      stmt.free();
      if (!columns.length && !values.length) return { ok: true, sets: [] };
      return { ok: true, sets: [{ columns, values }] };
    } catch (err) {
      return { ok: false, error: err && err.message ? err.message : String(err), sets: [] };
    }
  }
  try {
    return { ok: true, sets: state.db.exec(sql) };
  } catch (err) {
    return { ok: false, error: err && err.message ? err.message : String(err), sets: [] };
  }
}

function rowObject(set) {
  const row = {};
  if (!set || !set.values.length) return null;
  set.columns.forEach((name, index) => {
    row[name] = set.values[0][index];
  });
  return row;
}

function compare(result, totals, bonusValue) {
  if (!totals) {
    return [state.lang === "es" ? "sql.js no devolvió la consulta de totales." : "sql.js did not return the totals query."];
  }
  const summary = result.summary;
  const pairs = [
    ["users", totals.users, summary.users],
    ["events", totals.events, summary.events],
    ["clicks", totals.clicks, summary.clicks],
    ["views", totals.views, summary.views],
    ["conversions", totals.conversions, summary.conversions],
    ["google_clicks", totals.google_clicks, summary.google_clicks],
    ["facebook_clicks", totals.facebook_clicks, summary.facebook_clicks],
    ["website_events", totals.website_events, summary.website_events],
    ["google_cost", totals.google_cost, summary.google_cost],
    ["facebook_cost", totals.facebook_cost, summary.facebook_cost],
    ["website_cost", totals.website_cost, summary.website_cost],
    ["revenue", totals.revenue, summary.revenue],
  ];
  const bad = [];
  for (const [name, sqlValue, goValue] of pairs) {
    if (Math.abs(Number(sqlValue) - Number(goValue)) >= 1e-6) {
      bad.push(name + ": sql.js " + sqlValue + " / go " + goValue);
    }
  }
  if (bonusValue != null && Number(bonusValue) !== summary.multi_channel_converters) {
    bad.push("multi_channel_converters: sql.js " + bonusValue + " / go " + summary.multi_channel_converters);
  }
  if (!bad.length) return [];
  const prefix = state.lang === "es" ? "Desacuerdo entre sql.js y el binario:" : "sql.js and the binary disagree:";
  return [prefix + " " + bad.join("; ") + "."];
}

function ledgerNumbers() {
  if (state.totals && state.bonusValue != null && !state.sqlFailed) {
    return {
      fromSql: true,
      multi: state.bonusValue,
      users: state.totals.users,
      events: state.totals.events,
      clicks: state.totals.clicks,
      views: state.totals.views,
      conversions: state.totals.conversions,
      google_cost: state.totals.google_cost,
      facebook_cost: state.totals.facebook_cost,
      website_cost: state.totals.website_cost,
      revenue: state.totals.revenue,
    };
  }
  const summary = state.result.summary;
  return {
    fromSql: false,
    multi: summary.multi_channel_converters,
    users: summary.users,
    events: summary.events,
    clicks: summary.clicks,
    views: summary.views,
    conversions: summary.conversions,
    google_cost: summary.google_cost,
    facebook_cost: summary.facebook_cost,
    website_cost: summary.website_cost,
    revenue: summary.revenue,
  };
}

function renderLedger() {
  const numbers = ledgerNumbers();
  const table = document.getElementById("ledger");
  table.querySelector("caption").textContent = numbers.fromSql ? t("ledgerSql") : t("ledgerGo");
  const rows = [
    ["multi", numbers.multi, true],
    ["users", numbers.users, false],
    ["events", numbers.events, false],
    ["clicks", numbers.clicks, false],
    ["views", numbers.views, false],
    ["conversions", numbers.conversions, false],
    ["googleCost", numbers.google_cost, false],
    ["facebookCost", numbers.facebook_cost, false],
    ["websiteCost", numbers.website_cost, false],
    ["revenueLabel", numbers.revenue, false],
  ];
  const tbody = table.querySelector("tbody");
  tbody.replaceChildren();
  for (const [key, value, emphasis] of rows) {
    const tr = document.createElement("tr");
    if (emphasis) tr.className = "emphasis";
    const measure = document.createElement("td");
    measure.textContent = t(key);
    const cell = document.createElement("td");
    cell.className = "num";
    cell.textContent = fmt(value);
    tr.append(measure, cell);
    tbody.appendChild(tr);
  }
  document.getElementById("ledger-wrap").hidden = false;
}

function renderUsers() {
  const tbody = document.querySelector("#users tbody");
  tbody.replaceChildren();
  const users = state.result.users.slice().sort((a, b) => {
    return a.session_key.localeCompare(b.session_key) || a.tracked_user_id - b.tracked_user_id;
  });
  for (const user of users) {
    const tr = document.createElement("tr");
    const cells = [
      [String(user.tracked_user_id), "num"],
      [user.user_id, ""],
      [user.session_key, ""],
      [user.utm_channel, ""],
      [user.earliest_time, "time"],
      [user.latest_time, "time"],
      [user.tracking_hash, "hash"],
    ];
    for (const [text, className] of cells) {
      const td = document.createElement("td");
      if (className) td.className = className;
      td.textContent = text;
      tr.appendChild(td);
    }
    tbody.appendChild(tr);
  }
  document.getElementById("users-wrap").hidden = false;
}

function renderEvents() {
  const tbody = document.querySelector("#events tbody");
  tbody.replaceChildren();
  const events = state.result.events.slice().sort((a, b) => {
    return a.event_time.localeCompare(b.event_time) || a.source.localeCompare(b.source) || a.event_id - b.event_id;
  });
  for (const event of events) {
    const tr = document.createElement("tr");
    const cells = [
      [String(event.event_id), "num"],
      [String(event.tracked_user_id), "num"],
      [event.event_time, "time"],
      [event.event_type, ""],
      [event.source, ""],
      [event.campaign, ""],
      [fmt(event.cost), "num"],
      [fmt(event.revenue), "num"],
      [event.metadata, "meta"],
    ];
    for (const [text, className] of cells) {
      const td = document.createElement("td");
      if (className) td.className = className;
      td.textContent = text;
      tr.appendChild(td);
    }
    tbody.appendChild(tr);
  }
  document.getElementById("events-wrap").hidden = false;
}

function renderChart() {
  const numbers = ledgerNumbers();
  const series = [
    ["Google Ads", Number(numbers.google_cost)],
    ["Facebook Ads", Number(numbers.facebook_cost)],
    ["Website", Number(numbers.website_cost)],
  ];
  const max = Math.max(...series.map((item) => item[1]), 0);
  const svg = document.getElementById("cost-chart");
  svg.querySelector("title").textContent = t("costHeading");
  const desc = series.map((item) => item[0] + " " + fmt(item[1])).join(". ") + ". " + t("axis") + ".";
  document.getElementById("cost-desc").textContent = desc;
  [...svg.children].forEach((node) => {
    if (node.tagName.toLowerCase() !== "title" && node.tagName.toLowerCase() !== "desc") node.remove();
  });
  const ns = "http://www.w3.org/2000/svg";
  series.forEach((item, index) => {
    const y = 28 + index * 40;
    const width = max === 0 ? 0 : (item[1] / max) * 360;
    const label = document.createElementNS(ns, "text");
    label.setAttribute("x", "0");
    label.setAttribute("y", String(y));
    label.textContent = item[0];
    const bar = document.createElementNS(ns, "rect");
    bar.setAttribute("class", "bar");
    bar.setAttribute("x", "150");
    bar.setAttribute("y", String(y - 14));
    bar.setAttribute("width", String(Math.max(width, 0)));
    bar.setAttribute("height", "16");
    const value = document.createElementNS(ns, "text");
    value.setAttribute("x", String(158 + width));
    value.setAttribute("y", String(y));
    value.textContent = fmt(item[1]);
    svg.append(label, bar, value);
  });
  const axis = document.createElementNS(ns, "line");
  axis.setAttribute("x1", "150");
  axis.setAttribute("x2", "510");
  axis.setAttribute("y1", "148");
  axis.setAttribute("y2", "148");
  axis.setAttribute("stroke", "#1a1f24");
  axis.setAttribute("stroke-width", "1");
  svg.appendChild(axis);
  const reduce = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
  svg.classList.toggle("animate", !reduce);
  svg.classList.remove("play");
  document.getElementById("cost-figure").hidden = false;
  if (!reduce) {
    requestAnimationFrame(() => {
      requestAnimationFrame(() => svg.classList.add("play"));
    });
  }
}

function countBoth(result) {
  const byUser = new Map();
  for (const event of result.events) {
    if (!byUser.has(event.tracked_user_id)) byUser.set(event.tracked_user_id, []);
    byUser.get(event.tracked_user_id).push(event);
  }
  let count = 0;
  for (const events of byUser.values()) {
    const google = events.some((event) => event.source === "Google Ads" && event.event_type === "click");
    const facebook = events.some((event) => event.source === "Facebook Ads" && event.event_type === "click");
    if (google && facebook) count += 1;
  }
  return count;
}

function repeatedIds(result) {
  const sessions = new Map();
  for (const user of result.users) {
    if (!user.user_id) continue;
    if (!sessions.has(user.user_id)) sessions.set(user.user_id, new Set());
    sessions.get(user.user_id).add(user.session_key);
  }
  const rows = [];
  for (const [id, keys] of sessions) {
    if (keys.size > 1) rows.push({ id, sessions: keys.size });
  }
  rows.sort((a, b) => a.id.localeCompare(b.id));
  return rows;
}

function renderNarrative() {
  const box = document.getElementById("narrative");
  box.replaceChildren();
  const result = state.result;
  const both = countBoth(result);
  const repeats = repeatedIds(result);
  const sess = result.summary.sess006 || [];
  const goMulti = result.summary.multi_channel_converters;
  const lines = [];
  if (state.lang === "es") {
    if (state.bonusValue == null) {
      lines.push("bonus_query.sql no devolvió multi_channel_converters en esta carga.");
    } else {
      lines.push("En esta ejecución, " + both + " usuarios unificados tienen un clic de Google Ads y un clic de Facebook Ads. bonus_query.sql devolvió " + fmt(state.bonusValue) + ".");
    }
    lines.push("La identidad es session_key concatenado con device_type. El mismo user_id en otra sesión es otra fila, y la consulta no la junta.");
    if (repeats.length) {
      lines.push("user_id en más de una sesión: " + repeats.map((row) => row.id + " (" + row.sessions + ")").join(", ") + ".");
    }
    if (sess.length === 1) {
      lines.push("sess-006 quedó con utm_channel " + sess[0].utm_channel + ". Lo escribe el primer worker que crea ese hash.");
    } else if (sess.length > 1) {
      lines.push("sess-006 produjo " + sess.length + " hashes en esta carga.");
    }
    lines.push("El mismo predicado dentro del binario Go devolvió " + goMulti + ".");
  } else {
    if (state.bonusValue == null) {
      lines.push("bonus_query.sql did not return multi_channel_converters for this load.");
    } else {
      lines.push("In this run, " + both + " unified users have a Google Ads click and a Facebook Ads click. bonus_query.sql returned " + fmt(state.bonusValue) + ".");
    }
    lines.push("Identity is session_key concatenated with device_type. The same user_id on another session is another row, and the query does not merge it.");
    if (repeats.length) {
      lines.push("user_id on more than one session: " + repeats.map((row) => row.id + " (" + row.sessions + ")").join(", ") + ".");
    }
    if (sess.length === 1) {
      lines.push("sess-006 kept utm_channel " + sess[0].utm_channel + ". The first worker to create that hash writes it.");
    } else if (sess.length > 1) {
      lines.push("sess-006 produced " + sess.length + " hashes in this load.");
    }
    lines.push("The same predicate inside the Go binary returned " + goMulti + ".");
  }
  if (state.bonusValue != null && Number(state.bonusValue) !== goMulti) {
    lines.push(state.lang === "es"
      ? "Esos dos números no coinciden. La tabla de arriba muestra el de sql.js."
      : "Those two numbers differ. The table above shows the sql.js figure.");
  }
  for (const line of lines) {
    const p = document.createElement("p");
    p.textContent = line;
    box.appendChild(p);
  }
}

function renderLogs() {
  const box = document.getElementById("logs");
  box.replaceChildren();
  const logs = state.result.logs || [];
  if (!logs.length) {
    const p = document.createElement("p");
    p.textContent = t("skippedNone");
    box.appendChild(p);
    return;
  }
  const list = document.createElement("ul");
  for (const line of logs) {
    const item = document.createElement("li");
    item.textContent = line;
    list.appendChild(item);
  }
  box.appendChild(list);
}

function renderStatus() {
  const megabytes = (state.wasmBytes / (1024 * 1024)).toFixed(2);
  const parts = [];
  if (state.lang === "es") {
    parts.push("etl.wasm pesa " + megabytes + " MB. La respuesta tardó " + Math.round(state.wasmFetchMs) + " ms y la instancia " + Math.round(state.wasmCompileMs) + " ms. Esta corrida tardó " + state.runMs.toFixed(1) + " ms. sql.js tardó " + state.sqlMs.toFixed(1) + " ms.");
  } else {
    parts.push("etl.wasm is " + megabytes + " MB. The response took " + Math.round(state.wasmFetchMs) + " ms and instantiation took " + Math.round(state.wasmCompileMs) + " ms. This run took " + state.runMs.toFixed(1) + " ms. sql.js took " + state.sqlMs.toFixed(1) + " ms.");
  }
  if (state.build && state.build.go_version) {
    parts.push(state.lang === "es"
      ? "Binario: " + state.build.go_version + ", " + state.build.goos + "/" + state.build.goarch + "."
      : "Binary: " + state.build.go_version + ", " + state.build.goos + "/" + state.build.goarch + ".");
  }
  parts.push(...state.agreement);
  setStatus(parts.join(" "));
}

function renderLineage() {
  const tbody = document.querySelector("#lineage tbody");
  tbody.replaceChildren();
  for (const [column, from, key] of LINEAGE) {
    const tr = document.createElement("tr");
    for (const text of [column, from, t(key)]) {
      const td = document.createElement("td");
      td.textContent = text;
      tr.appendChild(td);
    }
    tbody.appendChild(tr);
  }
}

function renderSql(outcome) {
  state.lastSql = outcome;
  const host = document.getElementById("sql-output");
  host.replaceChildren();
  if (!outcome.ok) {
    const p = document.createElement("p");
    p.textContent = outcome.error;
    host.appendChild(p);
    return;
  }
  if (!outcome.sets.length) {
    const p = document.createElement("p");
    p.textContent = t("sqlEmpty");
    host.appendChild(p);
    return;
  }
  outcome.sets.forEach((set, index) => {
    const table = document.createElement("table");
    const caption = document.createElement("caption");
    caption.textContent = t("sqlRows").replace("{n}", String(index + 1)).replace("{rows}", String(set.values.length));
    table.appendChild(caption);
    const thead = document.createElement("thead");
    const headRow = document.createElement("tr");
    for (const name of set.columns) {
      const th = document.createElement("th");
      th.scope = "col";
      th.textContent = name;
      headRow.appendChild(th);
    }
    thead.appendChild(headRow);
    table.appendChild(thead);
    const tbody = document.createElement("tbody");
    if (!set.values.length) {
      const tr = document.createElement("tr");
      const td = document.createElement("td");
      td.colSpan = Math.max(set.columns.length, 1);
      td.textContent = t("zeroRows");
      tr.appendChild(td);
      tbody.appendChild(tr);
    }
    for (const valueRow of set.values) {
      const tr = document.createElement("tr");
      for (const cell of valueRow) {
        const td = document.createElement("td");
        td.textContent = typeof cell === "number" ? fmt(cell) : cell == null ? "" : String(cell);
        tr.appendChild(td);
      }
      tbody.appendChild(tr);
    }
    table.appendChild(tbody);
    const scroll = document.createElement("div");
    scroll.className = "table-scroll";
    scroll.appendChild(table);
    host.appendChild(scroll);
  });
}

function renderResult() {
  renderLedger();
  renderUsers();
  renderEvents();
  renderChart();
  renderNarrative();
  renderLogs();
  renderStatus();
}

async function execute(samples) {
  setBusy(true);
  try {
    const started = performance.now();
    const raw = runMarketingETL(samples.google, samples.facebook, samples.site);
    state.runMs = performance.now() - started;
    const result = JSON.parse(raw);
    if (result.error) throw new Error(result.error);
    state.result = result;
    const sqlStarted = performance.now();
    reloadDatabase(result);
    const totalsOutcome = execRaw(AGGREGATE);
    const bonusOutcome = execRaw(state.bonusSQL);
    state.sqlMs = performance.now() - sqlStarted;
    state.totals = totalsOutcome.ok ? rowObject(totalsOutcome.sets[0]) : null;
    state.bonusValue = null;
    state.sqlFailed = !totalsOutcome.ok || !bonusOutcome.ok || !state.totals;
    if (bonusOutcome.ok && bonusOutcome.sets[0]) {
      const column = bonusOutcome.sets[0].columns.indexOf("multi_channel_converters");
      if (column >= 0 && bonusOutcome.sets[0].values[0]) {
        state.bonusValue = bonusOutcome.sets[0].values[0][column];
      }
    }
    state.agreement = compare(result, state.totals, state.bonusValue);
    renderResult();
    const editor = document.getElementById("query").value;
    if (editor.trim() === state.bonusSQL.trim()) {
      renderSql(bonusOutcome);
    } else if (editor.trim()) {
      renderSql(execRaw(editor));
    } else {
      renderSql(bonusOutcome);
    }
  } catch (err) {
    setStatus(err && err.message ? err.message : String(err));
  } finally {
    setBusy(false);
  }
}

function runEditorQuery() {
  if (!state.db) return;
  const sql = document.getElementById("query").value;
  renderSql(execRaw(sql));
}

function presetText(key) {
  if (key === "bonus") return state.bonusSQL;
  return QUERIES[key] || "";
}

async function drawDiagram() {
  const node = document.getElementById("diagram");
  const source = node.textContent;
  try {
    const mod = await import("https://cdn.jsdelivr.net/npm/mermaid@11.4.1/dist/mermaid.esm.min.mjs");
    const mermaid = mod.default;
    mermaid.initialize({
      startOnLoad: false,
      securityLevel: "strict",
      theme: "base",
      fontFamily: "IBM Plex Sans",
      themeVariables: {
        fontFamily: "IBM Plex Sans, sans-serif",
        primaryColor: "#e6eef6",
        primaryTextColor: "#1a1f24",
        primaryBorderColor: "#1c4e89",
        lineColor: "#1c4e89",
        secondaryColor: "#f3f5f7",
        tertiaryColor: "#ffffff",
        clusterBkg: "#f3f5f7",
        clusterBorder: "#d5dbe1",
        titleColor: "#1a1f24",
        edgeLabelBackground: "#ffffff",
      },
    });
    const rendered = await mermaid.render("archDiagram", source);
    node.innerHTML = rendered.svg;
  } catch (err) {
    node.textContent = source;
  }
}

function bind() {
  document.getElementById("lang-es").addEventListener("click", () => setLang("es"));
  document.getElementById("lang-en").addEventListener("click", () => setLang("en"));
  document.getElementById("run-example").addEventListener("click", async () => {
    try {
      if (!state.ready) {
        await boot();
        return;
      }
      const samples = await loadSamples();
      fillEditors(samples);
      document.getElementById("preset").value = "bonus";
      document.getElementById("query").value = state.bonusSQL;
      await execute(samples);
    } catch (err) {
      setStatus(err && err.message ? err.message : String(err));
    }
  });
  document.getElementById("run-edited").addEventListener("click", () => execute(editors()));
  document.getElementById("restore").addEventListener("click", async () => {
    const samples = await loadSamples();
    fillEditors(samples);
  });
  document.getElementById("exec-sql").addEventListener("click", runEditorQuery);
  document.getElementById("preset").addEventListener("change", (event) => {
    document.getElementById("query").value = presetText(event.target.value);
    runEditorQuery();
  });
  document.getElementById("query").addEventListener("keydown", (event) => {
    if (event.key === "Enter" && (event.ctrlKey || event.metaKey)) {
      event.preventDefault();
      runEditorQuery();
    }
  });
  document.querySelectorAll("input[type=file]").forEach((input) => {
    input.addEventListener("change", () => {
      const file = input.files && input.files[0];
      if (!file) return;
      const reader = new FileReader();
      reader.onload = () => {
        document.getElementById(input.dataset.target).value = String(reader.result);
        input.value = "";
      };
      reader.readAsText(file);
    });
  });
}

async function boot() {
  if (state.booting) return;
  state.booting = true;
  setBusy(true);
  applyStatic();
  try {
    const wasmPromise = (async () => {
      const started = performance.now();
      const response = await fetch("etl.wasm");
      if (!response.ok) throw new Error("etl.wasm " + response.status);
      const buffer = await response.arrayBuffer();
      return { buffer, ms: performance.now() - started };
    })();
    const [wasm, google, facebook, site, bonus, build] = await Promise.all([
      wasmPromise,
      fetchText("data/google_clicks.csv"),
      fetchText("data/facebook_clicks.csv"),
      fetchText("data/site_events.csv"),
      fetchText("data/bonus_query.sql"),
      fetch("build.json").then((response) => (response.ok ? response.json() : null)).catch(() => null),
    ]);
    state.wasmBytes = wasm.buffer.byteLength;
    state.wasmFetchMs = wasm.ms;
    state.samples = { google, facebook, site };
    state.bonusSQL = bonus;
    state.build = build;
    fillEditors(state.samples);
    document.getElementById("query").value = bonus;
    document.getElementById("aggregate-sql").textContent = AGGREGATE;
    const sqlPromise = initSqlJs({ locateFile: (file) => "vendor/" + file });
    const compileStarted = performance.now();
    const go = new Go();
    const compiled = await WebAssembly.instantiate(wasm.buffer, go.importObject);
    state.wasmCompileMs = performance.now() - compileStarted;
    go.run(compiled.instance);
    if (typeof runMarketingETL !== "function") throw new Error("runMarketingETL");
    state.SQL = await sqlPromise;
    state.ready = true;
    renderLineage();
    await execute(state.samples);
    drawDiagram();
  } catch (err) {
    setStatus(err && err.message ? err.message : String(err));
  } finally {
    state.booting = false;
    setBusy(false);
  }
}

try {
  const saved = localStorage.getItem("etl-lang");
  if (saved === "en" || saved === "es") state.lang = saved;
} catch (err) {
  /* keep Spanish */
}
bind();
boot();
