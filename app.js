import * as duckdb from "https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm/+esm";
import perspective from "https://cdn.jsdelivr.net/npm/@finos/perspective/dist/esm/perspective.js";

const DATA_URL = "wide_certificate.parquet";
const REMOTE_DATA_URL = "https://r2.ybgmbh.com/wide_certificate.parquet";
const DATA_FILE = "dataset.parquet";
const CACHE_VERSION = "wide-certificate-v2";
const OPFS_DIRECTORY = "parquet-cache";
const SEARCH_COLUMN = "holder_name";
const viewer = document.querySelector("#viewer");
const searchInput = document.querySelector("#search");
const statusText = document.querySelector("#status-text");
const statusDot = document.querySelector("#status-dot");
const errorBox = document.querySelector("#error");

function setStatus(message, ready = false) {
  statusText.textContent = message;
  statusDot.classList.toggle("dot-on", ready);
  statusDot.classList.toggle("dot-off", !ready);
}

function showError(error) {
  errorBox.textContent = `Could not load the dataset: ${error.message}`;
  errorBox.classList.remove("hidden");
  setStatus("Load failed");
}

function updateSummary(rows) {
  const countries = new Set(rows.map((row) => row.country).filter(Boolean));
  const active = rows.filter((row) => /active|valid|reissued/i.test(row.License_Status__c ?? row.Cert_Status__c ?? ""));
  document.querySelector("#row-count").textContent = rows.length.toLocaleString();
  document.querySelector("#country-count").textContent = countries.size.toLocaleString();
  document.querySelector("#active-count").textContent = active.length.toLocaleString();
}

function toPlainRow(row) {
  return JSON.parse(JSON.stringify(row.toJSON(), (_key, value) => (
    typeof value === "bigint" ? Number(value) : value
  )));
}

function isLocalPreview() {
  return ["localhost", "127.0.0.1", "::1"].includes(location.hostname);
}

async function openOpfsCache() {
  if (!navigator.storage?.getDirectory) return null;
  const root = await navigator.storage.getDirectory();
  const directory = await root.getDirectoryHandle(OPFS_DIRECTORY, { create: true });
  const file = await directory.getFileHandle(`${CACHE_VERSION}.parquet`, { create: true });
  const manifest = await directory.getFileHandle(`${CACHE_VERSION}.json`, { create: true });
  return { file, manifest };
}

async function readOpfsManifest(cache) {
  if (!cache) return { ranges: [] };
  try {
    return JSON.parse(await (await cache.manifest.getFile()).text());
  } catch {
    return { ranges: [] };
  }
}

async function writeOpfsRange(cache, offset, bytes, manifest) {
  if (!cache) return;
  const writable = await cache.file.createWritable({ keepExistingData: true });
  await writable.seek(offset);
  await writable.write(bytes);
  await writable.close();
  manifest.ranges.push([offset, bytes.byteLength]);
  const manifestWriter = await cache.manifest.createWritable();
  await manifestWriter.write(JSON.stringify({ ...manifest, updatedAt: Date.now() }));
  await manifestWriter.close();
}

async function rangeFetch(url, offset, length, cache, manifest) {
  const response = await fetch(url, {
    headers: { Range: `bytes=${offset}-${offset + length - 1}` },
  });
  if (!response.ok && response.status !== 206) {
    throw new Error(`${url} returned ${response.status}`);
  }
  const bytes = new Uint8Array(await response.arrayBuffer());
  await writeOpfsRange(cache, offset, bytes, manifest);
  return bytes;
}

async function cacheFooter(url, cache, manifest) {
  const head = await fetch(url, { method: "HEAD" });
  if (!head.ok) throw new Error(`${url} returned ${head.status}`);
  const size = Number(head.headers.get("Content-Length"));
  if (!Number.isFinite(size) || size < 8) throw new Error("R2 did not return a valid Parquet size");
  const trailer = await rangeFetch(url, size - 8, 8, cache, manifest);
  const footerLength = new DataView(trailer.buffer, trailer.byteOffset).getUint32(0, true);
  if (footerLength + 8 > size) throw new Error("Invalid Parquet footer length");
  await rangeFetch(url, size - footerLength - 8, footerLength + 8, cache, manifest);
}

async function createDuckDb() {
  const bundles = duckdb.getJsDelivrBundles();
  const bundle = await duckdb.selectBundle(bundles);
  const workerSource = `importScripts(${JSON.stringify(bundle.mainWorker)});`;
  const workerUrl = URL.createObjectURL(new Blob([workerSource], { type: "text/javascript" }));
  const worker = new Worker(workerUrl);
  const logger = new duckdb.ConsoleLogger();
  const db = new duckdb.AsyncDuckDB(logger, worker);
  await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
  return db;
}

async function loadData() {
  const db = await createDuckDb();
  let dataSource = "remote";
  if (isLocalPreview()) {
    const file = await fetch(DATA_URL);
    if (!file.ok) throw new Error(`${DATA_URL} returned ${file.status}`);
    await db.registerFileBuffer(DATA_FILE, new Uint8Array(await file.arrayBuffer()));
    dataSource = "local file";
  } else {
    await db.registerFileURL(DATA_FILE, REMOTE_DATA_URL, duckdb.DuckDBDataProtocol.HTTP, false);
  }

  const cache = await openOpfsCache().catch(() => null);
  const cacheManifest = await readOpfsManifest(cache);
  if (!isLocalPreview()) {
    try {
      await cacheFooter(REMOTE_DATA_URL, cache, cacheManifest);
    } catch (error) {
      console.warn("OPFS footer cache unavailable; continuing with DuckDB", error);
    }
  }
  const connection = await db.connect();
  const metadata = await connection.query(`SELECT * FROM parquet_metadata('${DATA_FILE}')`);
  const metadataRows = metadata.toArray().map(toPlainRow);
  const rowGroups = [...new Map(metadataRows.map((row) => [row.row_group_id, row])).values()]
    .sort((left, right) => left.row_group_id - right.row_group_id);
  const totalRows = rowGroups.reduce((sum, row) => sum + Number(row.row_group_num_rows ?? 0), 0);
  if (totalRows) document.querySelector("#row-count").textContent = totalRows.toLocaleString();
  setStatus(`Metadata ready · ${dataSource} · ${rowGroups.length} row groups`);
  await perspective.init_server(fetch("https://cdn.jsdelivr.net/npm/@finos/perspective/dist/wasm/perspective-server.wasm"));
  const perspectiveWorker = await perspective.worker();
  let perspectiveTable;
  let offset = 0;
  const loadedRows = [];
  for (const [index, rowGroup] of rowGroups.entries()) {
    try {
      const rowCount = Number(rowGroup.row_group_num_rows ?? 0);
      if (!Number.isInteger(rowCount) || rowCount < 1) {
        throw new Error(`row group ${index + 1} has invalid row count: ${rowGroup.row_group_num_rows}`);
      }
      if (!isLocalPreview()) {
        const groupColumns = metadataRows.filter((row) => row.row_group_id === rowGroup.row_group_id);
        const offsets = groupColumns.flatMap((row) => [
          Number(row.file_offset),
          Number(row.data_page_offset),
          Number(row.dictionary_page_offset),
        ]).filter(Number.isFinite);
        const sizes = groupColumns.map((row) => Number(row.total_compressed_size)).filter(Number.isFinite);
        if (offsets.length && sizes.length) {
          const groupStart = Math.min(...offsets);
          const groupEnd = Math.max(...groupColumns.flatMap((row) => {
            const columnOffsets = [
              Number(row.file_offset),
              Number(row.data_page_offset),
              Number(row.dictionary_page_offset),
            ].filter(Number.isFinite);
            return columnOffsets.length && Number.isFinite(Number(row.total_compressed_size))
              ? [Math.min(...columnOffsets) + Number(row.total_compressed_size)]
              : [];
          }));
          if (Number.isFinite(groupEnd) && groupEnd > groupStart) {
            try {
              await rangeFetch(REMOTE_DATA_URL, groupStart, groupEnd - groupStart, cache, cacheManifest);
            } catch (error) {
              console.warn(`OPFS row group ${index + 1} cache unavailable; continuing with DuckDB`, error);
            }
          }
        }
      }
      setStatus(`Reading row group ${index + 1}/${rowGroups.length}…`);
      const groupRows = await connection.query(
        `SELECT * FROM read_parquet('${DATA_FILE}') LIMIT ${rowCount} OFFSET ${offset}`,
      );
      const rows = groupRows.toArray().map(toPlainRow);
      if (rows.length !== rowCount) {
        throw new Error(`row group ${index + 1} returned ${rows.length} of ${rowCount} rows`);
      }
      loadedRows.push(...rows);
      if (!perspectiveTable) {
        perspectiveTable = await perspectiveWorker.table(rows);
        await viewer.load(perspectiveTable);
      } else {
        try {
          await perspectiveTable.update(rows);
        } catch (error) {
          console.warn(`Perspective update failed for row group ${index + 1}; rebuilding table`, error);
          perspectiveTable = await perspectiveWorker.table(loadedRows);
          await viewer.load(perspectiveTable);
        }
      }
      updateSummary(loadedRows);
      offset += rowCount;
      setStatus(`Loaded row group ${index + 1}/${rowGroups.length} · ${offset.toLocaleString()} rows`);
    } catch (error) {
      throw new Error(`row group ${index + 1}/${rowGroups.length} failed: ${error.message}`);
    }
  }
  if (!rowGroups.length) throw new Error("The Parquet file contains no row groups");
  await connection.close();
  await db.terminate();
  setStatus(`Arrow table ready · OPFS has ${cacheManifest.ranges.length} cached ranges`, true);
}

searchInput.addEventListener("input", () => {
  const value = searchInput.value.trim();
  viewer.restore({ filter: value ? [[SEARCH_COLUMN, "contains", value]] : [] });
});

document.querySelector("#reset").addEventListener("click", () => {
  searchInput.value = "";
  viewer.reset();
});

loadData().catch(showError);