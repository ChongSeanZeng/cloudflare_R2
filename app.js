import perspective from "https://cdn.jsdelivr.net/npm/@finos/perspective/dist/esm/perspective.js";
import { parquetMetadataAsync, parquetReadObjects } from "https://cdn.jsdelivr.net/npm/hyparquet@1.31.1/+esm";
import { compressors } from "https://cdn.jsdelivr.net/npm/hyparquet-compressors@1.1.2/+esm";

const DATA_URL = "wide_certificate.parquet";
const REMOTE_DATA_URL = "https://r2.ybgmbh.com/wide_certificate.parquet";
const DATA_FILE = "dataset.parquet";
const CACHE_VERSION = "wide-certificate-v3";
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
  const source = typeof row.toJSON === "function" ? row.toJSON() : row;
  const normalize = (value) => {
    if (value == null || typeof value === "string" || typeof value === "boolean") return value ?? null;
    if (typeof value === "bigint") return Number(value);
    if (typeof value === "number") return Number.isFinite(value) ? value : null;
    if (value instanceof Date) return value.toISOString();
    if (ArrayBuffer.isView(value)) return JSON.stringify(Array.from(value, normalize));
    if (Array.isArray(value)) return JSON.stringify(value.map(normalize));
    if (typeof value === "object") {
      return JSON.stringify(Object.fromEntries(Object.entries(value).map(([key, item]) => [key, normalize(item)])));
    }
    return String(value);
  };
  return Object.fromEntries(Object.entries(source).map(([key, value]) => [key, normalize(value)]));
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

async function readCachedRange(cache, offset, length, manifest) {
  if (!cache) return null;
  const match = manifest.ranges.find(([cachedOffset, cachedLength]) => (
    cachedOffset <= offset && cachedOffset + cachedLength >= offset + length
  ));
  if (!match) return null;
  const file = await cache.file.getFile();
  return new Uint8Array(await file.slice(offset, offset + length).arrayBuffer());
}

async function rangeFetch(url, offset, length, cache, manifest) {
  const cached = await readCachedRange(cache, offset, length, manifest);
  if (cached) return cached;
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

async function createParquetBuffer(url, cache, manifest) {
  const head = await fetch(url, { method: "HEAD" });
  if (!head.ok) throw new Error(`${url} returned ${head.status}`);
  const size = Number(head.headers.get("Content-Length"));
  if (!Number.isFinite(size) || size < 8) throw new Error("R2 did not return a valid Parquet size");
  return {
    byteLength: size,
    slice: async (start, end = size) => {
      const bytes = await rangeFetch(url, start, end - start, cache, manifest);
      return bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength);
    },
  };
}

async function loadData() {
  let dataSource = "remote";
  let parquetFile;
  if (isLocalPreview()) {
    const file = await fetch(DATA_URL);
    if (!file.ok) throw new Error(`${DATA_URL} returned ${file.status}`);
    const buffer = await file.arrayBuffer();
    parquetFile = { byteLength: buffer.byteLength, slice: (start, end) => buffer.slice(start, end) };
    dataSource = "local file";
  } else {
    const cache = await openOpfsCache().catch(() => null);
    const cacheManifest = await readOpfsManifest(cache);
    parquetFile = await createParquetBuffer(REMOTE_DATA_URL, cache, cacheManifest);
    const metadata = await parquetMetadataAsync(parquetFile);
    await startPerspectiveLoad(parquetFile, metadata, cache, cacheManifest, dataSource);
    return;
  }
  const metadata = await parquetMetadataAsync(parquetFile);
  await startPerspectiveLoad(parquetFile, metadata, null, { ranges: [] }, dataSource);
}

async function startPerspectiveLoad(parquetFile, metadata, cache, cacheManifest, dataSource) {
  const rowGroups = metadata.row_groups ?? [];
  const totalRows = Number(metadata.num_rows ?? 0);
  if (totalRows) document.querySelector("#row-count").textContent = totalRows.toLocaleString();
  setStatus(`Metadata ready · ${dataSource} · ${rowGroups.length} row groups`);
  await perspective.init_server(fetch("https://cdn.jsdelivr.net/npm/@finos/perspective/dist/wasm/perspective-server.wasm"));
  const perspectiveWorker = await perspective.worker();
  let perspectiveTable;
  let offset = 0;
  const loadedRows = [];
  for (const [index, rowGroup] of rowGroups.entries()) {
    try {
      const rowCount = Number(rowGroup.num_rows ?? 0);
      if (!Number.isInteger(rowCount) || rowCount < 1) {
        throw new Error(`row group ${index + 1} has invalid row count: ${rowGroup.num_rows}`);
      }
      setStatus(`Reading row group ${index + 1}/${rowGroups.length}…`);
      const rows = (await parquetReadObjects({
        file: parquetFile,
        rowStart: offset,
        rowEnd: offset + rowCount,
        compressors,
      })).map(toPlainRow);
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