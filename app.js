import perspective from "https://cdn.jsdelivr.net/npm/@finos/perspective/dist/esm/perspective.js";
import { parquetMetadataAsync, parquetReadObjects } from "https://cdn.jsdelivr.net/npm/hyparquet@1.31.1/+esm";
import { compressors } from "https://cdn.jsdelivr.net/npm/hyparquet-compressors@1.1.2/+esm";

const DATA_URL = "wide_certificate.parquet";
const REMOTE_DATA_URL = "https://r2.ybgmbh.com/wide_certificate.parquet";
const DATA_FILE = "dataset.parquet";
const CACHE_VERSION = "wide-certificate-v3";
const OPFS_DIRECTORY = "parquet-cache";
const SEARCH_COLUMN = "_search_text";
const SEARCH_FIELDS = ["holder_name", "Certificate_Number__c", "Full_Certificate_Code__c", "species_list", "product_list"];
const FILTER_FIELDS = [
  ["filter-cb", "CB__c"],
  ["filter-ctype", "Ctype"],
  ["filter-status", "Cert_Status__c"],
  ["filter-certificate-type", "Certificate_Type__c"],
  ["filter-forest-type", "Forest_Type__c"],
  ["filter-country", "country"],
];
const viewer = document.querySelector("#viewer");
const searchInput = document.querySelector("#search");
const loadAllButton = document.querySelector("#load-all");
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

function rowForPerspective(row) {
  const values = { ...row };
  values[SEARCH_COLUMN] = SEARCH_FIELDS.map((field) => values[field] ?? "").join(" ");
  return values;
}

function updateFilterOptions(rows) {
  for (const [elementId, field] of FILTER_FIELDS) {
    const select = document.querySelector(`#${elementId}`);
    const selected = select.value;
    const values = [...new Set(rows.map((row) => String(row[field] ?? "")).filter(Boolean))].sort();
    select.replaceChildren(new Option(select.options[0].textContent, ""));
    for (const value of values) select.add(new Option(value, value));
    select.value = values.includes(selected) ? selected : "";
  }
}

function applyFilters() {
  if (!window.loadedPerspectiveViewer) return;
  const filters = FILTER_FIELDS
    .map(([elementId, field]) => [field, document.querySelector(`#${elementId}`).value])
    .filter(([_field, value]) => value)
    .map(([field, value]) => [field, "==", value]);
  const search = searchInput.value.trim();
  if (search) filters.push([SEARCH_COLUMN, "contains", search]);
  window.loadedPerspectiveViewer.restore({ filter: filters });
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
  let nextGroup = 0;

  async function loadNextGroup() {
    const index = nextGroup;
    const rowGroup = rowGroups[index];
    if (!rowGroup) return false;
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
      const perspectiveRows = rows.map(rowForPerspective);
      loadedRows.push(...perspectiveRows);
      if (!perspectiveTable) {
        perspectiveTable = await perspectiveWorker.table(perspectiveRows);
        await viewer.load(perspectiveTable);
        await viewer.restore({
          columns: Object.keys(perspectiveRows[0]).filter((column) => column !== SEARCH_COLUMN),
        });
        window.loadedPerspectiveViewer = viewer;
      } else {
        try {
          await perspectiveTable.update(perspectiveRows);
        } catch (error) {
          console.warn(`Perspective update failed for row group ${index + 1}; rebuilding table`, error);
          perspectiveTable = await perspectiveWorker.table(loadedRows);
          await viewer.load(perspectiveTable);
        }
      }
      updateSummary(loadedRows);
      updateFilterOptions(loadedRows);
      offset += rowCount;
      nextGroup += 1;
      setStatus(`Loaded row group ${index + 1}/${rowGroups.length} · ${offset.toLocaleString()} rows`);
      if (nextGroup === rowGroups.length) loadAllButton.disabled = true;
      return true;
    } catch (error) {
      throw new Error(`row group ${index + 1}/${rowGroups.length} failed: ${error.message}`);
    }
  }
  if (!rowGroups.length) throw new Error("The Parquet file contains no row groups");
  await loadNextGroup();
  setStatus(`Ready · 1/${rowGroups.length} row groups loaded`, true);
  loadAllButton.addEventListener("click", async () => {
    loadAllButton.disabled = true;
    try {
      while (await loadNextGroup()) {}
      setStatus(`Arrow table ready · ${offset.toLocaleString()} rows`, true);
    } catch (error) {
      showError(error);
    }
  }, { once: true });
}

searchInput.addEventListener("input", () => {
  applyFilters();
});

for (const [elementId] of FILTER_FIELDS) {
  document.querySelector(`#${elementId}`).addEventListener("change", applyFilters);
}

document.querySelector("#reset").addEventListener("click", () => {
  searchInput.value = "";
  for (const [elementId] of FILTER_FIELDS) document.querySelector(`#${elementId}`).value = "";
  applyFilters();
});

loadData().catch(showError);