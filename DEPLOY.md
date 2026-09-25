# Deployment

This directory is a static site. Connect the GitHub repository to Cloudflare Pages and set the Pages project root to `cloudflare_R2`; no Worker or Wrangler deployment is required.

The site reads `https://r2.ybgmbh.com/wide_certificate.parquet` directly. Configure the R2 custom domain to allow `GET`, `HEAD`, and `Range` requests from the Pages site origin, expose `Accept-Ranges`, `Content-Length`, `Content-Range`, and `ETag`, and keep the object publicly readable or authenticated by the R2 domain layer.

The browser uses hyparquet to read the Parquet footer and row groups progressively. Downloaded byte ranges are stored in OPFS under `parquet-cache`; IndexedDB and DuckDB are not used.

Before uploading a replacement dataset, normalize browser-sensitive columns with Polars:

```powershell
python .\prepare_parquet_for_browser.py .\wide_certificate.parquet .\wide_certificate.browser.parquet
```

Upload the generated file to R2 as `wide_certificate.parquet`, then publish the Pages change. The browser cache version in `app.js` must be bumped whenever the object changes.