# Deployment

This directory is a static site. Connect the GitHub repository to Cloudflare Pages and set the Pages project root to `cloudflare_R2`; no Worker or Wrangler deployment is required.

The site reads `https://r2.ybgmbh.com/wide_certificate.parquet` directly. Configure the R2 custom domain to allow `GET`, `HEAD`, and `Range` requests from the Pages site origin, expose `Accept-Ranges`, `Content-Length`, `Content-Range`, and `ETag`, and keep the object publicly readable or authenticated by the R2 domain layer.

The browser reads the Parquet footer first, then processes row groups progressively. Downloaded byte ranges are stored in OPFS under `parquet-cache`; IndexedDB is not used.