# Content Policy

This document defines how `web-crawler` handles fetched resources today.

## Current scope

- HTML pages are first-class crawl targets.
- HTML pages are fetched, stored through tiered page content storage, and used for link extraction.
- Text-like resources may also be stored when they can be represented safely as text.
- Binary documents remain valid crawl targets, but they are not first-class stored content yet.

## Metadata-only resources

The intended metadata-only category includes:

- PDF documents
- Images
- Audio and video streams
- Office documents
- Archives
- Other binary payloads that are not safe or useful to store as page text

For these resources, the target behavior is:

- keep the URL as a valid discovered page
- record fetch metadata such as status, URL, timestamps, and content length
- do not persist the full binary body into page content storage
- do not treat the resource as an extracted-link source
- do not read the response body when headers already prove the resource is metadata-only

## Classification rule

- Use `Content-Type` as the primary signal.
- Use URL suffixes only as a fallback or secondary hint.
- When the payload is clearly binary, prefer metadata-only handling.
- Treat oversized responses as metadata-only instead of spending a worker slot on large body reads.
- Bound body reads by both byte count and elapsed time.

## Fetch admission

Fetch admission happens after response headers are available and before the body is read.
This is intentionally earlier than parse or storage policy.

The crawler should read bodies for HTML and safe text-like resources. It should not read bodies for
binary payloads, media streams, archives, fonts, images, videos, or responses that exceed the
configured body-size limit.

A metadata-only resource is a successful crawl result. It should be finalized in the scheduler as
done, not counted as a fetch error.

## Page content storage

`pages` is the lightweight page index. Stored text content lives in `page_content` and is limited by
storage tier:

- `metadata_only`: no stored text body
- `summary`: small sample for low-value or oversized text
- `standard`: normal page text budget
- `extended`: larger budget for high-discovery-value text

`content_length` records the original response size when known. `stored_content_bytes` records the
actual bytes persisted. `content_truncated` tells operators whether the stored text is only a
sample. This keeps the crawler from becoming an unbounded page-body archive.

## Discovery breadth

Extracting links does not mean admitting every link. Each page applies bounded discovery:

- ignore links below the minimum discovery value
- cap total admitted links per page
- cap admitted links per target host per page

`outlink_count` records the number of extracted links. `stored_outlink_count` and `outlinks` record
the bounded set kept for scheduler admission and API inspection.

## Deferred work

- Dedicated PDF extraction is out of scope for now.
- Other binary-specific extractors are also out of scope for now.
- If a content type needs first-class support later, add a dedicated extractor instead of forcing the raw body into the text storage path.
