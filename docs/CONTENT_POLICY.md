# Content Policy

This document defines how `web-crawler` handles fetched resources today.

## Current scope

- HTML pages are first-class crawl targets.
- HTML pages are fetched, stored in `pages.content`, and used for link extraction.
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
- do not persist the full binary body into `pages.content`
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

## Deferred work

- Dedicated PDF extraction is out of scope for now.
- Other binary-specific extractors are also out of scope for now.
- If a content type needs first-class support later, add a dedicated extractor instead of forcing the raw body into the text storage path.
