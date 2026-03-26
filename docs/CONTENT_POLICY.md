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
- Office documents
- Archives
- Other binary payloads that are not safe or useful to store as page text

For these resources, the target behavior is:

- keep the URL as a valid discovered page
- record fetch metadata such as status, URL, timestamps, and content length
- do not persist the full binary body into `pages.content`
- do not treat the resource as an extracted-link source

## Classification rule

- Use `Content-Type` as the primary signal.
- Use URL suffixes only as a fallback or secondary hint.
- When the payload is clearly binary, prefer metadata-only handling.

## Deferred work

- Dedicated PDF extraction is deferred.
- Other binary-specific extractors are also deferred.
- If a content type needs first-class support later, add a dedicated extractor instead of forcing the raw body into the text storage path.
