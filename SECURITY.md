# Security Policy

## Supported Status

This project is under active development and should be treated as experimental for broad public
web crawling. Security fixes target the `main` branch.

## Reporting

Do not publish exploit details in a public issue before the maintainer has had time to respond.
Report suspected vulnerabilities through GitHub private vulnerability reporting when available, or
open a minimal public issue that asks for a private contact path without including secrets or
working exploit payloads.

## Security Model

The crawler is intended to contact the broad public web, not private networks. Application-layer
egress guards reject unsupported schemes, loopback, private, link-local, multicast, reserved,
unspecified, metadata, and unresolved destinations before fetches.

These guards are defense in depth, not the whole boundary. Production deployments should also use
network-layer egress controls to block private CIDRs, loopback, link-local ranges, metadata
endpoints, and internal service networks from the container or host.

The REST API is an internal operator API. Keep `CRAWLER_API_TOKEN` set, bind the API to localhost
or a private network, and put TLS/authentication at the access layer if exposing it beyond the
host.

The AI browser agent is experimental. It requires explicit CLI acknowledgement and should not be
enabled in production automation without separate network containment.
