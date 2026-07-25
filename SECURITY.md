# Security

## Reporting a vulnerability

Email fuadelizade6@gmail.com. Please do not open a public issue for anything
that could be used against a running database before there is a fix.

Include what you need to make the problem reproducible: the version or commit,
the platform, and the smallest sequence of statements or client behaviour that
triggers it. A failing test is ideal but not required.

Expect an acknowledgement within a week. If the report is valid you will get
an assessment and a rough timeline, and credit in the release notes unless you
would rather not be named.

## What counts

QuantaDB is experimental and has no authentication yet, so anything reachable
on its listening ports is by design reachable by anyone who can connect. That
is documented, not a vulnerability.

Reports that are in scope:

- data loss or corruption from a crash the durability rules say is survivable
- a client that can read or write data outside its own transaction's snapshot
- a query, statement, or protocol frame that panics, hangs, or exhausts memory
  in a way a bounded server should not allow
- anything that lets a client escape the server process

Reports that are out of scope for now, because they are known:

- no authentication, authorisation, or TLS on any listener
- the HTTP bridge trusting its callers, which is why it is off by default
- denial of service from a client that simply sends a lot of legitimate work

## Supported versions

There is one supported version: the current `main`. QuantaDB is pre-1.0 and
carries no support commitment for older commits.
