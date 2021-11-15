<a href="https://github.com/romshark/eventlog/actions?query=workflow%3ACI">
    <img src="https://github.com/romshark/eventlog/workflows/CI/badge.svg" alt="GitHub Actions: CI">
</a>
<a href="https://coveralls.io/github/romshark/eventlog">
    <img src="https://coveralls.io/repos/github/romshark/eventlog/badge.svg" alt="Coverage Status" />
</a>
<a href="https://goreportcard.com/report/github.com/romshark/eventlog">
    <img src="https://goreportcard.com/badge/github.com/romshark/eventlog" alt="GoReportCard">
</a>
<a href="https://pkg.go.dev/github.com/romshark/eventlog">
    <img src="https://godoc.org/github.com/romshark/eventlog?status.svg" alt="GoDoc">
</a>

# eventlog

## HTTP API

### Pushing events

A new event may be pushed through the `POST /log/` endpoint.
The request body is expected to contain either one or a series of objects in the following binary format:

```
1. labelLength Uint16 LE
2. payloadLength Uint32 LE
3. [label utf-8] // Not empty if labelLength > 0
4. payload utf-8
```

The document `{"version":"v","version-previous":"vp","time":"t"}` will be returned in case of success where `v` is the version the new event was written at, `vp` the version of the event it was appended on and `t` the time the event was recorded at. If multiple events have been appended then `"version-first":vf` will be added to the returned document where `vf` is the version of the first appended event while `v` is the version of the latest appended event.

### Pushing events transactionally (OCC)

A new event may be pushed transactionally following the [Optimistic Concurrency Control (OCC)](https://en.wikipedia.org/wiki/Optimistic_concurrency_control) principle through the `POST /log/:assumedVersion` endpoint where `assumedVersion` is the version of the current projection.

In case of a mismatch between the provided and actual versions `400 ErrMismatchingVersions` will be returned.

The request body is expected to be similar to the non-transactional push endpoint.
In case of success the returned result document will be similar to the one of the non-transactional push endpoint.

### Reading current version

The `GET /version` endpoint returns `{"version":"x"}` where `x` is a hexadecimal version number representing the current database version.

### Reading initial version

The `GET /version/initial` endpoint returns `{"version-initial":"x"}` where `x` is a hexadecimal version number representing the initial database version (it's the version the very first event was recorded at).


### Scanning events

The `GET /log/:version` endpoint returns an array of `{"time":"t","version":"v","version-previous":"vp","version-next":"vn","label":"l","payload":p}` event object where `t` is the time the event was recorded at, `v` is the version the event was recorded at, `vp` is the version of the antecedent event, `vn` is the version of the successive event, `l` is the event's label and `p` is the JSON payload document.

- Optionally, the `n` query parameter can be applied to limit the scan batch size.
- Optionally, the `reverse` query parameter can be applied to scan in reversed order.
- Optionally, the `skip_first` query parameter can be applied to skip the first event.


### Fetching metadata

The `// GET /meta` endpoint returns all metadata as a JSON document.

### Subscribing for updates

Websocket connections can be established on the `GET /subscription` endpoint.
Connected websockets will receive hexadecimal numbers representing the latest database version when a new event was pushed.
