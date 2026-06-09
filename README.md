# Roblox Hash Index

A service that indexes the Roblox asset you're getting by hashing and caching them persistently.

It is a clone of [Paradoxum Games' RHI](http://rhi.paradoxum.gg/).

## Build

```bash
git clone https://github.com/t7ru/rhi.git
cd rhi
go build -ldflags="-s -w" .
```

## Run

```bash
./rhi -port 7777 -db hashbrown.db
```

## Flags

| Flag             | Default     | Description                   |
| ---------------- | ----------- | ----------------------------- |
| `-port`          | `7771`      | Server listen port            |
| `-db`            | `hashes.db` | BoltDB cache file path        |
| `-timeout`       | `30s`       | HTTP client timeout           |
| `-read-timeout`  | `5s`        | Server read timeout           |
| `-write-timeout` | `30s`       | Server write timeout          |
| `-idle-timeout`  | `120s`      | Server idle timeout           |
| `-max-idle`      | `77`        | Max idle connections per host |
| `-nosync`        | `false`     | Skip fsync on BoltDB writes   |

## Endpoints

- `GET /{assetID}` -> SHA256 hash of the asset
