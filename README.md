# QuestDB REST API Python Client, CLI and REPL Shell

> QuestDB comes with a very nice web console, but there's no CLI, so I wrote one (can't live without the terminal!).

The REST API is very well defined: https://questdb.com/docs/reference/api/rest/, only 3 documented endpoints. One undocumented endpoints I also implemented are `/chk` to check for if a table exists, I found the route when trying to ingest CSV via the web console.

## PyPI packages and installation

`questdb-cli`, `questdb-rest` and `questdb-api` are the same package (just aliases), with `questdb-rest` guaranteed to be the most updated.

Install (Python >=3.11 required):

```bash
uv tool install questdb-rest
```

```bash
pipx install questdb-rest
```

```bash
# not recommended, but if you really want to:
pip install questdb-rest
```


## Docs, screenshots and video demos

- https://teddysc.me/blog/questdb-rest
- https://teddysc.me/blog/rlwrap-questdb-shell