[![Build](https://github.com/DGEXSolutions/chartos/actions/workflows/docker.yml/badge.svg)](https://github.com/DGEXSolutions/chartos/actions/workflows/docker.yml)

# Usage

```sh
export PSQL_DSN='postgres://user:passwd@localhost/chartos'
export ROOT_URL=http://localhost:8000
export REDIS_URL=redis://localhost
uvicorn --factory chartos:make_app
```
