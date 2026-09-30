# Two stages: the web bundle is built with Node and copied into the Python
# image, so the runtime image carries no Node toolchain and no node_modules.
#
# The API serves the built assets from package data (`tally/resources/web`),
# which is why the bundle is copied into the package rather than into an
# arbitrary path -- the app resolves it with importlib.resources, not relative
# to the working directory.

# ---------------------------------------------------------------- web build
FROM node:20-alpine AS web

WORKDIR /web

# The registry is pinned explicitly. A lockfile generated behind a corporate
# mirror pins every dependency to an internal host, and the failure inside a
# container with no route to that host is an eight-minute hang that appears to
# blame npm itself.
COPY web/package.json web/package-lock.json web/.npmrc ./
RUN npm ci --registry=https://registry.npmjs.org/

COPY web/ ./
COPY src/tally/resources /src/tally/resources
RUN npm run build

# ---------------------------------------------------------------- runtime
FROM python:3.12-slim AS runtime

# PYTHONDONTWRITEBYTECODE: no .pyc in a layer that is read-only anyway.
# PYTHONUNBUFFERED: logs appear in `docker logs` as they happen, not at exit --
# which matters most precisely when the container is dying.
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Dependency metadata first: this layer is rebuilt only when dependencies
# change, not on every source edit.
COPY pyproject.toml README.md ./
COPY src/ ./src/
RUN pip install --no-cache-dir .

# The built bundle, overwriting whatever was in the source tree.
COPY --from=web /src/tally/resources/web /usr/local/lib/python3.12/site-packages/tally/resources/web

# Runs as a non-root user. The application never writes to the filesystem, so
# it does not need to own anything it runs from.
RUN useradd --create-home --uid 10001 tally
USER tally

EXPOSE 8080

# Defaults point at the compose service names. Every one is overridable, and
# nothing here is a secret: the Postgres password must be supplied at run time.
ENV TALLY_API_HOST=0.0.0.0 \
    TALLY_API_PORT=8080 \
    TALLY_KAFKA_BOOTSTRAP_SERVERS=redpanda:9092 \
    TALLY_PG_HOST=postgres \
    TALLY_PG_PORT=5432 \
    TALLY_REDIS_HOST=redis \
    TALLY_REDIS_PORT=6379

HEALTHCHECK --interval=10s --timeout=3s --start-period=15s --retries=5 \
    CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8080/v1/health', timeout=2).status == 200 else 1)"

CMD ["uvicorn", "tally.api.app:app", "--host", "0.0.0.0", "--port", "8080"]
