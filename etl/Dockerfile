FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_LINK_MODE=copy \
    VIRTUAL_ENV=/opt/venv \
    PATH=/opt/venv/bin:$PATH

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Build the virtualenv OUTSIDE the /workspace bind mount so that runtime
# bind-mounting the host repo on /workspace does not overwrite or pollute
# .venv (the previous layout put .venv inside /workspace/etl, which leaked
# a root-owned .venv onto the host after every `docker compose run`).
WORKDIR /opt/etl-build

# 1) Install third-party deps using only pyproject + uv.lock so this layer
#    caches independently of source changes.
COPY pyproject.toml uv.lock ./
RUN uv sync --no-dev --no-install-project

# 2) Copy the project source and install the project itself into /opt/venv.
COPY src ./src
RUN uv sync --no-dev

# Runtime: start from /workspace (the host bind mount). UV_PROJECT_ENVIRONMENT
# makes `uv run --project /workspace/etl ...` reuse /opt/venv instead of
# creating a .venv next to the bind-mounted pyproject.toml.
WORKDIR /workspace
CMD ["bash"]
