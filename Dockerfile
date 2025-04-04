FROM python:3.12

# https://docs.astral.sh/uv/guides/integration/docker/#installing-uv
# install uv by copying the binary from the official distroless Docker image
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Copy the project into the image
ADD . /app

# Sync the project into a new environment, using the frozen lockfile
WORKDIR /app
RUN uv sync --frozen

# https://docs.astral.sh/uv/guides/integration/docker/#using-the-environment
# activate the project virtual environment by placing its binary directory at the front of the path
ENV PATH="/app/.venv/bin:$PATH"
