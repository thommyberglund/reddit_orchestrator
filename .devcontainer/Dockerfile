FROM python:3.12-slim

# Install uv
RUN pip install uv

# Create a non-root user
RUN useradd -m vscode && \
    echo "vscode ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers

WORKDIR /workspace

# Create virtual environment
RUN uv venv /workspace/.venv

# Ensure scripts in .venv are usable
ENV PATH="/workspace/.venv/bin:$PATH"

USER vscode
