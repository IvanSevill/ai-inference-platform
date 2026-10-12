# Architecture

## Current state

The architecture has not been implemented yet.

The project will evolve incrementally. Every service must have a clear reason
to exist.

## Initial target

The first practical milestone will contain only a few components so Docker
networking, volumes and Compose can be understood before introducing the full
AI platform.

## Principles

- Keep custom application code minimal.
- Prefer stateless services when possible.
- Persist state explicitly.
- Do not expose internal services unnecessarily.
- Introduce observability early enough to inspect system behaviour.
- Design the Docker Compose architecture so that it can later be mapped to
  Kubernetes concepts.
