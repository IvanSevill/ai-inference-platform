# AI Inference Platform — Project Context

## Goal

Build a small distributed AI inference platform whose main purpose is learning container infrastructure, Docker Compose, CI/CD and Kubernetes.

The focus is architecture, deployment and orchestration, not application development.

Application code should remain deliberately simple whenever possible.

## Learning objectives

The project should provide practical experience with:

- Docker images and containers
- Docker Compose
- Docker networks
- Persistent volumes and bind mounts
- Service discovery
- Reverse proxies / gateways
- Stateless vs stateful services
- Health checks
- Resource limits
- Horizontal scaling
- Observability
- GitHub Actions
- Container registries
- CI/CD
- Kubernetes Deployments
- Kubernetes Services
- Persistent storage
- ConfigMaps and Secrets
- Ingress
- Self-healing
- Horizontal Pod Autoscaling
- Multi-node Kubernetes

## General architecture

The platform will eventually contain services such as:

- API Gateway
- AI inference service
- Embedding service
- Worker service
- Redis or RabbitMQ
- PostgreSQL
- Qdrant
- MinIO
- Prometheus
- Grafana

Not all components should be introduced at once.

Each component must have a clear architectural reason to exist.

## Development philosophy

1. Prefer infrastructure work over application code.
2. Keep custom services extremely small.
3. Introduce one infrastructure concept at a time.
4. First implement the architecture with Docker and Docker Compose.
5. Add CI once the first services can be built and tested.
6. Publish project images to a container registry.
7. Afterwards migrate the same architecture to Kubernetes.
8. Add CD only once the Kubernetes deployment is understood.
9. Every major decision should be understandable and documented.
10. Avoid adding technologies merely to make the project look more complex.

## Project phases

### Phase 1 — Docker fundamentals

Create a minimal distributed system using a few containers.

Learn:

- images
- containers
- networks
- volumes
- environment variables
- health checks

### Phase 2 — Docker Compose

Describe the local architecture declaratively.

Learn:

- services
- networks
- volumes
- dependencies
- configuration
- replicas and scaling

### Phase 3 — CI

Introduce GitHub Actions.

Learn:

- build validation
- basic tests
- Docker image builds
- Compose validation
- image tagging
- publishing images to GitHub Container Registry

### Phase 4 — Distributed AI platform

Add AI-related services and observability.

### Phase 5 — Kubernetes

Migrate the Compose architecture to Kubernetes.

### Phase 6 — Scaling and resilience

Experiment with replicas, failures, load balancing, autoscaling and rolling updates.

### Phase 7 — CD

Connect GitHub Actions, GHCR and Kubernetes deployment.

## Important constraint

This is primarily an infrastructure learning project.

If implementing a feature requires significant application development but teaches little about containers, deployment, networking, storage, observability or orchestration, prefer a simpler implementation.

## Source of truth

The repository is the source of truth for the project.

Important context should live in:

- `docs/PROJECT_CONTEXT.md`
- `docs/ARCHITECTURE.md`
- `docs/ROADMAP.md`
- `docs/adr/`

AI tools should use these files as context rather than relying on long conversation history.
