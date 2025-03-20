# RCA Agent Kubernetes Deployment

This directory contains Kubernetes deployment configurations for the RCA Agent.

## Prerequisites

- Docker installed and configured
- kubectl installed and configured
- Access to a Kubernetes cluster
- Container registry access (Docker Hub or private registry)

## Configuration Files

- **deployment.yaml**: Main RCA Agent deployment
- **service.yaml**: Service for exposing the RCA Agent
- **configmap.yaml**: ConfigMap containing application configuration
- **secret.yaml**: Secret containing sensitive information
- **postgres.yaml**: PostgreSQL database deployment

## Deployment Steps

1. Update the registry information in `Makefile.k8s` by setting the `REGISTRY` variable to your container registry.

2. Create the Kubernetes namespace (if not using default):
   ```bash
   kubectl create namespace your-namespace
   ```

3. Generate the ConfigMap:
   ```bash
   make -f Makefile.k8s k8s-config NAMESPACE=your-namespace
   ```

4. Generate the Secret (you will be prompted for credentials):
   ```bash
   make -f Makefile.k8s k8s-secret NAMESPACE=your-namespace
   ```

5. Build and deploy the application:
   ```bash
   make -f Makefile.k8s k8s-deploy NAMESPACE=your-namespace
   ```

## Accessing the Application

To access the application locally:
```bash
make -f Makefile.k8s k8s-port-forward NAMESPACE=your-namespace
```

The application will be available at http://localhost:8080

## Viewing Logs

```bash
make -f Makefile.k8s k8s-logs NAMESPACE=your-namespace
```

## Cleanup

To delete all resources:
```bash
make -f Makefile.k8s k8s-delete NAMESPACE=your-namespace
``` 