# Kubernetes for Data Engineering

This repository contains the necessary configuration files and DAGs (Directed Acyclic Graphs) for setting up a robust data engineering environment using Kubernetes and Apache Airflow. It includes the setup for the Kubernetes Dashboard, which provides a user-friendly web interface for managing Kubernetes clusters, and Apache Airflow, a platform to programmatically author, schedule, and monitor workflows.

### DAGs

- `fetch_and_preview.py`: A DAG for fetching data and providing a preview.
- `hello.py`: A simple example DAG to demonstrate basic Airflow concepts.

### Kubernetes (k8s) Configuration

- `dashboard-adminuser.yaml`: YAML file for setting up an admin user for the Kubernetes Dashboard.
- `dashboard-clusterrole.yaml`: YAML file defining the cluster role for the Kubernetes Dashboard.
- `dashboard-secret.yaml`: YAML file for managing secrets used by the Kubernetes Dashboard.
- `recommended-dashboard.yaml`: YAML file for deploying the recommended Kubernetes Dashboard setup.
- `values.yaml`: YAML file containing values for customizing the Kubernetes setup.

### Prerequisites

- A Kubernetes cluster
- `kubectl` installed and configured
- Helm (optional, but recommended for managing Kubernetes applications)
