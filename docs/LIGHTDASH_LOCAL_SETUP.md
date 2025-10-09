# Local Lightdash Setup Guide

Quick setup guide for running Lightdash locally with the DJ extension.

Lightdash is an open-source BI tool that creates dashboards directly from your dbt models.

> **Already have Lightdash?** Return to the [main setup guide](SETUP.md#lightdash-integration) instead.

Table of Contents

1. [Prerequisites](#prerequisites)
2. [Setup](#setup)
3. [Troubleshooting](#troubleshooting)
4. [Next Steps](#next-steps)

## Prerequisites

- **Docker** and **Docker Compose**
- **Trino running locally** (see [Trino setup](TRINO_LOCAL_SETUP.md))

## Setup

This guide uses Docker Compose to run Lightdash locally. For other installation methods, refer to the [official Lightdash installation guide](https://docs.lightdash.com/self-host/self-host-lightdash-docker-compose).

### 1. Install Lightdash

```bash
# Clone and start Lightdash
git clone https://github.com/lightdash/lightdash
cd lightdash
```

Let's update .env config for the below settings:

```bash
PORT=8081 # Since trino is already running on port 8080, let's use a different port.
SITE_URL=http://localhost:8081
DBT_PROJECT_DIR=<path-to-your-dbt-project>
```

### Start Lightdash

```bash
docker compose up -d
```

### 2. Connect Trino and Lightdash Networks (optional, if you have setup Trino in local Docker)

**Important**: By default, the `profiles.yml` file specifies the Trino host as `localhost:8080`. However, if Trino is running in a separate Docker container, you need to ensure that both the Lightdash and Trino containers are on the same Docker network. After connecting the networks, update the connection settings in your Lightdash project to reflect the correct Trino host.

```bash
# Check docker network name of Lightdash (usually lightdash_default)
docker network ls | grep lightdash

# Connect Trino to Lightdash network.
# Replace `lightdash_default` with the network name you found above.
# Replace `trino_default` with the actual container name of Trino if you have created it with a different name.
docker network connect lightdash_default trino_default

# Verify connection (optional)
docker inspect trino_default --format='{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}'
```

### 3. Create Lightdash Project

Install Lightdash CLI if you haven't already:

```bash
npm install -g @lightdash/cli
```

Authenticate Lightdash CLI:

```bash
lightdash login http://localhost:8081
```

Then inside your dbt project, run the following command to create a Lightdash project:

```bash
lightdash deploy --create
```

> Note: dbt must be installed in the dbt project for this command to work.

This will prompt you to enter the project name and once created, you should see URL of the newly created project in the output. This will be something like ` http://localhost:8081/createProject/cli?projectUuid=48c5c6d6-3b63-4661-baa3-da07eb446769`.

### 4. Verify

```bash
# Check status
docker compose ps
```

Open the URL of the Lightdash project you created from the output in the browser. You should see the project you created.

#### Verify Connection Settings

If you are using local Trino, you may need to update the connection settings to Trino.

In the Lightdash UI, go to `Settings` -> `Project Settings` and click on the `Connection Settings` tab.

Under the `Warehouse Connections` tab, you should see the configuration to Trino.

Update host details:

```text
# This is the name of the Trino container. Replace with the actual container name if you have created it with a different name.
Host: trino_default
```

**Why `trino_default`?** Since both containers are now on the same network, Lightdash can reach Trino using its container name instead of IP addresses.

## Troubleshooting

### Connection Errors

If you get "Connection Refused" errors:

```bash
# Ensure containers are connected
docker network connect lightdash_default trino_default
```

### Alternative Hosts

If `trino_default` doesn't work, try:

- **Linux**: Get container IP with `docker inspect trino_default`

### Verify Trino

```bash
# Should return 200 or 303
curl -v http://localhost:8080
```

## Next Steps

- **Configure DJ integration**: [Setup Guide](SETUP.md#lightdash-integration)
- **Build dbt models**: [Tutorial](TUTORIAL.md)
- **Official docs**: [Lightdash Docker Setup](https://docs.lightdash.com/self-host/self-host-lightdash-docker-compose)

---

**Setup complete!** Return to the [Setup Guide](SETUP.md) to configure DJ integration.
