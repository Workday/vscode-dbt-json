# DJ Extension Setup Guide

Get the DJ extension up and running in VS Code. This guide covers extension installation and basic configuration.

Table of Contents

1. [Install Prerequisites](#1-install-prerequisites)
2. [Install Trino CLI](#2-install-trino-cli)
3. [Configure Trino Connection](#3-configure-trino-connection)
4. [Install DJ Extension](#4-install-dj-extension)
5. [Configure Extension Settings](#5-configure-extension-settings)
6. [Optional: Lightdash Integration](#6-optional-lightdash-integration)
7. [Verify Installation](#7-verify-installation)

## 1. Install Prerequisites

**Required Software:**

- **VS Code** (version 1.87.0 or higher)
- **Python**
- **dbt Core** (or dbt adapter with dbt-core)
- **Trino CLI** (for data catalog browsing)
- **Java** (required for Trino CLI)
- **Node.js** (v20.x - v22.x LTS recommended) (optional, for Lightdash CLI)

**dbt Project Requirements:**

The DJ extension requires a dbt project with Python dependencies installed:

- **dbt-core** (minimum version 1.0.0)
- **dbt adapter** (e.g., dbt-trino, dbt-postgres, dbt-snowflake) that includes dbt-core

Install dbt dependencies using pip:

```bash
# Create a virtual environment
python -m venv .venv
source .venv/bin/activate

# Install dbt-trino (necessary for this extension)
pip install dbt-trino
```

> **Note**: The dbt adapter packages (like `dbt-trino`) automatically include `dbt-core` as a dependency, so you don't need to install both separately.

## 2. Install Trino CLI

The DJ extension uses Trino CLI for data catalog integration and query execution.

> If you want to use Trino locally, see [TRINO_LOCAL_SETUP.md](TRINO_LOCAL_SETUP.md)

### Option A: Download JAR (Recommended)

```bash
curl -o trino-cli https://repo1.maven.org/maven2/io/trino/trino-cli/476/trino-cli-476-executable.jar
chmod +x trino-cli
sudo mv trino-cli /usr/local/bin/
sudo ln -s /usr/local/bin/trino-cli /usr/local/bin/trino
```

### Option B: Using Homebrew (macOS)

```bash
brew install trino
sudo ln -s /opt/homebrew/bin/trino /usr/local/bin/trino-cli

# This should return the Trino CLI version
trino-cli --version
trino --version
```

## 3. Configure Trino Connection

Set up your Trino connection using environment variables:

```bash
# Add to your shell profile (.bashrc, .zshrc, etc.)
export TRINO_HOST=<your-trino-host>
export TRINO_PORT=<your-trino-port>    # 443 for Starburst Galaxy, 8080 for Local Trino
export TRINO_USERNAME=<your-username>
export TRINO_PASSWORD=<your-password>  # Optional
export TRINO_CATALOG=<your-catalog>
export TRINO_SCHEMA=<your-schema>
```

**Examples:**

- **Starburst Galaxy**: `your-cluster.galaxy.starburst.io:443`
- **Local Trino**: `localhost:8080`
- **Enterprise**: `trino.company.com:443`

## 4. Install DJ Extension

1. **Open VS Code**
2. **Go to Extensions** (Cmd/Ctrl + Shift + X)
3. **Search for "DJ"** or "dbt-json"
4. **Install the extension**

> Note: Want to build from source? See the [DEVELOPMENT_SETUP.md](../DEVELOPMENT_SETUP.md) for development setup.

## 5. Configure Extension Settings

The extension can be configured in the VS Code settings or by adding to the `.vscode/settings.json` file.

To configure the extension via VS Code settings,

- Open the VS Code settings (Cmd/Ctrl + ,)
- Under "Extensions", select "DJ (dbt-json) Framework".
- Configure the extension settings as needed.

To configure the extension via `.vscode/settings.json`, add the configuration options as needed to the file:

```json
{
  "dj.pythonVenvPath": ".venv", // path to the local Python virtual environment relative to the workspace root
  "dj.dbtProjectNames": ["your_project_name"] // optional, configure if you have multiple dbt projects in your workspace
}
```

Available settings:

- **`dj.pythonVenvPath`**: Path to the local Python virtual environment relative to the workspace root.
- **`dj.trinoPath`**: Path to the Trino CLI executable. If not set, will try to find `trino-cli` in PATH.
- **`dj.dbtProjectNames`**: Restrict which dbt projects the extension recognizes. If omitted, all projects are allowed.
- **`dj.airflowGenerateDags`**: Toggle writing Airflow DAG files. Defaults to `true`; set to `false` to skip writing DAGs.
- **`dj.airflowTargetVersion`**: Target Airflow version to use when generating the DAG. Defaults to `2.7`.
- **`dj.airflowDagsPath`**: Folder where the extension writes Airflow DAGs. Defaults to `dags/_ext_`.
- **`dj.dbtMacroPath`**: Subfolder where the extension writes macros, relative to the `macro-paths` in `dbt_project.yml` (defaults to `macros` if not set). Defaults to `_ext_`.

## 6. Optional: Lightdash Integration

If you are using lightdash, you can integrate it with the extension to create dashboards.

Add `lightdash` or `lightdash-explore` tags to the models in your dbt project to add them to Lightdash. For example:

```json
{
  "tags": ["lightdash", "lightdash-explore"]
}
```

> **Local Setup**: If you want to setup Lightdash locally, see [LIGHTDASH_LOCAL_SETUP.md](LIGHTDASH_LOCAL_SETUP.md).
>
> **Troubleshooting**: If you encounter connection issues with Lightdash + Trino, see the [troubleshooting section](LIGHTDASH_LOCAL_SETUP.md#troubleshooting) in the local setup guide.

This requires Lightdash CLI to be installed which can be done by running the following command:

```bash
npm install -g @lightdash/cli
```

We will need to authenticate the Lightdash CLI, see [Authenticating your CLI](https://docs.lightdash.com/guides/cli/cli-authentication#login).

Next, lets configure the environment variables for Lightdash:

```bash
# Add to your shell profile (.bashrc, .zshrc, etc.)
export LIGHTDASH_URL=<lightdash-domain>
export LIGHTDASH_PREVIEW_NAME=<lightdash-preview-name>
export LIGHTDASH_PROJECT=<lightdash-project> # The UUID for your project.
```

> Note: If your URL looks like `https://<lightdash-domain>/projects/<UUID>/Tables`, then `<UUID>` is your LIGHTDASH_PROJECT
> If you are using local Lightdash, your domain will be `http://localhost:8081`
>
> **Configuration**: For custom dbt project path and profiles path, see [LIGHTDASH_CONFIGURATION.md](LIGHTDASH_CONFIGURATION.md).

## 7. Verify Installation

1. **Reload VS Code** to load environment variables
2. **Open Command Palette** (Cmd/Ctrl + Shift + P)
3. **Type "DJ"** - you should see DJ commands available
4. **Test Trino connection** by running `DJ: Test Trino Connection` from the command palette

## What's Next?

**Ready to build your first models?**

1. **[Follow the Tutorial](TUTORIAL.md)** - Build your first models step-by-step with the example dbt project based on jaffle shop data from dbt docs
2. **[Explore the Example Project](example/README.md)** - See what's possible with the example dbt project based on jaffle shop data from dbt docs
3. **[Browse Model Types](models/README.md)** - Learn about all 11 model types with the example dbt project based on jaffle shop data from dbt docs

## Troubleshooting

### Extension Not Working?

- **Reload VS Code** after setting environment variables
- **Check Python virtual environment path** in the extension settings
- **Verify Trino CLI** is in your PATH: `which trino-cli`
- **Verify Trino connection** by running `DJ: Test Trino Connection` from the command palette
- **Verify Groups** are defined in `dbt_project.yml` or `models/groups.yml`
- **Run `dbt parse`** to generate/update the manifest.json file
- **Verify environment variables** in your shell profile

### Need More Help?

- **Local Trino Setup**: See [TRINO_LOCAL_SETUP.md](TRINO_LOCAL_SETUP.md)
- **Local Lightdash Setup**: See [LIGHTDASH_LOCAL_SETUP.md](LIGHTDASH_LOCAL_SETUP.md)
- **Lightdash Configuration**: See [LIGHTDASH_CONFIGURATION.md](LIGHTDASH_CONFIGURATION.md)
- **Development Setup**: See [DEVELOPMENT_SETUP.md](../DEVELOPMENT_SETUP.md)
- **Example Project Issues**: Check [example/README.md](example/README.md)

---

**Installation complete!** → [Start the Tutorial](TUTORIAL.md)
