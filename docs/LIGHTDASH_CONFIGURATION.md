# Lightdash Configuration

The DJ extension provides flexible configuration options for Lightdash integration, allowing you to work with different dbt project structures and multiple projects.

## Configuration Options

### Auto-Detection (Default)

By default, the extension automatically detects dbt projects in your workspace by scanning for `dbt_project.yml` files. When you run the Lightdash preview command, it will:

1. **Single Project**: Automatically use the detected dbt project directory for both `--project-dir` and `--profiles-dir`
2. **Multiple Projects**: Show a quick pick dialog to let you select which project to use
3. **No Projects**: Show an error message asking you to add a dbt project or configure a custom path

**Important**: The Lightdash path will always be the same as the currently available dbt project path. Both the project directory and profiles directory will point to the same location unless explicitly overridden in configuration.

### Manual Configuration

You can override the auto-detection by setting these VS Code configuration options:

#### `dj.lightdashProjectPath`

- **Type**: `string`
- **Description**: Custom path to the dbt project directory (relative to workspace root)
- **Example**: `"docs/example"` for projects where dbt is in a subdirectory

#### `dj.lightdashProfilesPath`

- **Type**: `string`
- **Description**: Custom path to the dbt profiles directory (relative to workspace root)
- **Default**: Uses the project directory or workspace root if not specified

## Examples

### Example 1: dbt project in subdirectory

If your dbt project is located at `docs/example/` instead of the default `dags/dbt/`:

```json
{
  "dj.lightdashProjectPath": "docs/example"
}
```

### Example 2: Custom profiles location

If you want to use a specific profiles directory:

```json
{
  "dj.lightdashProjectPath": "my-dbt-project",
  "dj.lightdashProfilesPath": "profiles"
}
```

### Example 3: Multiple projects with auto-selection

Leave the configuration empty to enable auto-detection. When multiple projects are found, you'll see a selection dialog like:

```text
Select dbt project for Lightdash preview
> my_analytics_project (analytics/dbt)
  reporting_project (reports/dbt)
  staging_project (staging/dbt)
```

**Note**: If you cancel the selection dialog (press Escape), the Lightdash preview operation will be cancelled.

## Setting Configuration

### Via VS Code Settings UI

1. Open VS Code Settings (`Cmd+,` on Mac, `Ctrl+,` on Windows/Linux)
2. Search for "dj lightdash"
3. Set the desired values

### Via settings.json

Add to your workspace or user `settings.json`:

```json
{
  "dj.lightdashProjectPath": "docs/example",
  "dj.lightdashProfilesPath": "profiles"
}
```

### Via Workspace Settings

Create/edit `.vscode/settings.json` in your workspace:

```json
{
  "dj.lightdashProjectPath": "docs/example"
}
```

## Troubleshooting

### "No dbt projects found in workspace"

- **Add a dbt project**: Ensure your workspace contains a `dbt_project.yml` file
- **Configure custom path**: Set `dj.lightdashProjectPath` to point to your dbt project directory

### "Lightdash command fails"

- Verify the project path exists and contains a valid dbt project
- Check that Lightdash CLI is installed and accessible
- Ensure the project has the required `lightdash` and `lightdash-explore` tags on models
