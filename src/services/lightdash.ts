import { Coder } from '@services/coder';
import { getDjConfig } from '@services/config';
import { DJService } from '@services/types';
import { assertExhaustive } from '@shared';
import { ApiPayload, ApiResponse } from '@shared/api/types';
import { apiResponse } from '@shared/api/utils';
import { COMMAND_ID } from '@shared/constants';
import { ThemeIcon, TreeItem, WORKSPACE_ROOT } from 'admin';
import { spawn } from 'child_process';
import * as vscode from 'vscode';

export class Lightdash implements DJService {
  private readonly coder: Coder;
  readonly handleApi: (
    payload: ApiPayload<'lightdash'>,
  ) => Promise<ApiResponse>;

  readonly treeItemLightdashPreview: TreeItem = {
    command: {
      command: COMMAND_ID.LIGHTDASH_PREVIEW,
      title: 'Lightdash Preview',
    },
    iconPath: new ThemeIcon('lightbulb-sparkle'),
    label: 'Lightdash Preview',
  };

  constructor({ coder }: { coder: Coder }) {
    this.coder = coder;

    this.handleApi = async (payload) => {
      switch (payload.type) {
        case 'lightdash-start-preview': {
          try {
            return await vscode.window.withProgress(
              {
                location: vscode.ProgressLocation.Notification,
                title: 'Lightdash Preview',
              },
              async (progress) => {
                progress.report({ message: 'Getting dbt project info...' });

                const previewName =
                  process.env.LIGHTDASH_PREVIEW_NAME || 'DJ Preview';

                const { projectDir, profilesDir } =
                  await this.getDbtProjectPaths();

                progress.report({ message: 'Starting Lightdash preview...' });

                const previewUrl = await this.executeAndCaptureUrl(
                  {
                    previewName,
                    projectDir,
                    profilesDir,
                  },
                  progress,
                );

                progress.report({ message: 'Preview ready!' });
                return apiResponse<typeof payload.type>({ url: previewUrl });
              },
            );
          } catch (error) {
            let errorMessage = '';

            if (error instanceof Error) {
              // dbt not found
              if (
                error.message.includes('Failed to get dbt --version') ||
                error.message.includes('spawn dbt ENOENT')
              ) {
                const { pythonVenvPath } = getDjConfig();

                errorMessage =
                  'dbt command not found.' + pythonVenvPath
                    ? `Using Python venv: '${pythonVenvPath}'.`
                    : 'If dbt is in a Python virtual environment, configure "dj.pythonVenvPath" in VS Code settings.' +
                      '\n\nSee https://docs.getdbt.com/docs/core/installation for installation instructions';
              } else {
                // Missing dbt projects, User cancelled project selection, etc.
                errorMessage = error.message;
              }

              vscode.window.showErrorMessage(errorMessage);
              return apiResponse<typeof payload.type>({ error: errorMessage });
            }

            vscode.window.showErrorMessage(
              'Lightdash Preview Failed: Unknown error',
            );
            // Any other unknown error
            throw error;
          }
        }
        default:
          return assertExhaustive<ApiResponse>(payload.type);
      }
    };
  }

  /**
   * Execute the lightdash command and capture the URL
   * @param previewName - The name of the preview
   * @param projectDir - The directory of the project
   * @param profilesDir - The directory of the profiles
   * @returns The URL of the preview or an error message
   */
  private async executeAndCaptureUrl(
    {
      previewName,
      projectDir,
      profilesDir,
    }: {
      previewName: string;
      projectDir: string;
      profilesDir: string;
    },
    progress?: vscode.Progress<{ message?: string }>,
  ): Promise<string> {
    return new Promise((resolve, reject) => {
      const args = [
        'start-preview',
        '--name',
        previewName,
        '--profiles-dir',
        profilesDir,
        '--project-dir',
        projectDir,
        '-s',
        'tag:lightdash tag:lightdash-explore',
        '--skip-warehouse-catalog', // Skip caching, force re-read from profile
        '--verbose',
      ];

      this.coder.log.info(`Executing: lightdash ${args.join(' ')}`);

      // Get Python venv path from config
      const { pythonVenvPath } = getDjConfig();
      const env = { ...process.env };

      // Add Python venv PATH to environment variables
      if (pythonVenvPath) {
        const venvBinPath = `${WORKSPACE_ROOT}/${pythonVenvPath}/bin`;
        env.PATH = `${venvBinPath}:${env.PATH}`;
        this.coder.log.info(`Using Python venv PATH: ${venvBinPath}`);
      }

      // Override Trino host for Docker environments
      // When running Lightdash preview with Trino in Docker, the host should be
      // the container name (eg., trino_default) instead of localhost for the containers
      // to communicate on the same Docker network
      if (process.env.LIGHTDASH_TRINO_HOST) {
        env.DBT_HOST = process.env.LIGHTDASH_TRINO_HOST;
        this.coder.log.info(
          `Overriding Trino host: ${process.env.LIGHTDASH_TRINO_HOST}`,
        );
      }

      // Execute lightdash command
      const childProcess = spawn('lightdash', args, {
        cwd: WORKSPACE_ROOT,
        env,
      });

      let output = '';
      let errorOutput = '';

      // Collect stdout output
      childProcess.stdout?.on('data', (data: Buffer) => {
        const text = data.toString();
        output += text;
      });

      // Collect stderr output and update progress
      childProcess.stderr?.on('data', (data: Buffer) => {
        const text = data.toString();
        errorOutput += text;

        // Update progress based on key Lightdash milestones
        if (progress) {
          if (
            text.includes('Compiling with project dir') ||
            text.includes('Loading dbt_project.yml')
          ) {
            progress.report({ message: 'Setting up dbt project...' });
          } else if (
            text.includes('Running: dbt ls') ||
            text.includes('Models:')
          ) {
            progress.report({ message: 'Finding models to sync...' });
          } else if (text.includes('Validating') && text.includes('models')) {
            progress.report({ message: 'Validating models...' });
          } else if (
            text.includes('Fetching warehouse catalog') ||
            text.includes('Using target')
          ) {
            progress.report({ message: 'Connecting to warehouse...' });
          } else if (
            text.includes('Converting explores') ||
            text.includes('SUCCESS>')
          ) {
            progress.report({ message: 'Converting models to explores...' });
          } else if (
            text.includes('Making HTTP PUT request') ||
            (text.includes('Compiled') && text.includes('explores'))
          ) {
            progress.report({ message: 'Uploading to Lightdash...' });
          }
        }
      });

      // On completion of lightdash command, handle the result
      childProcess.on('close', (code, signal) => {
        this.coder.log.info(
          `Lightdash command completed with code: ${code}, signal: ${signal}`,
        );

        this.coder.log.info(`Full stdout output: ${output}`);
        this.coder.log.error(`Full stderr output: ${errorOutput}`);

        // Handle process killed by signal (user killed terminal, Ctrl+C, etc.)
        if (signal) {
          reject(
            new Error(`Lightdash command was terminated by signal ${signal}.`),
          );
          return;
        }

        if (code !== 0) {
          reject(
            new Error(
              `Lightdash command failed with code ${code}: ${errorOutput})`,
            ),
          );
          return;
        }

        // If lightdash command completes successfully, look for preview URL in complete output
        // Check both stdout and stderr since Lightdash may write to either
        const combinedOutput = output + '\n' + errorOutput;
        this.coder.log.info(`Searching for URL in combined output`);

        const urlMatch = combinedOutput.match(
          /(Project updated on|New project created on)\s+(https?:\/\/[^\s]+\/projects\/[^\s]+)/i,
        );
        if (urlMatch) {
          const previewUrl = urlMatch[2].trim();
          this.coder.log.info(`Found preview URL: ${previewUrl}`);
          resolve(previewUrl);
          return;
        }

        // No URL found in complete output
        reject(
          new Error(
            'No preview URL found in complete output. Please check the Lightdash logs for more details.',
          ),
        );
      });

      // If lightdash fails to start
      childProcess.on('error', (error) => {
        reject(new Error(`Failed to start Lightdash: ${error.message}`));
      });

      // Timeout after 2 minutes if lightdash takes too long
      setTimeout(() => {
        childProcess.kill();
        reject(new Error('Lightdash command timed out after 2 minutes.'));
      }, 120000);
    });
  }

  private async getDbtProjectPaths(): Promise<{
    projectDir: string;
    profilesDir: string;
  }> {
    const config = getDjConfig();

    // Check for custom configuration override
    if (config.lightdashProjectPath) {
      const projectDir = `${WORKSPACE_ROOT}/${config.lightdashProjectPath}`;
      const profilesDir = config.lightdashProfilesPath
        ? `${WORKSPACE_ROOT}/${config.lightdashProfilesPath}`
        : projectDir;

      return { projectDir, profilesDir };
    }

    // Auto-detect from available dbt projects
    const dbtProjects = Array.from(this.coder.framework.dbt.projects.values());

    if (dbtProjects.length === 0) {
      throw new Error(
        'No dbt projects found in workspace. Please ensure your workspace contains a dbt_project.yml file or configure a custom path using "dj.lightdashProjectPath".',
      );
    }

    if (dbtProjects.length === 1) {
      return {
        projectDir: dbtProjects[0].pathSystem,
        profilesDir: dbtProjects[0].pathSystem,
      };
    }

    // Multiple projects - show picker
    const projectItems = dbtProjects.map((project) => ({
      label: project.name,
      description: project.pathRelative,
      project: project,
    }));

    const selected = await vscode.window.showQuickPick(projectItems, {
      placeHolder: 'Select dbt project for Lightdash preview',
      title: 'Multiple dbt projects found',
    });

    if (!selected) {
      throw new Error('Lightdash preview cancelled: No project selected');
    }

    this.coder.log.info(`Selected dbt project: ${selected.project.name}`);
    return {
      projectDir: selected.project.pathSystem,
      profilesDir: selected.project.pathSystem,
    };
  }

  async activate(context: vscode.ExtensionContext) {
    // Register commands
    this.registerCommands(context);
  }

  /**
   * Register Lightdash-specific commands - Lightdash Preview
   * @param context
   */
  registerCommands(context: vscode.ExtensionContext) {
    context.subscriptions.push(
      vscode.commands.registerCommand(
        COMMAND_ID.LIGHTDASH_PREVIEW,
        async () => {
          try {
            const resp = await this.coder.lightdash.handleApi({
              type: 'lightdash-start-preview',
              request: null,
            });

            // Open the Lightdash URL in browser if available
            if (resp && typeof resp === 'object' && 'url' in resp && resp.url) {
              const selection = await vscode.window.showInformationMessage(
                'Lightdash preview started! Open in browser?',
                'Open Browser',
                'Copy URL',
              );

              if (selection === 'Open Browser') {
                vscode.env.openExternal(vscode.Uri.parse(resp.url));
              } else if (selection === 'Copy URL') {
                vscode.env.clipboard.writeText(resp.url);
                vscode.window.showInformationMessage(
                  'Lightdash URL copied to clipboard',
                );
              }
            }
          } catch (err) {
            this.coder.log.error('ERROR GENERATING PREVIEW: ', err);
          }
        },
      ),
    );
  }

  deactivate() {}
}
