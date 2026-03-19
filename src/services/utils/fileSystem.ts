import * as fs from 'fs';
import * as path from 'path';

export class FileSystemUtils {
  /**
   * Ensures a directory exists, creating it if necessary
   */
  static async ensureDirectory(dirPath: string): Promise<void> {
    try {
      await fs.promises.access(dirPath);
    } catch {
      try {
        await fs.promises.mkdir(dirPath, { recursive: true });
        // Verify directory was created
        await fs.promises.access(dirPath);
      } catch (mkdirError) {
        throw new Error(
          `Failed to create directory ${dirPath}: ${mkdirError instanceof Error ? mkdirError.message : 'Unknown error'}`,
        );
      }
    }
  }

  /**
   * Safely reads a JSON file with error handling
   */
  static async readJsonFile<T>(filePath: string): Promise<T | null> {
    try {
      const content = await fs.promises.readFile(filePath, 'utf8');
      return JSON.parse(content) as T;
    } catch (error: unknown) {
      if ((error as NodeJS.ErrnoException).code === 'ENOENT') {
        return null; // File doesn't exist
      }
      console.error(
        `[FileSystemUtils] Error reading JSON file ${filePath}:`,
        error,
      );
      return null;
    }
  }

  /**
   * Safely writes JSON to file using atomic write (temp file + rename)
   */
  static async writeJsonFile<T>(filePath: string, data: T): Promise<void> {
    const tempPath = `${filePath}.tmp`;

    try {
      // Ensure directory exists
      await this.ensureDirectory(path.dirname(filePath));

      // Write to temp file first
      await fs.promises.writeFile(
        tempPath,
        JSON.stringify(data, null, 2),
        'utf8',
      );

      // Verify temp file exists before rename
      try {
        await fs.promises.access(tempPath);
      } catch (accessError) {
        throw new Error(
          `Temp file was not created successfully: ${accessError instanceof Error ? accessError.message : 'Unknown error'}`,
        );
      }

      // Atomic rename
      await fs.promises.rename(tempPath, filePath);
    } catch (error: unknown) {
      // Clean up temp file on error
      try {
        await fs.promises.unlink(tempPath);
      } catch (cleanupError: unknown) {
        console.warn(
          `[FileSystemUtils] Failed to cleanup temp file ${tempPath}:`,
          cleanupError,
        );
      }
      throw error;
    }
  }

  /**
   * Checks if a file exists and is readable
   */
  static async fileExists(filePath: string): Promise<boolean> {
    try {
      await fs.promises.access(filePath, fs.constants.R_OK);
      return true;
    } catch {
      return false;
    }
  }

  /**
   * Checks if a directory is writable
   */
  static async isDirectoryWritable(dirPath: string): Promise<boolean> {
    try {
      await fs.promises.access(dirPath, fs.constants.W_OK);
      return true;
    } catch {
      return false;
    }
  }
}
