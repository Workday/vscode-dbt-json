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
      await fs.promises.mkdir(dirPath, { recursive: true });
    }
  }

  /**
   * Safely reads a JSON file with error handling
   */
  static async readJsonFile<T>(filePath: string): Promise<T | null> {
    try {
      const content = await fs.promises.readFile(filePath, 'utf8');
      return JSON.parse(content) as T;
    } catch (error) {
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

      // Atomic rename
      await fs.promises.rename(tempPath, filePath);
    } catch (error) {
      // Clean up temp file on error
      try {
        await fs.promises.unlink(tempPath);
      } catch {
        // Ignore cleanup errors
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
