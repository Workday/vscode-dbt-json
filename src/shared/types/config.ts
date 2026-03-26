/**
 * Configuration types shared between extension and web.
 * Currently only contains VSCodeApi since the web doesn't need full config.
 */

/**
 * VSCode webview API type (used by web).
 */
export type VSCodeApi = {
  postMessage: (msg: object) => void;
};
