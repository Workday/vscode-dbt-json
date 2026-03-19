/**
 * Discriminated union type for messages sent from webview panels to the extension
 */
export type WebviewMessage =
  | WebviewClosePanel
  | WebviewSaveDraftAndClose
  | WebviewOpenExternalUrl
  | WebviewModelUpdated
  | WebviewModelCreated
  | WebviewApiRequest;

/**
 * Close panel message
 */
export interface WebviewClosePanel {
  type: 'close-panel';
  panelType: string;
}

/**
 * Save draft and close panel message
 */
export interface WebviewSaveDraftAndClose {
  type: 'save-draft-and-close';
  panelType: string;
}

/**
 * Open external URL message
 */
export interface WebviewOpenExternalUrl {
  type: 'open-external-url';
  url: string;
}

/**
 * Model updated message
 */
export interface WebviewModelUpdated {
  type: 'model-updated';
  formType: string; // Required - model type identifier
  success?: boolean;
  message?: string;
}

/**
 * Model created message
 */
export interface WebviewModelCreated {
  type: 'model-created';
  formType?: string; // Optional - cleanup identifier
  success?: boolean;
  message?: string;
}

/**
 * API request message (forwarded to API handler)
 */
export interface WebviewApiRequest {
  type: 'api-request';
  apiType: string;
  request: unknown;
  _channelId?: string;
}

/**
 * Generic message for unknown/untyped messages
 * Used as fallback when message doesn't match known types
 */
export interface WebviewGenericMessage {
  type?: string;
  [key: string]: unknown;
}

/**
 * Type guard to check if a message is a WebviewMessage
 */
export function isWebviewMessage(msg: unknown): msg is WebviewMessage {
  return (
    typeof msg === 'object' &&
    msg !== null &&
    ('type' in msg || Object.keys(msg).length > 0)
  );
}

/**
 * Type guard for close-panel message
 */
export function isClosePanelMessage(
  msg: WebviewMessage,
): msg is WebviewClosePanel {
  return msg.type === 'close-panel';
}

/**
 * Type guard for save-draft-and-close message
 */
export function isSaveDraftAndCloseMessage(
  msg: WebviewMessage,
): msg is WebviewSaveDraftAndClose {
  return msg.type === 'save-draft-and-close';
}

/**
 * Type guard for open-external-url message
 */
export function isOpenExternalUrlMessage(
  msg: WebviewMessage,
): msg is WebviewOpenExternalUrl {
  return msg.type === 'open-external-url';
}

/**
 * Type guard for model-updated message
 */
export function isModelUpdatedMessage(
  msg: WebviewMessage,
): msg is WebviewModelUpdated {
  return msg.type === 'model-updated';
}

/**
 * Type guard for model-created message
 */
export function isModelCreatedMessage(
  msg: WebviewMessage,
): msg is WebviewModelCreated {
  return msg.type === 'model-created';
}

/**
 * Type guard for api-request message
 */
export function isApiRequestMessage(
  msg: WebviewMessage,
): msg is WebviewApiRequest {
  return msg.type === 'api-request';
}
