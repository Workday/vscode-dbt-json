import { join } from 'path';
import type { Webview } from 'vscode';
import { Uri } from 'vscode';

export function getHtml({
  extensionUri,
  route,
  webview,
}: {
  extensionUri: Uri;
  route: string;
  webview: Webview;
}) {
  const mainCss = webview.asWebviewUri(
    Uri.file(join(extensionUri.fsPath, 'dist', 'web', 'assets', 'main.css')),
  );
  const mainJs = webview.asWebviewUri(
    Uri.file(join(extensionUri.fsPath, 'dist', 'web', 'assets', 'main.js')),
  );
  return `
        <!DOCTYPE html>
          <html lang="en">
          <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <meta name="route" content="${route}">
            <title>DJ</title>
            <link rel="stylesheet" type="text/css" href="${String(mainCss)}">
          </head>
          <body>
            <div id="root"></div>
            <script type="module" src="${String(mainJs)}"></script>
          </body>
        </html>
      `;
}
