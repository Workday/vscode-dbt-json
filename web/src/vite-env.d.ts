/// <reference types="vite/client" />

// SVG imports as React components
declare module '*.svg?react' {
  import type React from 'react';
  const SVGComponent: React.FC<React.SVGProps<SVGSVGElement>>;
  export default SVGComponent;
}

// Regular SVG imports
declare module '*.svg' {
  const content: string;
  export default content;
}
