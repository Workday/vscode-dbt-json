import { makeClassName } from '@web';
import AddSvg from '@web/assets/icons/add.svg?react';
// Import all SVG icons - add new imports here when adding new icons
import CompassSvg from '@web/assets/icons/compass.svg?react';
import React from 'react';
/**
 * Icon registry - maps icon names to their imported SVG components
 * Add new icons here when you add new SVG files
 */
const iconRegistry = {
  compass: CompassSvg,
  add: AddSvg,
  // Add more icons here:
  // clock: ClockSvg,
  // user: UserSvg,
} as const;

/**
 * Available icon names (type-safe enum)
 */
export type IconName = keyof typeof iconRegistry;

export interface IconProps {
  /** Name of the icon from the icon registry */
  name: IconName;
  /** Tailwind classes for size, color, hover effects, etc. */
  className?: string;
  /** Accessibility title */
  title?: string;
}

/**
 * Generic Icon component that renders custom SVG icons from assets/icons/
 *
 * The icon will inherit colors from className using currentColor.
 * Make sure your SVG uses currentColor for fill/stroke attributes.
 *
 * @example
 * ```tsx
 * // Basic usage with Tailwind classes
 * <Icon name="compass" className="w-6 h-6 text-black hover:text-primary" />
 *
 * // With transition
 * <Icon name="compass" className="w-5 h-5 text-black hover:text-primary transition-colors" />
 *
 * // With title for accessibility
 * <Icon name="compass" className="w-4 h-4" title="Navigate" />
 * ```
 */
export const Icon: React.FC<IconProps> = ({ name, className = '', title }) => {
  const IconComponent = iconRegistry[name];

  if (!IconComponent) {
    console.warn(`Icon "${name}" not found in icon registry`);
    return null;
  }

  return (
    <span
      className={makeClassName('inline-block', className)}
      aria-hidden={!title}
      aria-label={title}
      role={title ? 'img' : undefined}
      title={title}
    >
      <IconComponent className="w-full h-full" />
    </span>
  );
};
