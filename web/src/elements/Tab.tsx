import {
  Tab as HeadlessTab,
  TabGroup,
  TabList,
  TabPanel,
  TabPanels,
} from '@headlessui/react';
import { makeClassName } from '@web';

export type TabProps = {
  tabs: string[];
  panels: React.ReactNode[];
  onChange?: (value: { index: number; value: string }) => void;
  defaultIndex?: number;
};

export function Tab({ tabs, panels, onChange, defaultIndex }: TabProps) {
  // Only pass defaultIndex to TabGroup if it's explicitly provided
  const tabGroupProps =
    defaultIndex !== undefined
      ? {
          onChange: (index: number) =>
            onChange?.({ index, value: tabs[index] }),
          defaultIndex,
        }
      : {
          onChange: (index: number) =>
            onChange?.({ index, value: tabs[index] }),
        };

  return (
    <TabGroup {...tabGroupProps}>
      <TabList className="flex bg-[var(--color-tab)]">
        {tabs.map((tab, index) => (
          <HeadlessTab
            key={index}
            className={makeClassName(
              'px-3 py-2 border-b-2 border-b-transparent',
              'data-[selected]:bg-[var(--color-tab-contrast)] data-[selected]:border-[var(--color-tab-active)]',
            )}
          >
            {tab}
          </HeadlessTab>
        ))}
      </TabList>
      <TabPanels className="p-4 bg-[var(--color-tab-contrast)]">
        {panels.map((panel, index) => (
          <TabPanel key={index}>{panel}</TabPanel>
        ))}
      </TabPanels>
    </TabGroup>
  );
}
