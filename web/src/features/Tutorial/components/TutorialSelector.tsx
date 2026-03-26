import { InformationCircleIcon } from '@heroicons/react/20/solid';
import { Button, Tooltip } from '@web/elements';
import { useState } from 'react';

import type { TutorialMode } from '../types';

interface TutorialSelectorProps {
  onSelectTutorial: (mode: TutorialMode) => void;
  onCancel: () => void;
}

/**
 * Modal for selecting which tutorial to run
 */
export function TutorialSelector({
  onSelectTutorial,
  onCancel,
}: TutorialSelectorProps) {
  const [selectedMode, setSelectedMode] = useState<TutorialMode>('select');

  const tutorials = [
    {
      mode: 'select' as TutorialMode,
      icon: '📋',
      iconBg: 'bg-blue-100',
      iconText: 'text-blue-600',
      title: 'Select Model',
      description:
        'Perfect for beginners. Learn to pull data from a single source and select specific columns.',
      duration: '~3 mins',
      durationBg: 'bg-blue-100',
      durationText: 'text-blue-700',
      level: 'Beginner',
      levelBg: 'bg-green-100',
      levelText: 'text-green-700',
    },
    {
      mode: 'join' as TutorialMode,
      icon: '🔗',
      iconBg: 'bg-purple-100',
      iconText: 'text-purple-600',
      title: 'Join Model',
      description:
        'Learn to combine data from multiple sources using SQL joins and create complex relationships.',
      duration: '~5 mins',
      durationBg: 'bg-purple-100',
      durationText: 'text-purple-700',
      level: 'Intermediate',
      levelBg: 'bg-orange-100',
      levelText: 'text-orange-700',
    },
    {
      mode: 'union' as TutorialMode,
      icon: '🔀',
      iconBg: 'bg-cyan-100',
      iconText: 'text-cyan-600',
      title: 'Union Model',
      description:
        'Combine multiple tables with the same structure into a single dataset using UNION operations.',
      duration: '~4 mins',
      durationBg: 'bg-cyan-100',
      durationText: 'text-cyan-700',
      level: 'Intermediate',
      levelBg: 'bg-orange-100',
      levelText: 'text-orange-700',
    },
    {
      mode: 'rollup' as TutorialMode,
      icon: '📊',
      iconBg: 'bg-indigo-100',
      iconText: 'text-indigo-600',
      title: 'Rollup Model',
      description:
        'Aggregate data across different dimensions to create summary views and reports.',
      duration: '~4 mins',
      durationBg: 'bg-indigo-100',
      durationText: 'text-indigo-700',
      level: 'Advanced',
      levelBg: 'bg-red-100',
      levelText: 'text-red-700',
    },
    {
      mode: 'lookback' as TutorialMode,
      icon: '⏱️',
      iconBg: 'bg-amber-100',
      iconText: 'text-amber-600',
      title: 'Lookback Model',
      description:
        'Create time-based analyses with historical data windows and temporal aggregations.',
      duration: '~4 mins',
      durationBg: 'bg-amber-100',
      durationText: 'text-amber-700',
      level: 'Advanced',
      levelBg: 'bg-red-100',
      levelText: 'text-red-700',
    },
  ];

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black bg-opacity-50">
      <div className="bg-card rounded-lg shadow-xl max-w-4xl w-full mx-4 p-6 max-h-[90vh] overflow-y-auto">
        <h2 className="text-2xl font-bold text-foreground mb-4">
          Choose Your Tutorial
        </h2>
        <p className="text-muted-foreground mb-6">
          Select a tutorial to learn how to create different types of models.
        </p>

        <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mb-6">
          {tutorials.map((tutorial) => (
            <button
              key={tutorial.mode}
              onClick={() => setSelectedMode(tutorial.mode)}
              className={`p-4 rounded-lg border-2 transition-all text-left ${
                selectedMode === tutorial.mode
                  ? 'border-primary bg-primary/10'
                  : 'border-border hover:border-primary/50'
              }`}
            >
              <div className="flex items-start gap-3">
                <div
                  className={`flex-shrink-0 w-8 h-8 ${tutorial.iconBg} rounded-full flex items-center justify-center`}
                >
                  <span className={`${tutorial.iconText} text-lg`}>
                    {tutorial.icon}
                  </span>
                </div>
                <div className="flex-1">
                  <div className="flex items-center gap-1.5 mb-2">
                    <h3 className="font-semibold text-foreground">
                      {tutorial.title}
                    </h3>
                    <Tooltip align="center" content={tutorial.description}>
                      <InformationCircleIcon className="w-4 h-4 text-muted-foreground hover:text-foreground transition-colors" />
                    </Tooltip>
                  </div>
                  <div className="flex items-center gap-2">
                    <span
                      className={`text-xs ${tutorial.durationBg} ${tutorial.durationText} px-2 py-1 rounded`}
                    >
                      {tutorial.duration}
                    </span>
                    <span
                      className={`text-xs ${tutorial.levelBg} ${tutorial.levelText} px-2 py-1 rounded`}
                    >
                      {tutorial.level}
                    </span>
                  </div>
                </div>
              </div>
            </button>
          ))}
        </div>

        {/* Action Buttons */}
        <div className="flex justify-end gap-3">
          <button
            onClick={onCancel}
            className="px-4 py-2 text-muted-foreground hover:text-foreground transition-colors"
          >
            Cancel
          </button>
          <Button
            label="Start Tutorial"
            variant="primary"
            onClick={() => onSelectTutorial(selectedMode)}
            className="px-6 py-2 rounded-lg hover:bg-primary/90 transition-colors font-medium"
          ></Button>
        </div>

        {/* Info Footer */}
        <div className="mt-4 p-3 bg-muted rounded-lg">
          <p className="text-xs text-muted-foreground">
            💡 <strong>Tip:</strong> The tutorial will guide you through
            creating a model with example data. You can restart it anytime from
            the header menu.
          </p>
        </div>
      </div>
    </div>
  );
}
