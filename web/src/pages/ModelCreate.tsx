import type { Api } from '@shared/api/types';
import type { DbtProject } from '@shared/dbt/types';
import { FRAMEWORK_MODEL_TYPE_OPTIONS } from '@shared/framework/constants';
import { useMount } from '@web';
import { useApp } from '@web/context/app';
import { useEnvironment } from '@web/context/environment';
import { Alert, Button, DialogBox, Spinner } from '@web/elements';
import {
  Controller,
  FieldInputText,
  FieldSelectSingle,
  Form,
} from '@web/forms';
import _ from 'lodash';
import { useCallback, useMemo, useState } from 'react';
import { usePersistedForm } from '../hooks/usePersistedForm';
import { stateSync } from '../utils/stateSync';
import {
  BookmarkSquareIcon,
  QuestionMarkCircleIcon,
  TrashIcon,
} from '@heroicons/react/24/outline';
import { EXTERNAL_LINKS } from '@shared/web/constants';

type Values = Api<'framework-model-create'>['request'];

export function ModelCreate() {
  const { api } = useApp();
  const { vscode } = useEnvironment();

  const {
    control,
    formState: { errors },
    handleSubmit,
    setValue,
    watch,
    isLoading,
    reset,
  } = usePersistedForm<Values>({
    formType: 'model-create',
    autoSave: true,
    debounceMs: 500,
  });

  const [projects, setProjects] = useState<DbtProject[] | null>(null);
  const [success, setSuccess] = useState<string | null>(null);
  const [showDiscardConfirm, setShowDiscardConfirm] = useState(false);
  const [isProjectsLoading, setIsProjectsLoading] = useState(true);
  const projectName = watch('projectName');
  const type = watch('type');
  const group = watch('group');
  const topic = watch('topic');
  const name = watch('name');

  // Check if form has any values to determine if discard should be enabled
  const hasFormData = useMemo(() => {
    return !!(projectName || type || group || topic || name);
  }, [projectName, type, group, topic, name]);

  const project = useMemo(
    () => projects?.find((p) => p.name === projectName) || null,
    [projectName, projects],
  );
  const groupOptions = useMemo(
    () =>
      _.map(project?.manifest?.groups, (g) => {
        const value = g?.name || '';
        return { label: value, value };
      }),
    [project],
  );
  const projectOptions = useMemo(
    () =>
      _.map(projects, (p) => {
        const value = p.name;
        return { label: value, value };
      }),
    [projects],
  );

  const onSubmit = useCallback(
    async (values: Values) => {
      try {
        const resp = await api.post({
          type: 'framework-model-create',
          request: values,
        });
        setSuccess(resp);
      } catch (err) {
        console.error('ERROR CREATING MODEL:', err);
        throw err;
      }
    },
    [api],
  );

  const onClose = useCallback(() => {
    if (vscode) {
      // Send a message to the extension with a custom close type
      vscode.postMessage({
        type: 'close-panel',
        panelType: 'model-create',
      });
    } else {
      window.parent.postMessage(
        {
          type: 'close-panel',
          panelType: 'model-create',
        },
        '*',
      );
    }
  }, [vscode]);

  const discardReset = useCallback(() => {
    reset({
      projectName: '',
      type: undefined,
      group: '',
      topic: '',
      name: '',
    });

    void stateSync.clearState('model-create');

    setShowDiscardConfirm(false);

    onClose();
  }, [reset, onClose]);

  const onDiscard = () => {
    setShowDiscardConfirm(true);
  };

  const onSaveForLater = useCallback(() => {
    onClose();
  }, [onClose]);

  const onHelp = useCallback(() => {
    console.log('[ModelCreate] Opening help documentation');

    if (vscode) {
      // Send message to extension to open external URL
      vscode.postMessage({
        type: 'open-external-url',
        url: EXTERNAL_LINKS.documentation,
      });
    } else {
      // Fallback for non-VS Code environments
      window.open(EXTERNAL_LINKS.documentation, '_blank');
    }
  }, [vscode]);

  useMount(() => {
    void (async () => {
      try {
        setIsProjectsLoading(true);
        const _projects = await api.post({
          type: 'dbt-fetch-projects',
          request: null,
        });
        if (_projects.length === 1) {
          setValue('projectName', _projects[0].name);
        }
        setProjects(_projects);
      } catch (err) {
        console.error('ERROR FETCHING PROJECTS', err);
      } finally {
        setIsProjectsLoading(false);
      }
    })();
  });

  if (isLoading || isProjectsLoading) {
    return (
      <div className="flex flex-col items-center justify-center min-h-[400px] p-8">
        <Spinner
          size={48}
          label={
            isProjectsLoading ? 'Loading Your Model Form...' : 'Loading form...'
          }
        />
        <p className="text-gray-600 mt-4 text-center">
          {isProjectsLoading
            ? 'Fetching your dbt projects and required configurations.'
            : 'Preparing the model creation form with your saved data.'}
        </p>
      </div>
    );
  }

  if (success) {
    return (
      <Alert
        description={success}
        label="Model Created Successfully"
        variant="success"
      />
    );
  }

  return (
    <>
      {/* 
          Title: Header 
          TODO: Move this to common component when working on New UI
      */}
      <div className="px-4 py-4 flex justify-between items-center">
        <h1 className="text-2xl font-bold">Create Model</h1>
        <div className="flex items-center">
          <Button
            label="Discard"
            variant="iconButton"
            onClick={onDiscard}
            type="button"
            disabled={isProjectsLoading || !hasFormData}
            icon={<TrashIcon className="h-5 w-5" />}
            className="ring-0 px-3 cursor-pointer"
          />
          <div className="h-4 w-px bg-gray-400 mx-2"></div>
          <Button
            label="Save draft"
            variant="iconButton"
            type="button"
            icon={<BookmarkSquareIcon className="h-5 w-5" />}
            onClick={onSaveForLater}
            disabled={isProjectsLoading}
            className="ring-0 px-3 cursor-pointer"
          />
          <div className="h-4 w-px bg-gray-400 mx-2"></div>
          <Button
            label="Help"
            variant="iconButton"
            type="button"
            icon={<QuestionMarkCircleIcon className="h-5 w-5" />}
            onClick={onHelp}
            className="ring-0 px-3 cursor-pointer"
          />
        </div>
      </div>
      <Form<Values>
        handleSubmit={handleSubmit}
        labelSubmit="Create Model"
        onSubmit={onSubmit}
      >
        <Controller
          control={control}
          name="projectName"
          rules={{ required: 'Type is required' }}
          render={({ field }) => (
            <FieldSelectSingle
              {...field}
              error={errors.projectName}
              label="Select Project"
              options={projectOptions}
              disabled={isProjectsLoading}
              tooltipText="Select the project to create the model in. If you have only one project, it will be selected automatically."
            />
          )}
        />
        <Controller
          control={control}
          name="type"
          rules={{ required: 'Type is required' }}
          render={({ field }) => (
            <FieldSelectSingle
              {...field}
              error={errors.type}
              label="Select Model Type"
              options={FRAMEWORK_MODEL_TYPE_OPTIONS}
              disabled={isProjectsLoading}
              tooltipText="Choose the type of dbt model to create. This determines the model's structure and behavior in your data pipeline."
            />
          )}
        />
        <Controller
          control={control}
          name="group"
          rules={{ required: 'Group is required' }}
          render={({ field }) => (
            <FieldSelectSingle
              {...field}
              error={errors.group}
              label="Select Group"
              options={groupOptions}
              disabled={isProjectsLoading || !project}
              tooltipText="Groups help organize related models together. Select the appropriate group for your model."
            />
          )}
        />
        <Controller
          control={control}
          name="topic"
          rules={{
            required: 'Topic is required',
            pattern: {
              value: /^(?!.*(__|\/\/))(?![_/])[a-z0-9_/]+(?<![_/])$/,
              message:
                'Topic can only contain lowercase letters, numbers, underscores, and slashes. It cannot start or end with an underscore or slash. Only single underscores or slashes are allowed consecutively.',
            },
            maxLength: {
              value: 127,
              message: 'Topic must be 127 characters or fewer',
            },
          }}
          render={({ field }) => (
            <FieldInputText
              {...field}
              error={errors.topic}
              label="Enter Topic"
              disabled={isProjectsLoading}
              tooltipText="The topic represents the subject area of your model. Use lowercase letters, numbers, underscores, and slashes. Max 127 characters."
            />
          )}
        />
        <Controller
          control={control}
          name="name"
          rules={{
            required: 'Name is required',
            pattern: {
              value: /^(?!.*__)(?!_)[a-z0-9_]+(?<!_)$/,
              message:
                'Name can only contain lowercase letters, numbers, and underscores. It cannot start or end with an underscore. Only single underscores are allowed consecutively.',
            },
            maxLength: {
              value: 127,
              message: 'Name must be 127 characters or fewer',
            },
          }}
          render={({ field }) => (
            <FieldInputText
              {...field}
              error={errors.name}
              label="Enter Name"
              disabled={isProjectsLoading}
              tooltipText="The specific name for your model. This will be used as the model filename and reference. Use descriptive, lowercase names with underscores (e.g., 'monthly_revenue_summary'). Max 127 characters."
            />
          )}
        />
      </Form>
      <DialogBox
        title="Confirm Discard"
        open={showDiscardConfirm}
        description="Are you sure you want to discard this model?"
        confirmCTALabel="Discard"
        discardCTALabel="Cancel"
        onConfirm={() => discardReset()}
        onDiscard={() => setShowDiscardConfirm(false)}
      />
    </>
  );
}
