import { InputField, Prompt } from '@services/agent/decorators';

@Prompt(`

## Purpose
* Create a model json for getting started with new model development

### Validation Rules
- Parameters marked required are mandatory. If they are not present, please ask the user for input and follow the order in which they are defined, mostly as space or comma separated.

## Model Template

\`\`\`json
{
    "type": "<type>",
    "group": "<group>",
    "topic": "<topic>",
    "name": "<name>",
    "tags": [],
    "from": {},
    "select": []
}
\`\`\`

## File Creation
- All the model files are created under \`dags/dbt/models\` directory. The placement and name of the file depends on input parameters.
- A typical model name will be {<type>.split("_")[0]}__<group>__<topic>__<name>.model.json}.
- So the file is naturally created in the following directory path {<type>.split("_")[0]}/<group>/<topic>}
`)
export class CreateModelSignature {
  @InputField('Type of the model - mart, staging, intermediate, source, etc.')
  type: string | undefined;

  @InputField('Group of the model - analytics, finops, sales, marketing, etc.')
  group: string | undefined;

  @InputField('Topic of the model - aws_cur, gcp_billing, salesforce, etc.')
  topic: string | undefined;

  @InputField(
    'Name of the model - accounts_billing_daily, opportunities_facts, etc.',
  )
  name: string | undefined;
}
