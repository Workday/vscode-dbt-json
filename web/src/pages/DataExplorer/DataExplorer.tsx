import ModelLineage from '../ModelLineage';

/**
 * DataExplorer wrapper component
 * Currently routes to ModelLineage, future will include ColumnLineage
 */
export default function DataExplorer() {
  // For now, render ModelLineage directly
  // Future: Add tabs or navigation for ModelLineage / ColumnLineage
  return <ModelLineage />;
}
