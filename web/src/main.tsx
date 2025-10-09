import React from 'react';
import ReactDOM from 'react-dom/client';

import './main.css';
import { EnvironmentProvider } from './context/environment';

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <EnvironmentProvider />
  </React.StrictMode>,
);
