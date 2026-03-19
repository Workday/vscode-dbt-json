import './main.css';

import React from 'react';
import ReactDOM from 'react-dom/client';

import { EnvironmentProvider } from './context/environment';

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <EnvironmentProvider />
  </React.StrictMode>,
);
