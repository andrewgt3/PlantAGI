import React, { useState, useEffect, Suspense } from "react";
import { BrowserRouter as Router, Routes, Route } from "react-router-dom";
import Layout from "./components/Layout";
import Dashboard from "./components/Dashboard";
import AssetMonitor from "./components/AssetMonitor";
import LoadingOverlay from "./components/LoadingOverlay";
import ErrorBoundary from "./components/ErrorBoundary";
import "./App.css";
// Import pre-initialized Highcharts
import './highcharts-init.js';

const AssetView = React.lazy(() => import("./components/AssetView"));
const ModelAudit = React.lazy(() => import("./components/ModelAudit"));
const BackendStatus = React.lazy(() => import("./components/BackendStatus"));

const MACHINE_IDS = [
  "H29424", "H29425", "H29432", "H29434", "H29441", "H29452", "H29457", "H29462"
];

import { ConfigProvider } from 'antd';
import enUS from 'antd/locale/en_US';
import themeConfig from './theme/themeConfig';

function App() {
  return (
    <ErrorBoundary>
      <ConfigProvider theme={themeConfig} locale={enUS}>
        <AppContent />
      </ConfigProvider>
    </ErrorBoundary>
  );
}

function AppContent() {
  const [robotsData, setRobotsData] = useState([]);
  const [isDataLoading, setIsDataLoading] = useState(true);

  // Robust Data Fetching with Guardrails
  useEffect(() => {
    let isMounted = true;
    console.log('[App] Starting data fetch...');

    const fetchData = async () => {
      try {
        console.log('[App] Fetching data for', MACHINE_IDS.length, 'machines');
        // Fetch in parallel for all machines
        const promises = MACHINE_IDS.map(async (id) => {
          try {
            const res = await fetch(`http://localhost:8000/api/v1/predict/machine/${id}`);
            if (!res.ok) throw new Error(`HTTP ${res.status}`);
            return await res.json();
          } catch (e) {
            console.warn(`Failed to fetch for ${id}:`, e);
            // Fallback for individual machine failure to prevent entire dashboard crash
            return {
              machine_id: id,
              status: "unknown",
              failure_probability: 0,
              rul_prediction: 0,
              sensor_data: {}
            };
          }
        });

        const results = await Promise.all(promises);
        console.log('[App] Fetched results:', results.length);

        if (!isMounted) return;

        const formattedData = results.map((r, i) => {
          // GUARDRAIL: Ensure 'r' is an object
          const safeR = r || {};

          let status = "healthy";
          // Helper to safely check string inclusion (Case Insensitive)
          const safeStatus = (safeR.status || "").toString().toLowerCase();

          if (safeStatus.includes("risk") || (safeR.failure_probability || 0) > 0.5) {
            status = "critical";
          } else if (safeStatus.includes("warn")) {
            status = "warning";
          } else if (safeStatus === "unknown") {
            status = "unknown";
          }

          return {
            id: safeR.machine_id || MACHINE_IDS[i],
            name: `Robot ${i + 1}`, // Clean display name
            status: status,
            risk: safeR.failure_probability
              ? Math.round(safeR.failure_probability * 100)
              : 0,
            prediction: safeR.rul_prediction
              ? `${Math.max(1, Math.round((safeR.rul_prediction < 1 ? safeR.rul_prediction * 4000 : safeR.rul_prediction) / 24))} Days`
              : "--",
            degradation_score: safeR.degradation_score || 0,
            details: safeStatus,
            sensors: safeR.sensor_data || {}, // GUARDRAIL: Default to empty object
          };
        });

        console.log('[App] Setting robots data:', formattedData.length, 'robots');
        setRobotsData(formattedData);
      } catch (err) {
        console.error("Critical API Fetch Error:", err);
        // Do not clear data on transient error, keep stale data if available
      } finally {
        if (isMounted) {
          console.log('[App] Setting isDataLoading to false');
          setIsDataLoading(false);
        }
      }
    };

    fetchData();
    const interval = setInterval(fetchData, 30000); // Changed to 30s to reduce noise
    return () => {
      isMounted = false;
      clearInterval(interval);
    };
  }, []);

  return (
    <Router>
      <Layout>
        {isDataLoading && <LoadingOverlay message="Syncing Telemetry..." />}
        <Routes>
          <Route path="/" element={<AssetMonitor robots={robotsData} />} />
          {/* <Route path="/assets" element={<AssetMonitor robots={robotsData} />} /> */}
          <Route path="/assets/:id" element={
            <Suspense fallback={<LoadingOverlay message="Loading Asset Context..." />}>
              <AssetView robots={robotsData} />
            </Suspense>
          } />
          <Route path="/audit" element={
            <Suspense fallback={<LoadingOverlay message="Auditing Models..." />}>
              <ModelAudit />
            </Suspense>
          } />
          <Route path="/status" element={
            <Suspense fallback={<LoadingOverlay message="Connecting to Backend..." />}>
              <BackendStatus />
            </Suspense>
          } />
          <Route path="*" element={<AssetMonitor robots={robotsData} />} />
        </Routes>
      </Layout>
    </Router>
  );
}

export default App;
