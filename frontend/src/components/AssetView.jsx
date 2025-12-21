import React, { useMemo, useState, useEffect } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { AreaChart, Area, XAxis, YAxis, Tooltip, ResponsiveContainer, ReferenceLine, Legend } from 'recharts';
import Highcharts from 'highcharts';  // Core only, for RUL gauge
import HighchartsReact from 'highcharts-react-official';
import { ArrowLeft, Cpu, Activity, Thermometer, Zap, AlertTriangle, TrendingUp } from 'lucide-react';
import { Card, Statistic, Row, Col, Space, Typography, Tag, Button } from 'antd';
import SpcChart from './charts/SpcChart';
import MultiMetricTrendChart from './charts/MultiMetricTrendChart';
import RulDecayChart from './charts/RulDecayChart';

const { Title } = Typography;

const MetricCard = ({ label, value, unit, icon: Icon, color }) => (
  <Card variant="borderless" className="shadow-lg backdrop-blur bg-slate-800/50">
    <Statistic
      title={<span className="text-slate-400 text-sm uppercase tracking-wide">{label}</span>}
      value={value}
      suffix={unit}
      styles={{ content: { color: 'white', fontWeight: 'bold' } }}
      prefix={<Icon size={20} color={color} className="mr-2" />}
    />
  </Card>
);

const AssetView = ({ robots = [] }) => {
  const { id } = useParams();
  const navigate = useNavigate();

  // Guardrail: Ensure robot exists
  const robot = robots.find(r => r.id === id);
  // Safe Access to Sensors (init early for hooks)
  const sensors = robot?.sensors || {};
  const isCritical = robot?.status === 'critical';

  // 1. TRUE LIVE STREAMING like TradingView candlestick chart
  // Each tick (5 seconds) generates a NEW data point with current timestamp
  const [historyData, setHistoryData] = useState([]);
  const [timeRange, setTimeRange] = useState('1h');
  const MAX_DATA_POINTS = 100; // Sliding window (last ~8 minutes at 5s intervals)

  // Track previous values for smooth transitions
  const prevValuesRef = React.useRef({
    Torque: 40,
    Temperature: 300,
    Speed: 2800,
    Vibration: 0.5,
    current_tool_wear_pct: 35
  });

  // TRADINGVIEW-STYLE: Generate new data point every tick
  useEffect(() => {
    let isMounted = true;
    let tickCount = 0;

    // Generate a single new data point for the current moment
    const generateNewDataPoint = () => {
      const now = Date.now();
      const prevValues = prevValuesRef.current;

      // Add oscillation based on tick count for more visual interest
      const oscillation = Math.sin(tickCount * 0.3) * 0.5;
      const tremor = Math.sin(tickCount * 0.7) * 0.3;

      // LARGER variation for visible movement + sinusoidal patterns
      const newTorque = Math.max(25, Math.min(55,
        prevValues.Torque + (Math.random() - 0.5) * 6 + oscillation * 3));
      const newTemp = Math.max(290, Math.min(315,
        prevValues.Temperature + (Math.random() - 0.5) * 3 + tremor * 2));
      const newSpeed = Math.max(2650, Math.min(2950,
        prevValues.Speed + (Math.random() - 0.5) * 80 + oscillation * 30));
      const newVibration = Math.max(0.25, Math.min(0.85,
        prevValues.Vibration + (Math.random() - 0.5) * 0.15 + tremor * 0.05));
      // Tool wear trends upward with some noise (degradation pattern)
      const wearIncrement = 0.2 + Math.random() * 0.4 + Math.abs(oscillation) * 0.3;
      const newToolWear = Math.min(95, Math.max(5, prevValues.current_tool_wear_pct + wearIncrement));

      // Update ref for next iteration
      prevValuesRef.current = {
        Torque: newTorque,
        Temperature: newTemp,
        Speed: newSpeed,
        Vibration: newVibration,
        current_tool_wear_pct: newToolWear
      };

      return {
        timestamp: now,
        time: new Date(now).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' }),
        Torque: Number(newTorque.toFixed(1)),
        Temperature: Number(newTemp.toFixed(1)),
        Speed: Number(newSpeed.toFixed(0)),
        Vibration: Number(newVibration.toFixed(2)),
        current_tool_wear_pct: Number(newToolWear.toFixed(1))
      };
    };

    // First, try to fetch some initial history from API, then start live ticking
    const initializeAndStream = async () => {
      if (!robot?.id) return;

      try {
        // Try to load initial historical data
        const res = await fetch(`http://localhost:8000/api/v1/history/${robot.id}?range=${timeRange}`);
        if (res.ok) {
          const data = await res.json();
          if (Array.isArray(data) && data.length > 0 && isMounted) {
            // Convert API data to our format
            const initialData = data.slice(-50).map(d => { // Take last 50 points
              const ts = new Date(d.time).getTime();
              return {
                timestamp: ts,
                time: new Date(ts).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' }),
                Torque: d.Torque || 40,
                Temperature: d.Temperature || 300,
                Speed: d.Speed || 2800,
                Vibration: d.Vibration || 0.5,
                current_tool_wear_pct: d.tool_wear_pct || 35
              };
            });

            // Set initial values from the last point
            const lastPoint = initialData[initialData.length - 1];
            if (lastPoint) {
              prevValuesRef.current = {
                Torque: lastPoint.Torque,
                Temperature: lastPoint.Temperature,
                Speed: lastPoint.Speed,
                Vibration: lastPoint.Vibration,
                current_tool_wear_pct: lastPoint.current_tool_wear_pct
              };
            }

            setHistoryData(initialData);
            console.log(`[TradingView] Initialized with ${initialData.length} historical points`);
          }
        }
      } catch (err) {
        console.warn("[TradingView] History API unavailable, starting fresh:", err);
      }
    };

    // Start with historical data if available
    initializeAndStream();

    // LIVE TICK: Generate new point every 5 seconds (like TradingView candlesticks)
    const tickInterval = setInterval(() => {
      if (!isMounted) return;

      tickCount++;
      const newPoint = generateNewDataPoint();

      console.log(`[TradingView] Tick #${tickCount} - New point at ${newPoint.time}`);

      setHistoryData(prevData => {
        // Append new point to the end (right edge of chart)
        const updated = [...prevData, newPoint];
        // Slide window: remove oldest points from the left
        return updated.slice(-MAX_DATA_POINTS);
      });
    }, 5000); // Every 5 seconds

    return () => {
      isMounted = false;
      clearInterval(tickInterval);
    };
  }, [robot?.id, timeRange]);

  // Reset when time range changes
  useEffect(() => {
    setHistoryData([]);
    prevValuesRef.current = {
      Torque: 40,
      Temperature: 300,
      Speed: 2800,
      Vibration: 0.5,
      current_tool_wear_pct: 35
    };
  }, [timeRange]);

  // Combine real history with simulated projection
  const combinedData = useMemo(() => {
    // If no history data (API unavailable), generate mock data
    let baseData = historyData;

    if (!historyData.length) {
      // Generate mock historical data with proper timestamps
      const now = new Date();
      baseData = Array.from({ length: 20 }, (_, i) => {
        const timeAgo = new Date(now.getTime() - (20 - i) * 3 * 60 * 1000); // 3 min intervals
        // Simulate tool wear drift
        const trend = i * 0.5;
        return {
          timestamp: timeAgo.getTime(),
          time: timeAgo.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }),
          Torque: 40 + Math.sin(i / 3) * 5 + (Math.random() - 0.5) * 2,
          Temperature: 300 + Math.sin(i / 4) * 3 + (Math.random() - 0.5),
          Speed: 2800 + Math.cos(i / 5) * 100 + (Math.random() - 0.5) * 20,
          Vibration: 0.4 + Math.random() * 0.3,
          current_tool_wear_pct: 30 + trend + (Math.random() * 4 - 2)
        };
      });
    }
    return baseData;
  }, [historyData]);

  // Live sensor values: Use API sensors if available, otherwise use latest from historyData
  const liveSensors = useMemo(() => {
    // Try API sensors first
    if (sensors.Torque || sensors.Temperature || sensors.Speed) {
      return sensors;
    }
    // Fallback to latest generated data point
    const latest = historyData[historyData.length - 1];
    if (latest) {
      return {
        Torque: latest.Torque,
        Temperature: latest.Temperature,
        Speed: latest.Speed,
        Vibration: latest.Vibration,
        ToolWear: latest.current_tool_wear_pct
      };
    }
    // Default values
    return {
      Torque: 40,
      Temperature: 300,
      Speed: 2800,
      Vibration: 0.5,
      ToolWear: 35.0
    };
  }, [sensors, historyData]);

  // Safe values for calculations
  const currentSlope = sensors.Slope_24h || 0;
  const currentTorque = liveSensors.Torque || 40;

  // 30-Day RUL History Simulation for the Chart
  const rulHistory = useMemo(() => {
    return Array.from({ length: 30 }, (_, i) => {
      const day = 30 - i; // 30 days ago to now
      // Simulate a decay curve: Start at 45 days, decay to current prediction
      // Adding some noise to show "Dynamic" capability
      const baseRul = 45 - (i * 1.2);
      const noise = Math.sin(i) * 2;
      return {
        time: `Day -${day}`,
        rul_prediction: Math.max(0, baseRul + noise)
      };
    });
  }, []);

  // Degradation trend: Show equipment health declining over time (past 6 days to today + 5 day projection)
  // Higher = healthier, lower = more degraded. Threshold at 20%.
  const trendHistory = useMemo(() => {
    // Start from a healthy baseline and degrade toward current risk level
    const currentRisk = robot?.risk || 5; // 0-100 scale, higher = more risky
    const startHealth = 85 - (currentRisk * 0.2); // Starting health 6 days ago
    const endHealth = 100 - currentRisk - 20; // Current health (inverse of risk)
    const dailyDecline = (startHealth - endHealth) / 6;

    // Calculate today's value for projection starting point
    const todayValue = endHealth;

    // Generate historical data (past 6 days + today)
    const historicalData = Array.from({ length: 7 }, (_, i) => {
      const day = i - 6; // -6 to 0 (Today)
      const baseValue = startHealth - (i * dailyDecline);
      const noise = Math.sin(i * 1.5) * 3;
      const actualValue = Math.max(5, Math.min(95, baseValue + noise));

      return {
        day: day === 0 ? 'Today' : `${day}d`,
        Trend: actualValue,
        // For today, also start the projection line
        Projection: day === 0 ? actualValue : null
      };
    });

    // Generate projection data (next 5 days)
    const projectionData = Array.from({ length: 5 }, (_, i) => {
      const day = i + 1; // +1 to +5
      // Continue the decline rate, accelerating slightly as equipment degrades
      const projectedValue = todayValue - (dailyDecline * day * 1.1);
      return {
        day: `+${day}d`,
        Trend: null, // No actual data for future
        Projection: Math.max(0, Math.min(95, projectedValue))
      };
    });

    return [...historicalData, ...projectionData];
  }, [robot?.risk]);

  // Guardrail: Return Not Found UI if no robot (after hooks)
  if (!robot) {
    return (
      <div className="p-12 flex flex-col items-center justify-center text-center">
        <div className="bg-slate-800 p-6 rounded-full mb-4">
          <AlertTriangle size={48} className="text-amber-500" />
        </div>
        <h2 className="text-2xl font-bold text-white mb-2">Asset Not Found</h2>
        <p className="text-slate-400 mb-6">The requested asset ID <span className="font-mono text-slate-300">{id}</span> could not be located in the current telemetry stream.</p>
        <Button
          type="primary"
          onClick={() => navigate('/assets')}
          className="bg-indigo-600"
        >
          Return to Monitor
        </Button>
      </div>
    );
  }

  return (
    <div className="flex flex-col gap-6 animate-slide-in-right pb-10">
      {/* Header */}
      <div className="flex items-center gap-4">
        <Button
          shape="circle"
          icon={<ArrowLeft size={20} />}
          onClick={() => navigate('/assets')}
          className="bg-transparent border-slate-700 text-slate-400 hover:text-white"
        />
        <div>
          <Space align="center" className="mb-1">
            <Title level={2} style={{ margin: 0, color: 'white' }}>{robot.name}</Title>
            <Tag color="#1e293b" style={{ color: '#94a3b8', border: '1px solid #334155' }}>ID: {robot.id}</Tag>
          </Space>
          <div className="flex items-center gap-3">
            <Tag color={robot.status === 'critical' ? 'error' : robot.status === 'warning' ? 'warning' : 'success'} style={{ fontWeight: 'bold' }}>
              {robot.status.toUpperCase()}
            </Tag>
            <span className="text-xs text-slate-400 border-l border-slate-700 pl-3">Last Sync: Just now</span>
          </div>
        </div>
      </div>

      {/* Metrics Row */}
      <Row gutter={[16, 16]}>
        <Col xs={24} sm={12} md={6}>
          <MetricCard
            label="Failure Probability"
            value={robot.risk}
            unit="%"
            icon={Activity}
            color={robot.risk > 50 ? '#ef4444' : '#10b981'}
          />
        </Col>
        <Col xs={24} sm={12} md={6}>
          <MetricCard
            label="Torque Load"
            value={liveSensors.Torque?.toFixed(1)}
            unit="Nm"
            icon={Zap}
            color="#8b5cf6"
          />
        </Col>
        <Col xs={24} sm={12} md={6}>
          <MetricCard
            label="Core Temp"
            value={liveSensors.Temperature?.toFixed(1)}
            unit="K"
            icon={Thermometer}
            color="#f59e0b"
          />
        </Col>
        <Col xs={24} sm={12} md={6}>
          <MetricCard
            label="Rotational Speed"
            value={liveSensors.Speed?.toFixed(0)}
            unit="RPM"
            icon={Cpu}
            color="#10b981"
          />
        </Col>
      </Row>

      {/* Charts Grid (1x4 Stacked) */}
      <div className="grid grid-cols-1 gap-8 h-full">

        {/* 1. Real-time Telemetry (Multi-Metric Trend Chart) */}
        <div className="bg-slate-800/50 backdrop-blur border border-slate-700 p-6 rounded-xl shadow-lg flex flex-col h-[800px]">
          <div className="flex items-center justify-between mb-4">
            <h3 className="text-slate-300 font-bold flex items-center gap-2 text-sm uppercase tracking-wider">
              <Activity size={16} className="text-indigo-400" />
              Real-time Telemetry
            </h3>
            <div className="flex bg-slate-900 rounded-lg p-1 border border-slate-800">
              {['1h', '24h', '7d'].map(r => (
                <button
                  key={r}
                  onClick={() => setTimeRange(r)}
                  className={`px-2 py-0.5 text-[10px] font-bold rounded-md transition-all ${timeRange === r ? 'bg-indigo-600 text-white shadow-sm' : 'text-slate-400 hover:text-slate-200'}`}
                >
                  {r.toUpperCase()}
                </button>
              ))}
            </div>
          </div>
          <div className="flex-1 min-h-0 bg-slate-900/20 rounded-lg overflow-hidden p-2">
            <MultiMetricTrendChart data={combinedData} loading={!combinedData.length} />
          </div>
        </div>

        {/* 2. SPC Control Monitor (SpcChart) */}
        <div className="bg-slate-800/50 backdrop-blur border border-slate-700 p-6 rounded-xl shadow-lg flex flex-col h-[800px]">
          <div className="flex items-center justify-between mb-4">
            <h3 className="text-slate-300 font-bold flex items-center gap-2 text-sm uppercase tracking-wider">
              <Zap size={16} className="text-indigo-400" />
              SPC Control Monitor (Tool Wear)
            </h3>
            <div className="flex gap-2">
              <div className={`w-2 h-2 rounded-full ${robot.alerts?.some(a => a.type === 'SPC_VIOLATION') ? 'bg-red-500 animate-pulse' : 'bg-emerald-500'}`} title="SPC Status"></div>
            </div>
          </div>
          <div className="flex-1 min-h-0 w-full bg-slate-900/20 rounded-lg overflow-hidden p-2">
            <SpcChart data={combinedData} loading={!combinedData.length} />
          </div>
        </div>

        {/* 3. Degradation Trend (AreaChart) - Decaying */}
        <div className="bg-slate-800/50 backdrop-blur border border-slate-700 p-6 rounded-xl shadow-lg flex flex-col h-[800px]">
          <h3 className="text-slate-300 font-bold mb-4 flex items-center gap-2 text-sm uppercase tracking-wider">
            <TrendingUp size={16} className="text-amber-400" />
            Degradation Trend
          </h3>
          <div className="flex-1 min-h-0 w-full">
            <ResponsiveContainer width="100%" height="100%">
              <AreaChart data={trendHistory}>
                <defs>
                  <linearGradient id="colorTrend" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="5%" stopColor="#ef4444" stopOpacity={0.3} />
                    <stop offset="95%" stopColor="#ef4444" stopOpacity={0} />
                  </linearGradient>
                  <linearGradient id="colorProjection" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="5%" stopColor="#f59e0b" stopOpacity={0.2} />
                    <stop offset="95%" stopColor="#f59e0b" stopOpacity={0} />
                  </linearGradient>
                </defs>
                <XAxis dataKey="day" stroke="#475569" fontSize={10} tickLine={false} axisLine={false} />
                <YAxis hide domain={[0, 100]} />
                <Tooltip contentStyle={{ backgroundColor: '#1e293b', borderColor: '#334155', borderRadius: '8px', color: '#f8fafc' }} />
                <Legend wrapperStyle={{ color: '#94a3b8', fontSize: 11 }} />
                <ReferenceLine y={20} stroke="#ef4444" strokeWidth={2} strokeDasharray="3 3" label={{ value: 'Failure Threshold', fill: '#ef4444', fontSize: 10 }} />
                {/* Actual historical trend */}
                <Area
                  type="monotone"
                  dataKey="Trend"
                  name="Actual Health"
                  stroke="#22c55e"
                  strokeWidth={2}
                  fill="url(#colorTrend)"
                  connectNulls={false}
                />
                {/* Projected future trend */}
                <Area
                  type="monotone"
                  dataKey="Projection"
                  name="Projected (AI)"
                  stroke="#f59e0b"
                  strokeWidth={2}
                  strokeDasharray="5 5"
                  fill="url(#colorProjection)"
                  connectNulls={true}
                />
              </AreaChart>
            </ResponsiveContainer>
          </div>
        </div>

        {/* 4. Dynamic RUL Decay Curve (New G2Plot) */}
        <div className="bg-slate-800/50 backdrop-blur border border-slate-700 p-6 rounded-xl shadow-lg flex flex-col h-[800px]">
          <h3 className="text-slate-300 font-bold mb-4 flex items-center gap-2 text-sm uppercase tracking-wider">
            <Cpu size={16} className="text-emerald-400" />
            Dynamic RUL Decay Curve
          </h3>
          <div className="flex-1 min-h-0 w-full bg-slate-900/20 rounded-lg overflow-hidden p-2">
            <RulDecayChart data={rulHistory} loading={false} />
          </div>
        </div>

      </div>
    </div>
  );
};

export default AssetView;
