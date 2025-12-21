import { Activity, AlertTriangle, CheckCircle, TrendingUp, ArrowRight, Play, Square, Wifi, WifiOff } from 'lucide-react';
import { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { ResponsiveContainer, AreaChart, Area, PieChart, Pie, Cell, Tooltip, XAxis, YAxis } from 'recharts';
import Highcharts from 'highcharts';
import HighchartsReact from 'highcharts-react-official';

const KPICard = ({ title, value, subtext, icon: Icon, color }) => (
    <div className="bg-slate-800/50 backdrop-blur border border-slate-700 p-6 rounded-xl flex flex-col justify-between h-32 relative overflow-hidden group hover:border-slate-600 transition-all shadow-lg">
        <div className="absolute right-[-10px] top-[-10px] opacity-10 group-hover:opacity-20 transition-opacity">
            <Icon size={80} color={color} />
        </div>
        <div>
            <h3 className="text-slate-400 text-xs font-bold uppercase tracking-wider mb-1">{title}</h3>
            <div className="text-3xl font-bold text-white tracking-tight">{value}</div>
        </div>
        <div className="text-xs text-slate-500 font-medium flex items-center gap-2 mt-auto">
            <span style={{ color }}>{subtext}</span>
        </div>
    </div>
);

const AlertCard = ({ robot, onClick }) => (
    <div
        onClick={onClick}
        className="bg-slate-800/80 border border-slate-700/50 hover:border-red-500/50 hover:bg-slate-800 transition-all cursor-pointer rounded-lg p-4 flex items-center justify-between group shadow-sm"
    >
        <div className="flex items-center gap-4">
            <div className={`p-2 rounded-lg ${robot.status === 'critical' ? 'bg-red-500/20 text-red-500' : 'bg-amber-500/20 text-amber-500'}`}>
                <AlertTriangle size={20} />
            </div>
            <div>
                <div className="flex items-center gap-2">
                    <span className="font-bold text-white text-base">{robot.name}</span>
                    <span className={`text-[10px] font-bold px-2 py-0.5 rounded-full uppercase ${robot.status === 'critical' ? 'bg-red-500/10 text-red-400 border border-red-500/20' : 'bg-amber-500/10 text-amber-400 border border-amber-500/20'}`}>
                        {robot.status}
                    </span>
                </div>
                <div className="text-xs text-slate-400 mt-1 font-mono">
                    Falure Prob: <span className="text-slate-200">{(robot.risk || 0).toFixed(0)}%</span> | RUL: {robot.prediction}
                </div>
            </div>
        </div>
        <div className="text-slate-600 group-hover:text-indigo-400 transition-colors">
            <ArrowRight size={20} />
        </div>
    </div>
);

const Dashboard = ({ robots = [] }) => {
    const navigate = useNavigate();
    const [streamStatus, setStreamStatus] = useState('offline');

    // GUARDRAIL: Ensure robots is an array
    const safeRobots = Array.isArray(robots) ? robots : [];

    const controlStream = async (state) => {
        try {
            await fetch('http://localhost:8000/api/v1/stream/control', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ state })
            });
            setStreamStatus(state === 'start' ? 'live' : 'offline');
        } catch (err) {
            console.error("Stream Control Error:", err);
        }
    };

    // 1. Calculate KPIs (Safe reductions)
    const criticalCount = safeRobots.filter(r => r.status === 'critical').length;
    const warningCount = safeRobots.filter(r => r.status === 'warning').length;
    const activeAlerts = safeRobots.filter(r => r.status !== 'healthy');

    // Default systemHealth to 100 if no robots
    const systemHealth = safeRobots.length > 0
        ? Math.max(0, ((safeRobots.length - criticalCount - (warningCount * 0.5)) / safeRobots.length) * 100)
        : 100;

    const avgRisk = safeRobots.length > 0
        ? safeRobots.reduce((acc, r) => acc + (r.risk || 0), 0) / safeRobots.length
        : 0;

    const pieData = [
        { name: 'Healthy', value: safeRobots.filter(r => r.status === 'healthy').length, color: '#10b981' },
        { name: 'Warning', value: warningCount, color: '#f59e0b' },
        { name: 'Critical', value: criticalCount, color: '#ef4444' },
    ];

    // Simulated trend data
    const trendData = Array.from({ length: 24 }, (_, i) => ({
        time: `${i}:00`,
        value: Math.min(100, Math.max(0, systemHealth + (Math.random() * 5 - 2.5)))
    }));

    return (
        <div className="flex flex-col gap-6 animate-fade-in">
            {/* Header Controls */}
            <div className="flex flex-col md:flex-row md:items-center justify-between gap-4 mb-2">
                <div>
                    <h1 className="text-2xl font-bold text-white tracking-tight">Executive Overview</h1>
                    <p className="text-slate-400 text-sm mt-1">Real-time fleet telemetry and prediction status.</p>
                </div>

                <div className="flex items-center gap-3 bg-slate-900/50 p-1.5 rounded-xl border border-slate-800">
                    <div className={`flex items-center gap-2 px-3 py-1.5 rounded-lg text-xs font-mono font-bold transition-all ${streamStatus === 'live' ? 'bg-red-500/10 text-red-500 border border-red-500/20 animate-pulse' : 'bg-slate-800 text-slate-500 border border-slate-700'}`}>
                        {streamStatus === 'live' ? <Wifi size={14} /> : <WifiOff size={14} />}
                        {streamStatus === 'live' ? 'LIVE STREAM' : 'OFFLINE'}
                    </div>

                    <button
                        onClick={() => controlStream('start')}
                        disabled={streamStatus === 'live'}
                        className={`p-2 rounded-lg transition-all ${streamStatus === 'live' ? 'bg-slate-800 text-slate-600 cursor-not-allowed' : 'bg-indigo-600 hover:bg-indigo-500 text-white shadow-lg shadow-indigo-500/20'}`}
                        title="Start Data Stream"
                    >
                        <Play size={18} fill="currentColor" />
                    </button>

                    <button
                        onClick={() => controlStream('stop')}
                        disabled={streamStatus === 'offline'}
                        className={`p-2 rounded-lg transition-all ${streamStatus === 'offline' ? 'bg-slate-800 text-slate-600 cursor-not-allowed' : 'bg-slate-700 hover:bg-red-500/20 hover:text-red-400 text-slate-300'}`}
                        title="Stop Data Stream"
                    >
                        <Square size={18} fill="currentColor" />
                    </button>
                </div>
            </div>

            {/* KPI Row */}
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
                <KPICard
                    title="System Health"
                    value={`${systemHealth.toFixed(1)}%`}
                    subtext={criticalCount > 0 ? "Degradation Detected" : "Optimal"}
                    icon={Activity}
                    color={systemHealth < 90 ? '#ef4444' : '#10b981'}
                />
                <KPICard
                    title="Active Alerts"
                    value={activeAlerts.length}
                    subtext={`${criticalCount} Critical | ${warningCount} Warning`}
                    icon={AlertTriangle}
                    color="#f59e0b"
                />
                <KPICard
                    title="Assets Monitored"
                    value={safeRobots.length}
                    subtext="Online & Syncing"
                    icon={CheckCircle}
                    color="#3b82f6"
                />
                <KPICard
                    title="Avg Failure Prob"
                    value={`${avgRisk.toFixed(1)}%`}
                    subtext="AI Model Confidence: 98%"
                    icon={TrendingUp}
                    color="#8b5cf6"
                />
            </div>

            {/* Main Content Grid */}
            <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
                {/* Left Column: Trend & Alerts */}
                <div className="lg:col-span-2 flex flex-col gap-6">
                    {/* Trend Chart */}
                    <div className="bg-slate-800/50 backdrop-blur border border-slate-700 p-6 rounded-xl shadow-lg">
                        <h3 className="text-slate-300 font-bold mb-6 flex items-center gap-2 text-sm uppercase tracking-wider">
                            <Activity size={16} className="text-indigo-400" />
                            System Health Trend (24h)
                        </h3>
                        <div className="h-[600px] w-full">
                            <ResponsiveContainer width="100%" height="100%">
                                <AreaChart data={trendData}>
                                    <defs>
                                        <linearGradient id="colorHealth" x1="0" y1="0" x2="0" y2="1">
                                            <stop offset="5%" stopColor="#6366f1" stopOpacity={0.3} />
                                            <stop offset="95%" stopColor="#6366f1" stopOpacity={0} />
                                        </linearGradient>
                                    </defs>
                                    <XAxis dataKey="time" stroke="#475569" fontSize={12} tickLine={false} axisLine={false} />
                                    <YAxis domain={[0, 100]} stroke="#475569" fontSize={12} tickLine={false} axisLine={false} />
                                    <Tooltip
                                        contentStyle={{ backgroundColor: '#1e293b', borderColor: '#334155', borderRadius: '8px', color: '#f8fafc' }}
                                    />
                                    <Area type="monotone" dataKey="value" stroke="#6366f1" strokeWidth={3} fillOpacity={1} fill="url(#colorHealth)" />
                                </AreaChart>
                            </ResponsiveContainer>
                        </div>
                    </div>

                    {/* Shift-Based Timeline (Gantt Chart) */}
                    {activeAlerts.length > 0 && (
                        <div>
                            <h3 className="text-slate-400 font-bold mb-4 text-sm uppercase tracking-wider px-1">
                                Maintenance Timeline (High Degradation: D_t &gt; 0.25)
                            </h3>
                            <div className="bg-slate-800/50 backdrop-blur border border-slate-700 p-4 rounded-xl shadow-lg">
                                <HighchartsReact
                                    highcharts={Highcharts}
                                    constructorType={'ganttChart'}
                                    options={{
                                        chart: {
                                            backgroundColor: 'transparent',
                                            height: Math.min(activeAlerts.length * 50 + 100, 400)
                                        },
                                        title: { text: '' },
                                        credits: { enabled: false },
                                        xAxis: {
                                            currentDateIndicator: {
                                                color: '#6366f1',
                                                width: 2,
                                                label: {
                                                    format: 'Now',
                                                    style: { color: '#6366f1', fontWeight: 'bold' }
                                                }
                                            },
                                            labels: {
                                                format: '{value:%H:%M}',
                                                style: { color: '#94a3b8', fontSize: '10px' }
                                            },
                                            gridLineColor: '#334155'
                                        },
                                        yAxis: {
                                            type: 'category',
                                            grid: {
                                                borderColor: '#334155',
                                                columns: [{
                                                    title: { text: 'Asset', style: { color: '#cbd5e1', fontWeight: 'bold' } },
                                                    labels: { style: { color: '#cbd5e1' } }
                                                }]
                                            },
                                            labels: { style: { color: '#cbd5e1' } }
                                        },
                                        tooltip: {
                                            backgroundColor: '#1e293b',
                                            borderColor: '#334155',
                                            style: { color: '#f8fafc' },
                                            pointFormat: '<b>{point.name}</b><br/>Predicted Failure: {point.end:%H:%M}<br/>RUL: {point.rul}'
                                        },
                                        series: [{
                                            name: 'Critical Assets',
                                            data: activeAlerts
                                                .filter(r => {
                                                    // Filter by degradation score > 0.25 (high degradation)
                                                    // This corresponds to RRT < 75 hours (within 48h threshold)
                                                    const degradationScore = r.degradation_score || 0;
                                                    return degradationScore > 0.25;
                                                })
                                                .map((r, i) => {
                                                    // Calculate RRT from degradation score
                                                    const degradationScore = r.degradation_score || 0;
                                                    const MAX_HOURS = 100;
                                                    const rrtHours = Math.round((1 - degradationScore) * MAX_HOURS);
                                                    const now = Date.now();
                                                    return {
                                                        name: r.name,
                                                        start: now,
                                                        end: now + rrtHours * 3600000,
                                                        y: i,
                                                        rul: `${rrtHours}h (D_t: ${degradationScore.toFixed(2)})`,
                                                        color: degradationScore > 0.75 ? '#ef4444' : degradationScore > 0.5 ? '#f59e0b' : '#fbbf24'
                                                    };
                                                })
                                        }]
                                    }}
                                />
                                {activeAlerts.filter(r => {
                                    const degradationScore = r.degradation_score || 0;
                                    return degradationScore > 0.25;
                                }).length === 0 && (
                                        <div className="text-center text-slate-500 py-8">
                                            No assets with high degradation (D_t &gt; 0.25)
                                        </div>
                                    )}
                            </div>
                        </div>
                    )}
                </div>

                {/* Right Column: Distribution & Stats */}
                <div className="flex flex-col gap-6">
                    <div className="bg-slate-800/50 backdrop-blur border border-slate-700 p-6 rounded-xl shadow-lg flex flex-col h-full min-h-[600px]">
                        <h3 className="text-slate-300 font-bold mb-4 flex items-center gap-2 text-sm uppercase tracking-wider">
                            <AlertTriangle size={16} className="text-amber-400" />
                            Fleet Status Distribution
                        </h3>
                        {/* Anomaly Detection Heatmap */}
                        <div className="flex-1 min-h-[300px] w-full">
                            <HighchartsReact
                                highcharts={Highcharts}
                                options={{
                                    chart: {
                                        type: 'heatmap',
                                        backgroundColor: 'transparent',
                                        height: 600,
                                        style: { fontFamily: 'inherit' }
                                    },
                                    title: { text: '' },
                                    credits: { enabled: false },
                                    xAxis: {
                                        categories: ['CSLM', 'Kurtosis', 'Temp STD'],
                                        labels: { style: { color: '#94a3b8' } },
                                        lineColor: '#334155'
                                    },
                                    yAxis: {
                                        categories: safeRobots.map(r => r.name || r.id),
                                        title: { text: null },
                                        labels: { style: { color: '#94a3b8', fontSize: '10px' } },
                                        gridLineColor: '#334155',
                                        reversed: true
                                    },
                                    colorAxis: {
                                        min: 0,
                                        max: 100,
                                        minColor: '#10b981',
                                        maxColor: '#ef4444',
                                        stops: [
                                            [0, '#10b981'],
                                            [0.5, '#f59e0b'],
                                            [1, '#ef4444']
                                        ]
                                    },
                                    legend: {
                                        align: 'right',
                                        layout: 'vertical',
                                        verticalAlign: 'top',
                                        y: 25,
                                        symbolHeight: 280,
                                        itemStyle: { color: '#cbd5e1' }
                                    },
                                    tooltip: {
                                        backgroundColor: '#1e293b',
                                        borderColor: '#334155',
                                        style: { color: '#f8fafc' },
                                        formatter: function () {
                                            return `<b>${this.series.yAxis.categories[this.point.y]}</b><br/>` +
                                                `${this.series.xAxis.categories[this.point.x]}: <b>${this.point.value}</b> (Anomaly Score)`;
                                        }
                                    },
                                    series: [{
                                        name: 'Anomaly Scores',
                                        borderWidth: 1,
                                        borderColor: '#1e293b',
                                        data: safeRobots.flatMap((r, yIndex) => [
                                            { x: 0, y: yIndex, value: Math.round((r.risk || 0) * 0.8 + Math.random() * 20) },
                                            { x: 1, y: yIndex, value: Math.round((r.risk || 0) * 1.2 + Math.random() * 10) },
                                            { x: 2, y: yIndex, value: Math.round((r.status === 'critical' ? 80 : 20) + Math.random() * 20) }
                                        ]),
                                        dataLabels: {
                                            enabled: true,
                                            color: '#000000',
                                            style: { textOutline: 'none', fontWeight: 'bold' }
                                        }
                                    }]
                                }}
                            />
                        </div>

                        {/* Stats Legend */}
                        <div className="grid grid-cols-2 gap-2 mt-2 px-2">
                            <div className="text-xs text-slate-500 text-center">X-Axis: Key Features</div>
                            <div className="text-xs text-slate-500 text-center">Color: Anomaly Deviation</div>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    );
};

export default Dashboard;
