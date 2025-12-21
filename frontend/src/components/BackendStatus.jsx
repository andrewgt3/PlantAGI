import React, { useState, useEffect } from 'react';
import { Server, Database, Activity, Wifi, CheckCircle, Database as DbIcon, Cpu } from 'lucide-react';

const StatusCard = ({ label, value, unit, icon: Icon, color, subtext }) => (
    <div className="bg-slate-800/50 backdrop-blur border border-slate-700 p-6 rounded-xl shadow-lg flex flex-col justify-between group hover:border-slate-600 transition-all">
        <div className="flex items-center justify-between mb-4">
            <h3 className="text-slate-400 text-xs font-bold uppercase tracking-wider">{label}</h3>
            <div className={`p-2 rounded-lg bg-slate-800/80 ${color}`}>
                <Icon size={20} />
            </div>
        </div>
        <div>
            <div className="text-3xl font-bold text-white tracking-tight">
                {value} <span className="text-sm text-slate-500 font-medium ml-1">{unit}</span>
            </div>
            <div className="text-xs text-slate-500 mt-2 font-medium">{subtext}</div>
        </div>
    </div>
);

const BackendStatus = () => {
    const [statusData, setStatusData] = useState(null);
    const [isLoading, setIsLoading] = useState(true);

    useEffect(() => {
        const fetchStatus = async () => {
            try {
                const res = await fetch('http://localhost:8000/api/v1/system/status');
                if (res.ok) {
                    const data = await res.json();
                    setStatusData(data);
                }
            } catch (err) {
                console.error("Failed to fetch backend status:", err);
            } finally {
                setIsLoading(false);
            }
        };

        fetchStatus();
        const interval = setInterval(fetchStatus, 2000); // Poll every 2s
        return () => clearInterval(interval);
    }, []);

    if (isLoading) {
        return <div className="p-8 text-center text-slate-500">Connecting to Backend Control Plane...</div>;
    }

    if (!statusData) {
        return <div className="p-8 text-center text-red-500">Backend Connection Failed</div>;
    }

    return (
        <div className="space-y-6 animate-fade-in pb-10">
            {/* Header */}
            <div>
                <h1 className="text-2xl font-bold text-white flex items-center gap-3">
                    Backend Transparency <span className="text-xs font-mono bg-emerald-500/10 text-emerald-500 px-2 py-1 rounded border border-emerald-500/20">SYSTEM ONLINE</span>
                </h1>
                <p className="text-slate-400 mt-1">Live performance metrics and data lineage mapping.</p>
            </div>

            {/* Metrics Grid */}
            <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
                <StatusCard
                    label="Redis Ingestion Rate"
                    value={statusData.redis_ingestion_rate}
                    unit="msg/s"
                    icon={Wifi}
                    color="text-indigo-400"
                    subtext="Sensor Telemetry Channel"
                />
                <StatusCard
                    label="TimescaleDB Lag"
                    value={statusData.timescaledb_lag_ms}
                    unit="ms"
                    icon={Database}
                    color={statusData.timescaledb_lag_ms < 1000 ? "text-emerald-400" : "text-amber-400"}
                    subtext="Write-to-Read Latency"
                />
                <StatusCard
                    label="Active Data Sources"
                    value={statusData.active_sources}
                    unit=""
                    icon={Server}
                    color="text-blue-400"
                    subtext="Triple Fusion: NASA + C-MAPSS + Prop."
                />
            </div>

            {/* Source Map Table */}
            <div className="bg-slate-800/50 backdrop-blur border border-slate-700 rounded-xl overflow-hidden shadow-lg">
                <div className="px-6 py-4 border-b border-slate-700 flex items-center justify-between bg-slate-900/40">
                    <h3 className="font-bold text-white flex items-center gap-2">
                        <DbIcon size={18} className="text-slate-400" />
                        Data Stream Transparency (Source Decomposition)
                    </h3>
                    <div className="text-xs text-slate-500">Updated: Just now</div>
                </div>
                <div className="overflow-x-auto">
                    <table className="w-full text-left border-collapse">
                        <thead>
                            <tr className="border-b border-slate-700/50 text-xs text-slate-400 uppercase tracking-wider bg-slate-900/20">
                                <th className="px-6 py-3 font-semibold">Data Stream Component</th>
                                <th className="px-6 py-3 font-semibold">Origin Source</th>
                                <th className="px-6 py-3 font-semibold">Purpose</th>
                                <th className="px-6 py-3 font-semibold">Latency</th>
                            </tr>
                        </thead>
                        <tbody className="divide-y divide-slate-700/50">
                            <tr className="hover:bg-slate-700/30 transition-colors text-sm text-slate-300">
                                <td className="px-6 py-4 font-bold text-white">Vibration Signals (RMS)</td>
                                <td className="px-6 py-4 text-emerald-400 font-mono">NASA IMS PCoE</td>
                                <td className="px-6 py-4">Bearing Fault Detection</td>
                                <td className="px-6 py-4 text-xs font-mono text-slate-500">~12ms</td>
                            </tr>
                            <tr className="hover:bg-slate-700/30 transition-colors text-sm text-slate-300">
                                <td className="px-6 py-4 font-bold text-white">Operational Context (Load/Temp)</td>
                                <td className="px-6 py-4 text-amber-400 font-mono">NASA C-MAPSS</td>
                                <td className="px-6 py-4">Engine State Simulation</td>
                                <td className="px-6 py-4 text-xs font-mono text-slate-500">~15ms</td>
                            </tr>
                            <tr className="hover:bg-slate-700/30 transition-colors text-sm text-slate-300">
                                <td className="px-6 py-4 font-bold text-white">Tool Wear & Criticality</td>
                                <td className="px-6 py-4 text-indigo-400 font-mono">Proprietary Synthesis</td>
                                <td className="px-6 py-4">RUL & Network Importance</td>
                                <td className="px-6 py-4 text-xs font-mono text-slate-500">Local</td>
                            </tr>
                        </tbody>
                    </table>
                </div>
            </div>
        </div>
    );
};

export default BackendStatus;
