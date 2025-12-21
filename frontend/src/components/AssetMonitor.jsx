import React, { useEffect, useRef, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { ArrowRight, Activity, AlertTriangle, CheckCircle, TrendingUp } from 'lucide-react';
import { Card, Tag, Progress, Button, Row, Col, Statistic, Space, Typography, Tabs } from 'antd';
import { ProTable } from '@ant-design/pro-components';
import Highcharts from 'highcharts';
import HighchartsReact from 'highcharts-react-official';
import RCA_DependencyGraph from './RCA_DependencyGraph';
import FleetTreemap from './charts/FleetTreemap';
import { LayoutGrid } from 'lucide-react';

// Gantt module is initialized in highcharts-init.js which is imported in App.jsx

const { Title, Text } = Typography;

const AssetMonitor = ({ robots = [] }) => {
    const navigate = useNavigate();

    // Guardrail
    const safeRobots = Array.isArray(robots) ? robots : [];

    // --- KPI CALCULATIONS ---
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

    // Filter for Gantt Chart (High Degradation)
    const ganttData = activeAlerts
        .filter(r => {
            const degradationScore = r.degradation_score || 0;
            return degradationScore > 0.25;
        })
        .map((r, i) => {
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
        });

    // ProTable Columns
    const columns = [
        {
            title: 'Asset Identity',
            dataIndex: 'name',
            key: 'name',
            copyable: true,
            render: (dom, entity) => (
                <div onClick={() => navigate(`/assets/${entity.id}`)} className="cursor-pointer">
                    <div className="font-bold text-base text-white">{entity.name}</div>
                    <div className="text-xs text-slate-500 font-mono mt-0.5">{entity.id}</div>
                </div>
            ),
        },
        {
            title: 'Live Status',
            dataIndex: 'status',
            key: 'status',
            valueType: 'select',
            valueEnum: {
                healthy: { text: 'Healthy', status: 'Success' },
                warning: { text: 'Warning', status: 'Warning' },
                critical: { text: 'Critical', status: 'Error' },
            },
            render: (_, record) => {
                let color = 'success';
                let status = record.status;
                if (status === 'critical') color = 'error';
                if (status === 'warning') color = 'warning';
                return (
                    <Tag color={color} key={status} style={{ fontWeight: 'bold' }}>
                        {status ? status.toUpperCase() : 'UNKNOWN'}
                    </Tag>
                );
            },
        },
        {
            title: 'Failure Probability',
            dataIndex: 'risk',
            key: 'risk',
            sorter: (a, b) => a.risk - b.risk,
            render: (_, record) => (
                <div style={{ width: 180 }}>
                    <Progress
                        percent={record.risk}
                        size="small"
                        status={record.risk > 50 ? 'exception' : 'active'}
                        strokeColor={record.risk > 50 ? '#ef4444' : record.risk > 20 ? '#f59e0b' : '#10b981'}
                        format={percent => `${percent}%`}
                    />
                </div>
            )
        },
        {
            title: 'RUL Prediction',
            dataIndex: 'prediction',
            key: 'prediction',
            render: (text) => <Text code>{text || '--'}</Text>
        },
        {
            title: 'Action',
            key: 'action',
            valueType: 'option',
            align: 'right',
            render: (_, record) => (
                <Button
                    type="default"
                    shape="circle"
                    icon={<ArrowRight size={16} />}
                    onClick={(e) => { e.stopPropagation(); navigate(`/assets/${record.id}`); }}
                />
            ),
        },
    ];

    return (
        <div className="flex flex-col gap-6 animate-fade-in pb-10">
            {/* Header */}
            <div>
                <Title level={2} style={{ margin: 0, color: 'white' }}>Asset Monitor</Title>
                <Text type="secondary">Unified fleet telemetry and maintenance dashboard.</Text>
            </div>

            {/* KPI Row */}
            <Row gutter={[16, 16]}>
                <Col xs={24} sm={12} lg={6}>
                    <Card variant="borderless" className="shadow-lg backdrop-blur bg-slate-800/50">
                        <Statistic
                            title="System Health"
                            value={systemHealth}
                            precision={1}
                            styles={{ content: { color: systemHealth < 90 ? '#ef4444' : '#10b981' } }}
                            prefix={<Activity size={20} className="mr-2" />}
                            suffix="%"
                        />
                        <div className="text-xs text-slate-500 mt-2">
                            {criticalCount > 0 ? "Degradation Detected" : "Optimal Performance"}
                        </div>
                    </Card>
                </Col>
                <Col xs={24} sm={12} lg={6}>
                    <Card variant="borderless" className="shadow-lg backdrop-blur bg-slate-800/50">
                        <Statistic
                            title="Active Alerts"
                            value={activeAlerts.length}
                            styles={{ content: { color: '#f59e0b' } }}
                            prefix={<AlertTriangle size={20} className="mr-2" />}
                        />
                        <div className="text-xs text-slate-500 mt-2">
                            {criticalCount} Critical | {warningCount} Warning
                        </div>
                    </Card>
                </Col>
                <Col xs={24} sm={12} lg={6}>
                    <Card variant="borderless" className="shadow-lg backdrop-blur bg-slate-800/50">
                        <Statistic
                            title="Avg Failure Prob"
                            value={avgRisk}
                            precision={1}
                            styles={{ content: { color: '#8b5cf6' } }}
                            prefix={<TrendingUp size={20} className="mr-2" />}
                            suffix="%"
                        />
                        <div className="text-xs text-slate-500 mt-2">AI Confidence: 98%</div>
                    </Card>
                </Col>
                <Col xs={24} sm={12} lg={6}>
                    <Card variant="borderless" className="shadow-lg backdrop-blur bg-slate-800/50">
                        <Statistic
                            title="Maintenance Due"
                            value={safeRobots.length}
                            styles={{ content: { color: '#3b82f6' } }}
                            prefix={<CheckCircle size={20} className="mr-2" />}
                        />
                        <div className="text-xs text-slate-500 mt-2">Online & Syncing</div>
                    </Card>
                </Col>
            </Row>

            {/* Visualizations: Graph (Topology) & Heatmap */}
            <Row gutter={[24, 24]}>
                {/* 1. Topology Graph (Replaces Gantt as Primary View) */}
                <Col xs={24} lg={16}>
                    {/* We can use Tabs to switch between Graph and Gantt if desired, or just show Graph */}
                    <Tabs
                        defaultActiveKey="1"
                        type="card"
                        items={[
                            {
                                key: '1',
                                label: <Space><Activity size={14} /> Plant Topology</Space>,
                                children: <RCA_DependencyGraph robots={safeRobots} />
                            },
                            {
                                key: '2',
                                label: <Space><Activity size={14} /> Maintenance Gantt</Space>,
                                children: (
                                    <div className="bg-slate-800/50 backdrop-blur p-4 rounded-xl h-[600px]">
                                        {ganttData.length > 0 ? (
                                            <HighchartsReact
                                                highcharts={Highcharts}
                                                constructorType={'ganttChart'}
                                                options={{
                                                    chart: { backgroundColor: 'transparent', height: 550, style: { fontFamily: 'inherit' } },
                                                    title: { text: '' },
                                                    credits: { enabled: false },
                                                    xAxis: {
                                                        currentDateIndicator: { color: '#6366f1', width: 2, label: { format: 'Now', style: { color: '#6366f1', fontWeight: 'bold' } } },
                                                        labels: { format: '{value:%H:%M}', style: { color: '#94a3b8', fontSize: '10px' } },
                                                        gridLineColor: '#334155'
                                                    },
                                                    yAxis: { type: 'category', grid: { borderColor: '#334155', columns: [{ title: { text: 'Asset', style: { color: '#cbd5e1' } }, labels: { style: { color: '#cbd5e1' } } }] }, labels: { style: { color: '#cbd5e1' } } },
                                                    series: [{ name: 'Critical Assets', data: ganttData }]
                                                }}
                                            />
                                        ) : (
                                            <div className="flex flex-col items-center justify-center h-full text-slate-500">
                                                <CheckCircle size={48} className="mb-4 opacity-70" />
                                                <p>No critical maintenance schedules.</p>
                                            </div>
                                        )}
                                    </div>
                                )
                            },
                            {
                                key: '3',
                                label: <Space><LayoutGrid size={14} /> Risk Treemap (Hierarchy)</Space>,
                                children: (
                                    <div className="bg-slate-800/50 backdrop-blur p-4 rounded-xl h-[600px]">
                                        <FleetTreemap robots={safeRobots} />
                                    </div>
                                )
                            }
                        ]}
                    />
                </Col>

                {/* 2. Heatmap */}
                <Col xs={24} lg={8}>
                    <Card title={<Space><AlertTriangle size={16} className="text-amber-400" /> Sensor Anomalies</Space>} variant="borderless" className="shadow-lg backdrop-blur bg-slate-800/50 h-[655px]">
                        <HighchartsReact
                            highcharts={Highcharts}
                            options={{
                                chart: { type: 'heatmap', backgroundColor: 'transparent', height: 550, style: { fontFamily: 'inherit' } },
                                title: { text: '' },
                                credits: { enabled: false },
                                xAxis: { categories: ['Torque', 'Temp', 'Tool'], labels: { style: { color: '#94a3b8' } }, lineColor: '#334155' },
                                yAxis: { categories: safeRobots.map(r => r.name || r.id), title: null, labels: { style: { color: '#94a3b8', fontSize: '10px' } }, gridLineColor: '#334155', reversed: true },
                                colorAxis: { min: 0, max: 100, minColor: '#10b981', maxColor: '#ef4444', stops: [[0, '#10b981'], [0.5, '#f59e0b'], [1, '#ef4444']] },
                                legend: { enabled: false },
                                tooltip: {
                                    backgroundColor: '#1e293b', borderColor: '#334155', style: { color: '#f8fafc' },
                                    formatter: function () {
                                        const point = this.point;
                                        return `<b>${this.series.yAxis.categories[point.y]}</b><br/>${this.series.xAxis.categories[point.x]}: <b>${point.customDisplay}</b><br/>(Anomaly: ${point.value})`;
                                    }
                                },
                                series: [{
                                    name: 'Sensor Anomalies', borderWidth: 1, borderColor: '#1e293b',
                                    data: safeRobots.flatMap((r, yIndex) => [
                                        { x: 0, y: yIndex, value: Math.min(100, (r.sensors?.Torque || 0)), customDisplay: `${(r.sensors?.Torque || 0).toFixed(1)} Nm` },
                                        { x: 1, y: yIndex, value: Math.min(100, (r.sensors?.Temperature || 300) - 273), customDisplay: `${(r.sensors?.Temperature || 0).toFixed(1)} K` },
                                        { x: 2, y: yIndex, value: Math.min(100, (r.sensors?.['Tool Wear'] || 0) * 0.5), customDisplay: `${(r.sensors?.['Tool Wear'] || 0).toFixed(0)} min` }
                                    ]),
                                    dataLabels: { enabled: false }
                                }]
                            }}
                        />
                    </Card>
                </Col>
            </Row>

            {/* ProTable Registry */}
            <div className="mt-4">
                <Title level={4} style={{ marginBottom: 16, color: 'white' }}>Fleet Registry</Title>
                <ProTable
                    columns={columns}
                    dataSource={safeRobots}
                    rowKey="id"
                    pagination={{
                        pageSize: 8,
                        showTotal: (total, range) => `Showing ${range[0]}-${range[1]} of ${total} items`
                    }}
                    search={{
                        labelWidth: 'auto',
                        filterType: 'light',
                    }}
                    options={{
                        density: true,
                        fullScreen: true,
                        reload: () => { }, // Mock reload
                        setting: true,
                    }}
                    headerTitle="Asset Database"
                    className="shadow-lg rounded-xl overflow-hidden border border-slate-700"
                    tableStyle={{ backgroundColor: 'rgba(30, 41, 59, 0.5)' }}
                />
            </div>
        </div>
    );
};

export default AssetMonitor;
