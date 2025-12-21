import React from 'react';
import { Treemap } from '@ant-design/plots';

const FleetTreemap = ({ robots = [] }) => {
    // 1. Transform Data into Hierarchy
    // Root -> Status -> Asset
    // 1. Transform Data into Hierarchy
    // User Request: Group by 'criticality_rating', Size by 'current_tool_wear_pct'

    // Helper to safely resolve fields
    const getStatus = (r) => {
        // Prefer explicit rating, fallback to status, default to Healthy
        const rating = r.criticality_rating || r.status || 'healthy';
        // Normalize to Title Case for grouping
        return rating.charAt(0).toUpperCase() + rating.slice(1);
    };

    const getValue = (r) => {
        // Prefer percent, fallback to raw wear, default to 10
        return r.current_tool_wear_pct || r.sensors?.['Tool Wear'] || r.risk || 10;
    };

    // Grouping
    const groups = {
        'Critical': [],
        'Warning': [],
        'Healthy': []
    };

    robots.forEach(r => {
        const rawStatus = getStatus(r).toLowerCase();
        const groupKey = rawStatus === 'critical' ? 'Critical' :
            rawStatus === 'warning' ? 'Warning' : 'Healthy';

        groups[groupKey].push({
            name: r.name,
            value: getValue(r),
            status: rawStatus
        });
    });

    const data = {
        name: 'root',
        children: [
            { name: 'Critical', children: groups['Critical'] },
            { name: 'Warning', children: groups['Warning'] },
            { name: 'Healthy', children: groups['Healthy'] }
        ]
    };

    // 2. Remove empty categories to prevent rendering issues
    data.children = data.children.filter(c => c.children.length > 0);

    const config = {
        data,
        colorField: 'name',
        // Custom Color Mapping for the parent categories
        color: (datum) => {
            if (datum.name === 'Critical') return '#ef4444';
            if (datum.name === 'Warning') return '#f59e0b';
            if (datum.name === 'Healthy') return '#10b981';
            return '#64748b'; // Fallback
        },
        // Leaf node style
        style: {
            stroke: '#1e293b',
            lineWidth: 2,
            fillOpacity: 0.8,
        },
        // V5: Label configuration
        label: {
            style: {
                fill: '#fff',
                fontSize: 12,
                fontWeight: 'bold',
            },
            position: 'top-left',
        },
        tooltip: {
            formatter: (v) => {
                return { name: v.name, value: `${v.value.toFixed(0)} min (Wear)` };
            }
        },
        theme: 'dark',
        // Interactions
        interaction: {
            tooltip: {
                render: (e, { title, items }) => {
                    return (
                        <div style={{ padding: 10, background: '#1e293b', border: '1px solid #334155', color: '#fff' }}>
                            <div style={{ fontWeight: 'bold' }}>{title}</div>
                            {items.map((item, i) => (
                                <div key={i}>
                                    <span style={{ marginRight: 8 }}>Tool Wear:</span>
                                    <span>{item.value}</span>
                                </div>
                            ))}
                        </div>
                    );
                }
            }
        }
    };

    if (robots.length === 0) {
        return <div className="text-center text-slate-500 py-10">No Fleet Data Available</div>;
    }

    return (
        <div className="h-full w-full bg-slate-800/20 rounded-lg p-2">
            <Treemap {...config} />
        </div>
    );
};

export default FleetTreemap;
