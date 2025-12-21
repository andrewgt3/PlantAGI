import React from 'react';
import { Line } from '@ant-design/plots';

const MultiMetricTrendChart = ({ data, loading }) => {
    // DEBUG: Log what we're receiving
    console.log('[MultiMetricTrendChart] Received data:', {
        dataType: Array.isArray(data) ? 'array' : typeof data,
        dataLength: data?.length,
        firstItem: data?.[0],
        loading
    });

    // Guard: Loading state
    if (loading) {
        return <div className="flex items-center justify-center h-full text-slate-500">Loading Trend Data...</div>;
    }

    // Guard: Empty or invalid data
    if (!data || !Array.isArray(data) || data.length === 0) {
        console.warn('[MultiMetricTrendChart] No data available');
        return <div className="flex items-center justify-center h-full text-slate-500">No telemetry data available</div>;
    }

    // Guard: Validate data structure
    const firstItem = data[0];
    const hasRequiredFields = firstItem &&
        'time' in firstItem &&
        ('Vibration' in firstItem || 'Torque' in firstItem || 'Temperature' in firstItem);

    if (!hasRequiredFields) {
        console.warn('[MultiMetricTrendChart] Invalid data structure', { firstItem, availableFields: Object.keys(firstItem || {}) });
        return <div className="flex items-center justify-center h-full text-slate-500">Invalid data format</div>;
    }

    // Transform data for multi-line chart (G2 v5 format: single array with series field)
    const transformedData = [];

    data.forEach(d => {
        if (d.time) {
            if (d.Vibration !== undefined) {
                transformedData.push({
                    time: d.time,
                    value: Number(d.Vibration) || 0,
                    metric: 'Vibration'
                });
            }
            if (d.Torque !== undefined) {
                transformedData.push({
                    time: d.time,
                    value: Number(d.Torque) || 0,
                    metric: 'Torque'
                });
            }
            if (d.Temperature !== undefined) {
                transformedData.push({
                    time: d.time,
                    value: Number(d.Temperature) || 0,
                    metric: 'Temperature'
                });
            }
        }
    });

    // Final guard: Ensure we have data to plot
    if (transformedData.length === 0) {
        console.warn('[MultiMetricTrendChart] No valid data points after transformation');
        return <div className="flex items-center justify-center h-full text-slate-500">No valid data points to display</div>;
    }

    // CRITICAL: Deep clone to prevent race conditions
    const immutableData = JSON.parse(JSON.stringify(transformedData));
    console.log('[MultiMetricTrendChart] Created immutable snapshot with', immutableData.length, 'points');

    // G2 v5 / @ant-design/plots v2.x Configuration
    const config = {
        data: immutableData,
        // G2 v5 Explicit Encoding
        encode: {
            x: 'time',
            y: 'value',
            color: 'metric',  // Series field for multi-line
        },
        // Scale configuration
        scale: {
            color: {
                range: ['#10b981', '#8b5cf6', '#f59e0b'],  // Vibration=green, Torque=purple, Temp=amber
            }
        },
        // Axis configuration
        axis: {
            x: {
                title: false,
                label: { autoRotate: false }
            },
            y: {
                title: 'Value'
            }
        },
        // Style
        style: {
            lineWidth: 2,
        },
        shapeField: 'smooth',
        // Legend
        legend: {
            color: {
                position: 'top',
                itemLabelFill: '#cbd5e1',
            }
        },
        // Slider for time range selection
        slider: {
            x: {
                values: [0, 1],
            }
        },
        // Tooltip
        tooltip: {
            title: 'time',
            items: [{ channel: 'y', name: (d) => d.metric }]
        },
        theme: 'dark',
    };

    return (
        <div className="h-full w-full">
            <Line {...config} />
        </div>
    );
};

export default MultiMetricTrendChart;
