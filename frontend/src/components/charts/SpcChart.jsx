import React from 'react';
import { Line } from '@ant-design/plots';

const SpcChart = ({ data, loading }) => {
    // DEBUG: Log what we're receiving
    console.log('[SpcChart] Received data:', {
        dataType: Array.isArray(data) ? 'array' : typeof data,
        dataLength: data?.length,
        firstItem: data?.[0],
        loading
    });

    // Guard: Loading state
    if (loading) {
        return <div className="flex items-center justify-center h-full text-slate-500">Loading SPC Data...</div>;
    }

    // Guard: Check data exists and is not empty
    if (!data || !Array.isArray(data) || data.length === 0) {
        console.warn('[SpcChart] No data available');
        return <div className="flex items-center justify-center h-full text-slate-500">No SPC data available</div>;
    }

    // Guard: Validate required fields exist
    const requiredXField = 'time';
    const requiredYField = 'current_tool_wear_pct';
    const firstItem = data[0];

    console.log('[SpcChart] Field validation:', {
        expectedX: requiredXField,
        expectedY: requiredYField,
        actualFields: Object.keys(firstItem),
        hasX: requiredXField in firstItem,
        hasY: requiredYField in firstItem,
        xValue: firstItem[requiredXField],
        yValue: firstItem[requiredYField]
    });

    if (!(requiredXField in firstItem)) {
        console.error(`[SpcChart] Missing X field '${requiredXField}'. Available fields:`, Object.keys(firstItem));
        return <div className="flex items-center justify-center h-full text-slate-500">
            Data error: Missing '{requiredXField}' field
        </div>;
    }

    if (!(requiredYField in firstItem)) {
        console.error(`[SpcChart] Missing Y field '${requiredYField}'. Available fields:`, Object.keys(firstItem));
        return <div className="flex items-center justify-center h-full text-slate-500">
            Data error: Missing '{requiredYField}' field
        </div>;
    }

    console.log('[SpcChart] Validation passed, rendering chart with', data.length, 'data points');

    // CRITICAL: Deep clone data to prevent race condition
    const immutableData = JSON.parse(JSON.stringify(data));
    console.log('[SpcChart] Created immutable data snapshot with', immutableData.length, 'points');

    // CRITICAL: Ensure Y-axis values are strict Numbers (G2Plot fails on strings)
    const cleanedData = immutableData.map(item => ({
        ...item,
        current_tool_wear_pct: Number(item.current_tool_wear_pct) || 0,
    }));
    console.log('[SpcChart] Converted Y-axis to numeric type');

    // G2 v5 / Ant Design Plots v2 Configuration - SIMPLIFIED (no annotations to avoid encoding issues)
    const config = {
        data: cleanedData,
        // G2 v5 Explicit Encoding
        encode: {
            x: requiredXField,
            y: requiredYField,
        },
        // Scale / Axis
        scale: {
            y: { domain: [0, 100] }
        },
        axis: {
            x: { title: false, label: { autoRotate: false } },
            y: { title: 'Tool Wear (%)' }
        },
        // Style
        style: {
            lineWidth: 3,
            stroke: '#8b5cf6',
        },
        shapeField: 'smooth',

        // Tooltip
        tooltip: {
            title: 'time',
            items: [{ channel: 'y', name: 'Tool Wear' }]
        },

        theme: 'dark',
    };

    return (
        <div className="h-full w-full">
            <Line {...config} />
        </div>
    );
};

export default SpcChart;
