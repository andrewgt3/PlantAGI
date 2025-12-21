import React from 'react';
import { Line } from '@ant-design/plots';

const RulDecayChart = ({ data, loading }) => {
    // DEBUG: Log what we're receiving
    console.log('[RulDecayChart] Received data:', {
        dataType: Array.isArray(data) ? 'array' : typeof data,
        dataLength: data?.length,
        firstItem: data?.[0],
        loading
    });

    // Guard: Loading state
    if (loading) {
        return <div className="flex items-center justify-center h-full text-slate-500">Calculating RUL Projection...</div>;
    }

    // Guard: Check data exists and is not empty
    if (!data || !Array.isArray(data) || data.length === 0) {
        console.warn('[RulDecayChart] No data available');
        return <div className="flex items-center justify-center h-full text-slate-500">No RUL data available</div>;
    }

    // Guard: Validate required fields exist
    const requiredXField = 'time';
    const requiredYField = 'rul_prediction';
    const firstItem = data[0];

    console.log('[RulDecayChart] Field validation:', {
        expectedX: requiredXField,
        expectedY: requiredYField,
        actualFields: Object.keys(firstItem),
        hasX: requiredXField in firstItem,
        hasY: requiredYField in firstItem,
        xValue: firstItem[requiredXField],
        yValue: firstItem[requiredYField]
    });

    if (!(requiredXField in firstItem)) {
        console.error(`[RulDecayChart] Missing X field '${requiredXField}'. Available fields:`, Object.keys(firstItem));
        return <div className="flex items-center justify-center h-full text-slate-500">
            Data error: Missing '{requiredXField}' field
        </div>;
    }

    if (!(requiredYField in firstItem)) {
        console.error(`[RulDecayChart] Missing Y field '${requiredYField}'. Available fields:`, Object.keys(firstItem));
        return <div className="flex items-center justify-center h-full text-slate-500">
            Data error: Missing '{requiredYField}' field
        </div>;
    }

    console.log('[RulDecayChart] Validation passed, rendering chart with', data.length, 'data points');

    // CRITICAL: Deep clone data to prevent race condition
    const immutableData = JSON.parse(JSON.stringify(data));
    console.log('[RulDecayChart] Created immutable data snapshot with', immutableData.length, 'points');

    // CRITICAL: Ensure Y-axis values are strict Numbers (G2Plot fails on strings)
    const cleanedData = immutableData.map(item => ({
        ...item,
        rul_prediction: Number(item.rul_prediction) || 0,
    }));
    console.log('[RulDecayChart] Converted Y-axis to numeric type');

    // G2 v5 / Ant Design Plots v2 Configuration - SIMPLIFIED (no annotations to avoid encoding issues)
    const config = {
        data: cleanedData,
        encode: {
            x: requiredXField,
            y: requiredYField
        },
        scale: {
            y: { domainMin: 0 }
        },
        axis: {
            x: { title: false },
            y: {
                title: 'RUL (Days)',
            }
        },
        style: {
            stroke: '#10b981',
            lineWidth: 2,
        },
        shapeField: 'smooth',

        // Tooltip
        tooltip: {
            title: 'time',
            items: [{ channel: 'y', name: 'RUL (Days)' }]
        },

        theme: 'dark',
    };

    return (
        <div className="h-full w-full">
            <Line {...config} />
        </div>
    );
};

export default RulDecayChart;
