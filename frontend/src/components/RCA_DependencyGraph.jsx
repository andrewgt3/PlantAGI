import React, { useEffect, useRef } from 'react';
import { Graph } from '@antv/g6';

/**
 * RCA Blast Zone Graph (G6 V5 Compatible)
 */
const RCA_DependencyGraph = ({ robots = [] }) => {
    const containerRef = useRef(null);
    const graphRef = useRef(null);

    // 1. Initialize Graph Instance (Once)
    useEffect(() => {
        if (!containerRef.current) return;

        // Clean up any existing instance
        if (graphRef.current) {
            graphRef.current.destroy();
        }

        const graph = new Graph({
            container: containerRef.current,
            autoFit: 'view',
            // V5 Layout
            layout: {
                type: 'dagre',
                rankdir: 'LR',
                nodesep: 40,
                ranksep: 80,
            },
            // V5 Default Styles
            node: {
                style: {
                    fill: '#1e3a8a',
                    stroke: '#3b82f6',
                    lineWidth: 2,
                    labelPlacement: 'bottom',
                    labelFill: '#cbd5e1',
                    labelFontSize: 10,
                    labelFontWeight: 'bold',
                },
            },
            edge: {
                style: {
                    stroke: '#475569',
                    lineWidth: 2,
                    endArrow: true,
                },
            },
            behaviors: ['drag-canvas', 'zoom-canvas', 'drag-element'],
            animation: true,
        });

        graphRef.current = graph;

        // --- INTERACTION LOGIC (V5 Data-Centric) ---
        graph.on('node:click', (evt) => {
            // In V5, evt.target.id might be shape or node id. 
            // evt.id should be the element id if behavior normalized it, 
            // but let's be safe and check the structure or generic click.

            // Actually, simplest V5 interaction is often via the specific event properties
            // console.log(evt);
            const nodeId = evt.target.id;
            if (!nodeId) return;

            // 1. Get All Data
            const allNodes = graph.getNodeData();
            const allEdges = graph.getEdgeData();

            // 2. Build Graph Map for Traversal
            const outboundMap = {};
            allEdges.forEach(e => {
                if (!outboundMap[e.source]) outboundMap[e.source] = [];
                outboundMap[e.source].push(e.target);
            });

            // 3. Find Downstream (Blast Zone)
            const downstreamNodeIds = new Set();
            const downstreamEdgeIds = new Set();

            const traverse = (currentId) => {
                const targets = outboundMap[currentId] || [];
                targets.forEach(targetId => {
                    // Find the edge connecting current -> target
                    const edge = allEdges.find(e => e.source === currentId && e.target === targetId);
                    if (edge) downstreamEdgeIds.add(edge.id);

                    if (!downstreamNodeIds.has(targetId)) {
                        downstreamNodeIds.add(targetId);
                        traverse(targetId);
                    }
                });
            };
            traverse(nodeId);

            // 4. Update Styles (Data Driven)
            const nodeUpdates = allNodes.map(n => {
                const isSelf = n.id === nodeId;
                const isDownstream = downstreamNodeIds.has(n.id);

                // Default Style Recovery (simplified: hardcode default or store in data)
                // Start with default style
                let style = { opacity: 1, stroke: '#3b82f6', lineWidth: 2, shadowBlur: 0 };

                // Apply Logic
                if (isSelf) {
                    style = { opacity: 1, stroke: '#ef4444', lineWidth: 4, shadowColor: '#ef4444', shadowBlur: 20 };
                } else if (isDownstream) {
                    style = { opacity: 1, stroke: '#ef4444', lineWidth: 3 };
                } else {
                    // Upstream / Unrelated -> Dim
                    style = { opacity: 0.2, stroke: '#3b82f6', lineWidth: 2 };
                }

                // Restore critical/warning base colors if needed, but for Blast Zone we override
                return { id: n.id, style };
            });

            const edgeUpdates = allEdges.map(e => {
                const isDownstream = downstreamEdgeIds.has(e.id);
                if (isDownstream) {
                    return { id: e.id, style: { stroke: '#ef4444', lineWidth: 3, lineDash: [10, 5] } };
                } else {
                    return { id: e.id, style: { stroke: '#475569', lineWidth: 2, opacity: 0.1, lineDash: [] } };
                }
            });

            graph.updateNodeData(nodeUpdates);
            graph.updateEdgeData(edgeUpdates);
            graph.render();
        });

        graph.on('canvas:click', () => {
            // Reset
            const allNodes = graph.getNodeData();
            const allEdges = graph.getEdgeData();

            const nodeUpdates = allNodes.map(n => ({
                id: n.id,
                style: { opacity: 1, stroke: n.data?.origStroke || '#3b82f6', lineWidth: 2, shadowBlur: 0 }
            }));

            const edgeUpdates = allEdges.map(e => ({
                id: e.id,
                style: { stroke: '#475569', lineWidth: 2, opacity: 1, lineDash: [] }
            }));

            graph.updateNodeData(nodeUpdates);
            graph.updateEdgeData(edgeUpdates);
            graph.render();
        });

        // Initial Empty Render
        graph.render();

        return () => {
            if (graphRef.current) {
                graphRef.current.destroy();
                graphRef.current = null;
            }
        };
    }, []);

    // 2. Update Data on Prop Change
    useEffect(() => {
        if (!graphRef.current) return;

        const processSteps = [
            { id: 'STAMP', name: 'Stamping Press', type: 'source' },
            { id: 'WELD1', name: 'Welding A', type: 'process' },
            { id: 'WELD2', name: 'Welding B', type: 'process' },
            { id: 'PAINT', name: 'Paint Shop', type: 'process' },
            { id: 'ASSEM', name: 'Final Assembly', type: 'sink' }
        ];

        const nodes = processSteps.map((step, i) => {
            const robot = robots[i] || {};
            const status = robot.status || 'healthy';
            const isCritical = status === 'critical';
            const isWarning = status === 'warning';

            const strokeColor = isCritical ? '#ef4444' : isWarning ? '#f59e0b' : '#3b82f6';

            return {
                id: step.id,
                data: { origStroke: strokeColor }, // Store meta data in 'data' field for V5
                style: {
                    labelText: step.name, // V5 label mapping
                    fill: isCritical ? '#450a0a' : isWarning ? '#422006' : '#1e3a8a',
                    stroke: strokeColor,
                    lineWidth: isCritical ? 3 : 2,
                    shadowColor: isCritical ? '#ef4444' : 'transparent',
                    shadowBlur: isCritical ? 10 : 0
                }
            };
        });

        const edges = [
            { id: 'e1', source: 'STAMP', target: 'WELD1' },
            { id: 'e2', source: 'WELD1', target: 'PAINT' },
            { id: 'e3', source: 'WELD2', target: 'PAINT' },
            { id: 'e4', source: 'PAINT', target: 'ASSEM' }
        ];

        graphRef.current.setData({ nodes, edges });
        graphRef.current.render();

    }, [robots]);

    return (
        <div
            ref={containerRef}
            className="w-full h-[350px] bg-slate-800/50 backdrop-blur border border-slate-700 rounded-xl shadow-lg relative overflow-hidden"
        >
            <div className="absolute top-4 left-6 z-10 pointer-events-none">
                <h3 className="text-slate-300 font-bold flex items-center gap-2 text-sm uppercase tracking-wider">
                    <span className="w-2 h-2 rounded-full bg-indigo-500"></span>
                    Plant Topology (Blast Zone Enabled)
                </h3>
            </div>
            <div className="absolute bottom-2 right-4 z-10 pointer-events-none text-[10px] text-slate-500 font-mono">
                CLICK NODE TO TRACE IMPACT
            </div>
        </div>
    );
};

export default RCA_DependencyGraph;
