// Highcharts Module Initialization
// This file must be imported AFTER all Highcharts modules are imported
import Highcharts from 'highcharts';
import HighchartsMore from 'highcharts/highcharts-more';
import HighchartsHeatmap from 'highcharts/modules/heatmap';
import HighchartsBoost from 'highcharts/modules/boost';
import HighchartsSolidGauge from 'highcharts/modules/solid-gauge';
import HighchartsGantt from 'highcharts/modules/gantt';

// Initialize modules in correct order
// HighchartsMore MUST be first (required for solid-gauge)
if (typeof HighchartsMore === 'function') {
    HighchartsMore(Highcharts);
}

if (typeof HighchartsHeatmap === 'function') {
    HighchartsHeatmap(Highcharts);
}

if (typeof HighchartsBoost === 'function') {
    HighchartsBoost(Highcharts);
}

if (typeof HighchartsSolidGauge === 'function') {
    HighchartsSolidGauge(Highcharts);
}

if (typeof HighchartsGantt === 'function') {
    HighchartsGantt(Highcharts);
}

export default Highcharts;
