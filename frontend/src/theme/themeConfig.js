import { theme } from 'antd';

const themeConfig = {
    algorithm: theme.darkAlgorithm,
    token: {
        colorPrimary: '#9A7FD4', // Bright industrial blue/purple
        colorBgBase: '#0A192F', // Deep navy
        colorSuccess: '#52c41a', // Green
        colorWarning: '#faad14', // Yellow
        colorError: '#f5222d', // Red
        colorInfo: '#9A7FD4',
        // Customizing additional tokens to ensure specific deep navy look
        colorBgContainer: '#112240',
        colorBgLayout: '#0A192F',
    },
    components: {
        Layout: {
            colorBgBody: '#0A192F',
            colorBgHeader: '#112240',
            colorBgSider: '#112240',
        },
        Card: {
            colorBgContainer: '#112240',
        },
        Button: {
            // Ensuring buttons pop against the dark background
            primaryShadow: '0 2px 0 rgba(0, 0, 0, 0.045)',
        }
    },
};

export default themeConfig;
