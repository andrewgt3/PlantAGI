import React from 'react';
import { useLocation, Link } from 'react-router-dom';
import { LayoutDashboard, Server, Activity, Cpu, LogOut, Database } from 'lucide-react';

// Navigation Items Configuration
const NAV_ITEMS = [
    { label: 'Asset Monitor', path: '/', icon: Server },
    { label: 'Model Performance', path: '/audit', icon: Activity },
    { label: 'Backend Status', path: '/status', icon: Database },
];

const SidebarItem = ({ icon: Icon, label, path, isActive }) => (
    <Link
        to={path}
        className={`flex items-center gap-3 px-3 py-2 rounded-lg transition-all duration-200 text-sm font-medium ${isActive
            ? 'bg-indigo-600 text-white shadow-lg shadow-indigo-500/20'
            : 'text-slate-400 hover:bg-slate-800 hover:text-slate-200'
            }`}
    >
        <Icon size={18} />
        <span>{label}</span>
    </Link>
);

const PageLayout = ({ children }) => {
    const location = useLocation();

    // Helper to determine active state (handling sub-routes like /assets/:id)
    const isPathActive = (path) => {
        if (path === '/') return location.pathname === '/';
        return location.pathname.startsWith(path);
    }

    const currentTitle = NAV_ITEMS.find(item => isPathActive(item.path))?.label || 'Dashboard';

    return (
        <div className="flex h-screen w-full overflow-hidden bg-[var(--bg-app)] font-sans text-slate-100">
            {/* Sidebar Navigation */}
            <aside className="w-64 border-r border-slate-800 bg-slate-900/50 backdrop-blur-xl flex flex-col fixed inset-y-0 left-0 z-50">
                {/* Brand Header */}
                <div className="h-16 flex items-center px-6 border-b border-slate-800 bg-slate-900/80">
                    <div className="flex items-center gap-2 font-bold tracking-wider text-white">
                        <div className="bg-indigo-500/20 p-1.5 rounded-lg">
                            <Cpu size={20} className="text-indigo-500" />
                        </div>
                        <span>PREDICT<span className="text-slate-500">LAB</span></span>
                    </div>
                </div>

                {/* Navigation Links */}
                <nav className="flex-1 p-4 space-y-1 overflow-y-auto">
                    <div className="text-xs font-bold text-slate-500 uppercase tracking-wider mb-4 px-2 mt-2">
                        Platform
                    </div>
                    {NAV_ITEMS.map((item) => (
                        <SidebarItem
                            key={item.path}
                            {...item}
                            isActive={isPathActive(item.path)}
                        />
                    ))}
                </nav>

                {/* User Footer */}
                <div className="p-4 border-t border-slate-800 bg-slate-900/30">
                    <div className="flex items-center gap-3 px-2 py-2 rounded-lg hover:bg-slate-800/50 transition-colors cursor-pointer group">
                        <div className="w-8 h-8 rounded-full bg-gradient-to-br from-indigo-500 to-purple-600 flex items-center justify-center text-xs font-bold text-white shadow-inner">
                            AG
                        </div>
                        <div className="flex flex-col flex-1 min-w-0">
                            <span className="text-xs font-bold text-white truncate group-hover:text-indigo-400 transition-colors">Admin User</span>
                            <span className="text-[10px] text-slate-500 truncate">Reliability Engineer</span>
                        </div>
                        <LogOut size={16} className="text-slate-600 group-hover:text-slate-400" />
                    </div>
                </div>
            </aside>

            {/* Main Content Area */}
            <main className="flex-1 flex flex-col ml-64 min-w-0 relative">
                {/* Top Header */}
                <header className="h-16 border-b border-slate-800/50 flex items-center justify-between px-8 bg-[var(--bg-app)]/90 backdrop-blur sticky top-0 z-40">
                    <div className="flex flex-col justify-center">
                        <div className="text-xs text-slate-500 font-medium">Workspace / {currentTitle}</div>
                    </div>

                    <div className="flex items-center gap-4">
                        <div className="flex items-center gap-2 text-xs font-mono text-emerald-400 bg-emerald-500/10 border border-emerald-500/20 px-3 py-1.5 rounded-full">
                            <span className="w-1.5 h-1.5 rounded-full bg-emerald-500 animate-pulse shadow-[0_0_8px_rgba(16,185,129,0.5)]"></span>
                            SYSTEM ONLINE
                        </div>
                    </div>
                </header>

                {/* Content Scroll Area */}
                <div className="flex-1 p-6 md:p-8 overflow-y-auto overflow-x-hidden">
                    <div className="w-full">
                        {children}
                    </div>
                </div>
            </main>
        </div>
    );
};

export default PageLayout;
