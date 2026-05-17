import React, { useState, useEffect, useRef } from 'react';
import * as MyTypes from './types';
import { 
  AlertTriangle, Activity, DollarSign, ShieldAlert, 
  Zap, X, Info, Fingerprint, BarChart3 
} from 'lucide-react';

interface StatCardProps {
  label: string;
  value: string | number;
  icon: React.ReactNode;
  isAlert?: boolean;
}

const App: React.FC = () => {
  const [transactions, setTransactions] = useState<MyTypes.Transaction[]>([]);
  const [stats, setStats] = useState<MyTypes.DashboardStats>({
    totalCount: 0,
    fraudCount: 0,
    totalAmount: 0
  });
  
  const [isConnected, setIsConnected] = useState<boolean>(false);
  const [, setError] = useState<string | null>(null);
  const [selectedFraud, setSelectedFraud] = useState<MyTypes.Transaction | null>(null); // State cho Modal
  const socketRef = useRef<WebSocket | null>(null);
  const [showFullLogs, setShowFullLogs] = useState<boolean>(false); 
  const [allFraudHistory, setAllFraudHistory] = useState<MyTypes.Transaction[]>([]);
  const fraudLogs = transactions.filter(t => t.isFraud === 1);

  useEffect(() => {
    socketRef.current = new WebSocket('ws://localhost:9999');
    socketRef.current.onopen = () => { setIsConnected(true); setError(null); };
    socketRef.current.onclose = () => setIsConnected(false);
    socketRef.current.onerror = () => setError("Kết nối Socket thất bại!");
    socketRef.current.onmessage = (event: MessageEvent) => {
      try {
        const rawData = JSON.parse(event.data) as MyTypes.Transaction;
        setStats(prev => ({
          totalCount: prev.totalCount + 1,
          fraudCount: prev.fraudCount + (rawData.isFraud === 1 ? 1 : 0),
          totalAmount: prev.totalAmount + Number(rawData.amount)
        }));
        setTransactions(prev => [rawData, ...prev].slice(0, 50));
        if (rawData.isFraud === 1) {
          setAllFraudHistory(prev => [rawData, ...prev]);
        }
      } catch (err) { console.error(err); }
    };
    return () => socketRef.current?.close();
  }, []);

  return (
    <div className="min-h-screen bg-[#020617] text-slate-200 flex flex-col font-sans overflow-hidden">
      
      {/* NAVBAR */}
      <nav className="border-b border-slate-800/60 bg-[#020617]/80 backdrop-blur-md px-6 py-4 flex justify-between items-center z-40">
        <div className="flex items-center gap-3">
          <div className="bg-rose-600 p-1.5 rounded-lg">
            <ShieldAlert size={20} className="text-white" />
          </div>
          <h1 className="text-xl font-black tracking-tighter uppercase italic">SENTINEL <span className="text-rose-500">FRAUD</span></h1>
        </div>
        <div className={`flex items-center gap-2 px-3 py-1 rounded-full border text-[10px] font-bold ${isConnected ? 'border-emerald-500/50 text-emerald-400' : 'border-rose-500/50 text-rose-400'}`}>
          <div className={`w-1.5 h-1.5 rounded-full ${isConnected ? 'bg-emerald-500 animate-pulse' : 'bg-rose-500'}`} />
          {isConnected ? 'LIVE ENGINE' : 'OFFLINE'}
        </div>
      </nav>

      <main className="flex-1 grid grid-cols-1 lg:grid-cols-4 overflow-hidden">
        {/* MAIN DASHBOARD */}
        <div className="lg:col-span-3 p-6 overflow-y-auto border-r border-slate-800/50 space-y-6">
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            <StatCard label="Total Vol" value={stats.totalCount} icon={<Activity size={20} className="text-blue-400" />} />
            <div onClick={() => setShowFullLogs(true)}>
              <StatCard label="Fraud Alerts" value={stats.fraudCount} icon={<AlertTriangle size={20} className="text-rose-500" />} isAlert={stats.fraudCount > 0} />
            </div>
            <StatCard label="Analysed" value={`$${(stats.totalAmount/1000).toFixed(1)}k`} icon={<DollarSign size={20} className="text-emerald-400" />} />
          </div>

          <div className="bg-[#0f172a] rounded-2xl border border-slate-800 overflow-hidden shadow-xl">
            <table className="w-full text-left text-sm">
              <thead className="bg-slate-900/50 text-slate-500 text-[10px] uppercase font-black border-b border-slate-800">
                <tr>
                  <th className="px-6 py-4 italic">Type</th>
                  <th className="px-6 py-4">Amount</th>
                  <th className="px-6 py-4">Origin Node</th>
                  <th className="px-6 py-4 text-right">Status</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-800/50">
                {transactions.map((tx, i) => (
                  <tr key={i} className={`${tx.isFraud === 1 ? 'bg-rose-500/5' : ''}`}>
                    <td className="px-6 py-3 font-bold text-[10px] text-slate-400 uppercase">{tx.type}</td>
                    <td className={`px-6 py-3 font-mono font-bold ${tx.isFraud === 1 ? 'text-rose-500' : 'text-blue-400'}`}>${tx.amount.toLocaleString()}</td>
                    <td className="px-6 py-3 font-mono text-[10px] text-slate-500">{tx.nameOrig}</td>
                    <td className="px-6 py-3 text-right">
                      <span className={`text-[9px] font-black px-2 py-0.5 rounded ${tx.isFraud === 1 ? 'bg-rose-600 text-white' : 'text-slate-600'}`}>{tx.isFraud === 1 ? 'FRAUD' : 'VALID'}</span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>

        {/* FRAUD ALERTS */}
        <div className="lg:col-span-1 bg-slate-950/40 flex flex-col h-full border-l border-slate-800/50">
          <div className="p-4 bg-rose-500/10 border-b border-rose-500/20 flex justify-between items-center">
            <div className="flex items-center gap-2 text-rose-500 font-black text-xs uppercase italic">
              <Zap size={14} fill="currentColor" className="animate-pulse" /> Fraud Alerts
            </div>
          </div>

          <div className="flex-1 overflow-y-auto p-4 space-y-3">
            {fraudLogs.map((tx, i) => (
              <div 
                key={i} 
                onClick={() => setSelectedFraud(tx)}
                className="bg-rose-950/20 border border-rose-500/30 p-4 rounded-xl cursor-pointer hover:bg-rose-900/30 transition-all group"
              >
                <div className="flex justify-between items-start mb-1">
                  <span className="text-[9px] font-black text-rose-500 uppercase">Suspicious</span>
                  <Info size={12} className="text-rose-500 opacity-0 group-hover:opacity-100 transition-opacity" />
                </div>
                <p className="text-xl font-black text-rose-500 tracking-tighter">${tx.amount.toLocaleString()}</p>
                <p className="text-[10px] text-slate-500 font-mono mt-1">From: {tx.nameOrig}</p>
                <p className="text-[9px] text-rose-300/60 mt-2 italic font-bold">View error details →</p>
              </div>
            ))}
          </div>
        </div>
      </main>

      {showFullLogs && (
        <div className="fixed inset-0 z-50 flex items-center justify-center p-4 backdrop-blur-md bg-black/60">
          <div className="bg-[#0f172a] border border-rose-500/20 w-full max-w-2xl rounded-3xl shadow-2xl flex flex-col max-h-[70vh]">
            <div className="p-5 border-b border-rose-500/10 flex justify-between items-center">
              <div className="flex items-center gap-2 text-rose-500">
                <BarChart3 size={18} />
                <h2 className="font-black uppercase italic tracking-tighter">Fraud History Logs</h2>
              </div>
              <button onClick={() => setShowFullLogs(false)} className="text-slate-500 hover:text-white"><X size={20} /></button>
            </div>
            <div className="p-4 overflow-y-auto space-y-2">
              {allFraudHistory.length === 0 ? <p className="text-center text-slate-500 text-xs py-10 uppercase font-black">No fraud detected yet</p> : 
                allFraudHistory.map((tx, idx) => (
                  <div key={idx} onClick={() => setSelectedFraud(tx)} className="p-3 bg-white/5 border border-white/5 rounded-xl flex justify-between items-center hover:bg-rose-500/5 hover:border-rose-500/20 cursor-pointer">
                    <span className="font-mono text-xs font-bold text-rose-500">${tx.amount.toLocaleString()}</span>
                    <span className="text-[9px] text-slate-500 uppercase font-black">{tx.type} • {tx.nameOrig}</span>
                  </div>
                ))
              }
            </div>
          </div>
        </div>
      )}

      {/* MODAL ERR DETAIL FRAUD */}
      {selectedFraud && (
        <div className="fixed inset-0 z-[100] flex items-center justify-center p-4 backdrop-blur-sm bg-black/60 animate-in fade-in">
          <div className="bg-[#0f172a] border border-rose-500/30 w-full max-w-md rounded-3xl shadow-[0_0_50px_rgba(244,63,94,0.2)] overflow-hidden">
            <div className="p-6 bg-rose-500/10 border-b border-rose-500/20 flex justify-between items-center">
              <div className="flex items-center gap-3">
                <Fingerprint className="text-rose-500" size={24} />
                <h2 className="text-lg font-black text-white italic uppercase tracking-tighter">Fraud Investigation</h2>
              </div>
              <button onClick={() => setSelectedFraud(null)} className="p-1 hover:bg-rose-500/20 rounded-full transition-colors text-slate-400">
                <X size={20} />
              </button>
            </div>

            <div className="p-6 space-y-6">
              {/* Transaction Amount vs Account Balance */}
              <div className="space-y-4">
                <div className="flex justify-between items-end">
                  <span className="text-xs text-slate-500 uppercase font-bold">Transaction</span>
                  <span className="text-2xl font-black text-rose-500">${selectedFraud.amount.toLocaleString()}</span>
                </div>
                
                <div className="bg-black/40 p-4 rounded-2xl space-y-3 border border-white/5">
                  <div className="flex justify-between text-xs">
                    <span className="text-slate-400">Initial Balance:</span>
                    <span className="font-mono font-bold">${selectedFraud.oldbalanceOrg.toLocaleString()}</span>
                  </div>
                  
                  <div className="space-y-1">
                    <div className="flex justify-between text-[10px] font-black uppercase text-rose-400">
                      <span>Balance Utilization Rate</span>
                      <span>{((selectedFraud.amount / selectedFraud.oldbalanceOrg) * 100).toFixed(1)}%</span>
                    </div>
                    <div className="w-full h-2 bg-slate-800 rounded-full overflow-hidden">
                      <div 
                        className="h-full bg-rose-500 shadow-[0_0_10px_#f43f5e]" 
                        style={{ width: `${Math.min((selectedFraud.amount / selectedFraud.oldbalanceOrg) * 100), 100}%` }}
                      />
                    </div>
                  </div>

                  <div className="flex justify-between text-xs pt-2 border-t border-white/5">
                    <span className="text-slate-400">Post-Transaction Balance:</span>
                    <span className={`font-mono font-bold ${selectedFraud.newbalanceOrig === 0 ? 'text-rose-500 animate-pulse' : ''}`}>
                      ${selectedFraud.newbalanceOrig.toLocaleString()}
                    </span>
                  </div>
                </div>
              </div>

              {/* Error Explanation */}
              <div className="bg-rose-500/5 border border-rose-500/20 p-4 rounded-2xl italic">
                <p className="text-xs text-rose-300 leading-relaxed">
                  <span className="font-black uppercase not-italic block mb-1">System Conclusion:</span>
                  This transaction was flagged as fraudulent because the transferred amount accounted for <span className="text-rose-500 font-bold">{((selectedFraud.amount / selectedFraud.oldbalanceOrg) * 100).toFixed(1)}%</span> tổng tài sản hiện có. 
                  {selectedFraud.newbalanceOrig === 0 && " This is a typical account clean-out behavior commonly associated with financial crime."}
                </p>
              </div>

              <button 
                onClick={() => setSelectedFraud(null)}
                className="w-full bg-rose-600 hover:bg-rose-500 text-white font-black py-4 rounded-2xl transition-all shadow-lg shadow-rose-900/20 uppercase tracking-widest text-xs"
              >
                Confirm & Close
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

const StatCard: React.FC<StatCardProps> = ({ label, value, icon, isAlert = false }) => (
  <div className={`p-5 rounded-2xl border transition-all ${isAlert ? 'bg-rose-500/5 border-rose-500/30' : 'bg-[#0f172a] border-slate-800 shadow-xl'}`}>
    <div className="mb-3">{icon}</div>
    <p className="text-slate-500 text-[10px] font-black uppercase tracking-widest leading-none">{label}</p>
    <p className={`text-2xl font-black mt-2 tracking-tight ${isAlert ? 'text-rose-500' : 'text-white'}`}>
      {typeof value === 'number' ? value.toLocaleString() : value}
    </p>
  </div>
);

export default App;