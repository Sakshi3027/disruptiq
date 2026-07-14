"use client";
import { useState, useEffect } from "react";

const API_URL = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8084";

interface CompanyRisk {
  company: string;
  sector: string;
  country: string;
  risk_score: number;
  affected_ports: string[];
  impacted_products: string[];
  alert_level: string;
}

interface GraphStats {
  companies: number;
  ports: number;
  products: number;
  relationships: number;
}

interface Port {
  code: string;
  name: string;
  country: string;
  risk_score: number;
  volume_teu: number;
}

export default function Home() {
  const [risks, setRisks] = useState<CompanyRisk[]>([]);
  const [stats, setStats] = useState<GraphStats | null>(null);
  const [ports, setPorts] = useState<Port[]>([]);
  const [totalEvents, setTotalEvents] = useState(0);
  const [loading, setLoading] = useState(true);
  const [lastUpdated, setLastUpdated] = useState("");
  const [selectedPort, setSelectedPort] = useState<string | null>(null);
  const [portCompanies, setPortCompanies] = useState<any[]>([]);

  const fetchData = async () => {
    try {
      const [risksRes, statsRes, portsRes] = await Promise.all([
        fetch(`${API_URL}/risks`),
        fetch(`${API_URL}/graph/stats`),
        fetch(`${API_URL}/graph/ports`)
      ]);
      const risksData = await risksRes.json();
      const statsData = await statsRes.json();
      const portsData = await portsRes.json();
      setRisks(risksData.company_risks || []);
      setTotalEvents(risksData.total_events_processed || 0);
      setStats(statsData);
      setPorts(portsData.ports || []);
      setLastUpdated(new Date().toLocaleTimeString());
    } catch {
      console.error("API error");
    } finally {
      setLoading(false);
    }
  };

  const fetchPortImpact = async (portCode: string) => {
    try {
      const res = await fetch(`${API_URL}/graph/affected/${portCode}`);
      const data = await res.json();
      setPortCompanies(data.affected_companies || []);
      setSelectedPort(portCode);
    } catch {
      console.error("Port fetch error");
    }
  };

  useEffect(() => {
    fetchData();
    const interval = setInterval(fetchData, 60000);
    return () => clearInterval(interval);
  }, []);

  const getRiskColor = (score: number) => {
    if (score >= 0.7) return "text-red-400 bg-red-400/10 border-red-400/30";
    if (score >= 0.5) return "text-orange-400 bg-orange-400/10 border-orange-400/30";
    if (score >= 0.3) return "text-yellow-400 bg-yellow-400/10 border-yellow-400/30";
    return "text-green-400 bg-green-400/10 border-green-400/30";
  };

  const getRiskLabel = (score: number) => {
    if (score >= 0.7) return "🔴 CRITICAL";
    if (score >= 0.5) return "🟠 HIGH";
    if (score >= 0.3) return "🟡 MEDIUM";
    return "🟢 LOW";
  };

  const getPortRiskColor = (score: number) => {
    if (score >= 0.5) return "bg-red-500";
    if (score >= 0.3) return "bg-orange-500";
    return "bg-green-500";
  };

  if (loading) {
    return (
      <div className="min-h-screen bg-[#0d1117] flex items-center justify-center">
        <div className="text-center">
          <div className="text-4xl mb-4">🌐</div>
          <div className="text-white text-xl">Loading DisruptIQ...</div>
          <div className="text-gray-400 text-sm mt-2">Connecting to knowledge graph</div>
        </div>
      </div>
    );
  }

  const criticalCount = risks.filter(r => r.risk_score >= 0.7).length;
  const highCount = risks.filter(r => r.risk_score >= 0.5 && r.risk_score < 0.7).length;
  const mediumCount = risks.filter(r => r.risk_score >= 0.3 && r.risk_score < 0.5).length;

  return (
    <div className="min-h-screen bg-[#0d1117] text-white">
      {/* Header */}
      <header className="border-b border-gray-800 px-6 py-4">
        <div className="max-w-7xl mx-auto flex items-center justify-between">
          <div>
            <h1 className="text-2xl font-bold">🌐 DisruptIQ</h1>
            <p className="text-gray-400 text-sm">Real-time supply chain risk intelligence</p>
          </div>
          <div className="text-right">
            <div className="text-green-400 font-semibold text-sm">● LIVE</div>
            <div className="text-gray-400 text-xs">Updated: {lastUpdated}</div>
          </div>
        </div>
      </header>

      <main className="max-w-7xl mx-auto px-6 py-6">
        {/* Pipeline Banner */}
        <div className="bg-[#161b22] border border-blue-500/20 rounded-lg p-4 mb-6">
          <div className="flex items-center gap-2 mb-2">
            <span className="text-blue-400 font-semibold text-sm">⚡ Intelligence Pipeline</span>
          </div>
          <div className="flex flex-wrap gap-2 text-xs text-gray-400">
            <span className="bg-gray-800 px-2 py-1 rounded">NewsAPI (Real News)</span>
            <span className="text-gray-600">→</span>
            <span className="bg-gray-800 px-2 py-1 rounded">Apache Kafka</span>
            <span className="text-gray-600">→</span>
            <span className="bg-gray-800 px-2 py-1 rounded">Risk Engine</span>
            <span className="text-gray-600">→</span>
            <span className="bg-gray-800 px-2 py-1 rounded">Neo4j Knowledge Graph</span>
            <span className="text-gray-600">→</span>
            <span className="bg-gray-800 px-2 py-1 rounded">Risk Propagation</span>
            <span className="text-gray-600">→</span>
            <span className="bg-gray-800 px-2 py-1 rounded">Live Dashboard</span>
          </div>
        </div>

        {/* Stats Row */}
        <div className="grid grid-cols-2 md:grid-cols-6 gap-3 mb-6">
          <div className="bg-[#161b22] border border-gray-800 rounded-lg p-3 text-center">
            <div className="text-xs text-gray-400 mb-1">Events Processed</div>
            <div className="text-xl font-bold text-blue-400">{totalEvents}</div>
          </div>
          <div className="bg-[#161b22] border border-red-500/20 rounded-lg p-3 text-center">
            <div className="text-xs text-gray-400 mb-1">Critical</div>
            <div className="text-xl font-bold text-red-400">{criticalCount}</div>
          </div>
          <div className="bg-[#161b22] border border-orange-500/20 rounded-lg p-3 text-center">
            <div className="text-xs text-gray-400 mb-1">High Risk</div>
            <div className="text-xl font-bold text-orange-400">{highCount}</div>
          </div>
          <div className="bg-[#161b22] border border-yellow-500/20 rounded-lg p-3 text-center">
            <div className="text-xs text-gray-400 mb-1">Medium Risk</div>
            <div className="text-xl font-bold text-yellow-400">{mediumCount}</div>
          </div>
          {stats && (
            <>
              <div className="bg-[#161b22] border border-purple-500/20 rounded-lg p-3 text-center">
                <div className="text-xs text-gray-400 mb-1">Graph Nodes</div>
                <div className="text-xl font-bold text-purple-400">
                  {stats.companies + stats.ports + stats.products}
                </div>
              </div>
              <div className="bg-[#161b22] border border-green-500/20 rounded-lg p-3 text-center">
                <div className="text-xs text-gray-400 mb-1">Relationships</div>
                <div className="text-xl font-bold text-green-400">{stats.relationships}</div>
              </div>
            </>
          )}
        </div>

        <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
          {/* Company Risk Table */}
          <div className="md:col-span-2 bg-[#161b22] border border-gray-800 rounded-lg overflow-hidden">
            <div className="px-4 py-3 border-b border-gray-800">
              <h2 className="font-semibold text-gray-200">🏭 Company Risk Scores</h2>
              <p className="text-xs text-gray-500 mt-1">Propagated from disruption events via Neo4j knowledge graph</p>
            </div>
            <div className="divide-y divide-gray-800">
              {risks.map((risk) => (
                <div key={risk.company} className="px-4 py-3 hover:bg-gray-800/30 transition">
                  <div className="flex items-center justify-between mb-1">
                    <div className="flex items-center gap-2">
                      <span className="font-semibold text-white">{risk.company}</span>
                      <span className="text-xs text-gray-500">{risk.sector}</span>
                    </div>
                    <span className={`text-xs px-2 py-0.5 rounded border font-medium ${getRiskColor(risk.risk_score)}`}>
                      {getRiskLabel(risk.risk_score)} {(risk.risk_score * 100).toFixed(0)}%
                    </span>
                  </div>
                  <div className="w-full h-1.5 bg-gray-700 rounded-full overflow-hidden mb-2">
                    <div
                      className={`h-full rounded-full ${risk.risk_score >= 0.7 ? 'bg-red-400' : risk.risk_score >= 0.5 ? 'bg-orange-400' : risk.risk_score >= 0.3 ? 'bg-yellow-400' : 'bg-green-400'}`}
                      style={{ width: `${risk.risk_score * 100}%` }}
                    />
                  </div>
                  <div className="flex flex-wrap gap-2 text-xs text-gray-400">
                    {risk.affected_ports.length > 0 && (
                      <span>🚢 {risk.affected_ports.join(", ")}</span>
                    )}
                    {risk.impacted_products.length > 0 && (
                      <span>📦 {risk.impacted_products.slice(0, 2).join(", ")}</span>
                    )}
                    <span>🌍 {risk.country}</span>
                  </div>
                </div>
              ))}
              {risks.length === 0 && (
                <div className="px-4 py-8 text-center text-gray-500">
                  No risks detected yet. Run the producer and risk engine.
                </div>
              )}
            </div>
          </div>

          {/* Port Risk Panel */}
          <div className="bg-[#161b22] border border-gray-800 rounded-lg overflow-hidden">
            <div className="px-4 py-3 border-b border-gray-800">
              <h2 className="font-semibold text-gray-200">🚢 Port Risk Monitor</h2>
              <p className="text-xs text-gray-500 mt-1">Click a port to see affected companies</p>
            </div>
            <div className="divide-y divide-gray-800">
              {ports.map((port) => (
                <div
                  key={port.code}
                  onClick={() => fetchPortImpact(port.code)}
                  className={`px-4 py-3 cursor-pointer hover:bg-gray-800/30 transition ${selectedPort === port.code ? 'bg-gray-800/50' : ''}`}
                >
                  <div className="flex items-center justify-between">
                    <div>
                      <div className="text-sm font-medium text-white">{port.name}</div>
                      <div className="text-xs text-gray-500">{port.country} · {port.code}</div>
                    </div>
                    <div className={`w-2 h-2 rounded-full ${getPortRiskColor(port.risk_score)}`} />
                  </div>
                </div>
              ))}
            </div>

            {/* Port Impact Panel */}
            {selectedPort && portCompanies.length > 0 && (
              <div className="border-t border-gray-700 px-4 py-3 bg-gray-800/30">
                <div className="text-xs font-semibold text-gray-300 mb-2">
                  Companies affected if {selectedPort} disrupted:
                </div>
                {portCompanies.map((c) => (
                  <div key={c.company} className="flex justify-between text-xs py-1">
                    <span className="text-gray-300">{c.company}</span>
                    <span className="text-orange-400">{c.sector}</span>
                  </div>
                ))}
              </div>
            )}
          </div>
        </div>

        <div className="mt-6 text-center text-xs text-gray-600">
          <p>DisruptIQ — Powered by Neo4j Knowledge Graph · Apache Kafka · NewsAPI</p>
          <p className="mt-1">Real supply chain intelligence. Not financial advice.</p>
        </div>
      </main>
    </div>
  );
}
