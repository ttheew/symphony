import { useState, useEffect, useRef, useCallback } from 'react';
import {
  Server,
  RefreshCw,
  AlertCircle,
  Clock,
  Pause,
  Play,
} from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { useToast } from '@/hooks/use-toast';
import Layout from '@/components/Layout';
import NodeCard from '@/components/nodes/NodeCard';
import { nodesApi, NodeSnapshot, subscribeSnapshotStream } from '@/lib/api';
import { getNodeStatus } from '@/lib/formatters';

const NodesPage = () => {
  const [nodes, setNodes] = useState<Record<string, NodeSnapshot>>({});
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [lastUpdate, setLastUpdate] = useState<Date | null>(null);
  const [latency, setLatency] = useState<number | null>(null);
  const [autoRefresh, setAutoRefresh] = useState(true);
  const [wsConnected, setWsConnected] = useState(false);
  const requestInFlight = useRef(false);

  const { toast } = useToast();

  const fetchNodes = useCallback(async () => {
    if (requestInFlight.current) return;
    requestInFlight.current = true;

    const startTime = Date.now();
    try {
      const data = await nodesApi.list();
      setNodes(data.nodes);
      setLastUpdate(new Date());
      setLatency(Date.now() - startTime);
      setError(null);
    } catch (err: any) {
      setError(err?.message || 'Failed to fetch nodes');
      // Keep last known data
    } finally {
      requestInFlight.current = false;
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchNodes();
  }, [fetchNodes]);

  useEffect(() => {
    if (!autoRefresh) {
      setWsConnected(false);
      return;
    }
    const unsubscribe = subscribeSnapshotStream(
      (payload) => {
        if (payload.type !== 'snapshot') return;
        setNodes(payload.nodes);
        setLastUpdate(new Date());
        setError(null);
        setLoading(false);
      },
      (connected) => {
        setWsConnected(connected);
        if (!connected) {
          setError('Live updates disconnected. You can refresh manually.');
        }
      }
    );
    return unsubscribe;
  }, [autoRefresh]);

  const nodeList = Object.values(nodes);
  const healthyCounts = nodeList.reduce(
    (acc, node) => {
      const status = getNodeStatus(node.last_heartbeat);
      acc[status]++;
      return acc;
    },
    { healthy: 0, stale: 0, offline: 0 }
  );

  const sortedNodes = [...nodeList].sort((a, b) => {
    const statusOrder = { healthy: 0, stale: 1, offline: 2 };
    const statusA = getNodeStatus(a.last_heartbeat);
    const statusB = getNodeStatus(b.last_heartbeat);
    return statusOrder[statusA] - statusOrder[statusB];
  });

  return (
    <Layout>
      <div className="space-y-6">
        {/* Header */}
        <div className="flex flex-col lg:flex-row lg:items-center lg:justify-between gap-4">
          <div>
            <h1 className="text-2xl font-bold tracking-tight flex items-center gap-3">
              <Server className="h-7 w-7 text-primary" />
              Nodes
            </h1>
            <p className="text-muted-foreground mt-1">
              Real-time monitoring of connected nodes
            </p>
          </div>

          <div className="flex flex-wrap items-center gap-3">
            {/* Status Summary */}
            <div className="flex items-center gap-2">
              <Badge variant="healthy" className="gap-1">
                {healthyCounts.healthy} Healthy
              </Badge>
              <Badge variant="stale" className="gap-1">
                {healthyCounts.stale} Stale
              </Badge>
              <Badge variant="offline" className="gap-1">
                {healthyCounts.offline} Offline
              </Badge>
            </div>

            <div className="h-6 w-px bg-border" />

            {/* Last Update */}
            {lastUpdate && (
              <div className="flex items-center gap-2 text-xs text-muted-foreground">
                <Clock className="h-3.5 w-3.5" />
                {lastUpdate.toLocaleTimeString()}
                {latency !== null && (
                  <span className="text-primary">({latency}ms)</span>
                )}
              </div>
            )}

            {/* Controls */}
            <div className="flex items-center gap-2">
              <Badge variant={wsConnected ? 'healthy' : 'secondary'}>
                {wsConnected ? 'WS Connected' : 'WS Disconnected'}
              </Badge>

              <Button
                variant={autoRefresh ? 'default' : 'outline'}
                size="sm"
                onClick={() => setAutoRefresh(!autoRefresh)}
              >
                {autoRefresh ? (
                  <>
                    <Pause className="h-4 w-4 mr-1" />
                    Pause
                  </>
                ) : (
                  <>
                    <Play className="h-4 w-4 mr-1" />
                    Resume
                  </>
                )}
              </Button>

              <Button variant="outline" size="icon" onClick={fetchNodes}>
                <RefreshCw className={`h-4 w-4 ${loading ? 'animate-spin' : ''}`} />
              </Button>
            </div>
          </div>
        </div>

        {/* Error Banner */}
        {error && (
          <div className="flex items-center gap-2 p-4 rounded-lg bg-destructive/10 border border-destructive/20 text-destructive">
            <AlertCircle className="h-5 w-5 flex-shrink-0" />
            <div className="flex-1">
              <span className="font-medium">API Error:</span> {error}
              {lastUpdate && (
                <span className="text-sm ml-2 opacity-75">
                  (Last successful: {lastUpdate.toLocaleTimeString()})
                </span>
              )}
            </div>
            <Button variant="outline" size="sm" onClick={fetchNodes}>
              Retry
            </Button>
          </div>
        )}

        {/* Loading State */}
        {loading && nodeList.length === 0 && (
          <div className="flex flex-col items-center justify-center py-16 text-muted-foreground">
            <RefreshCw className="h-8 w-8 animate-spin mb-4" />
            <p>Loading nodes...</p>
          </div>
        )}

        {/* Empty State */}
        {!loading && nodeList.length === 0 && !error && (
          <div className="flex flex-col items-center justify-center py-16 text-muted-foreground border border-dashed border-border rounded-xl">
            <Server className="h-12 w-12 mb-4 opacity-50" />
            <p className="text-lg font-medium">No nodes connected</p>
            <p className="text-sm">Nodes will appear here when they connect to the conductor</p>
          </div>
        )}

        {/* Nodes Grid */}
        {nodeList.length > 0 && (
          <div className="grid gap-4 grid-cols-1">
            {sortedNodes.map((node) => (
              <NodeCard key={node.node_id} node={node} />
            ))}
          </div>
        )}
      </div>
    </Layout>
  );
};

export default NodesPage;
