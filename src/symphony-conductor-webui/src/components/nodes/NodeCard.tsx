import { Clock, Cpu, HardDrive, Layers, MemoryStick, Server, Tag } from 'lucide-react';
import { Badge } from '@/components/ui/badge';
import { NodeSnapshot } from '@/lib/api';
import { formatTimeAgo, formatIsoTimestamp, getNodeStatus, truncateId } from '@/lib/formatters';
import CpuChart from './CpuChart';
import MemoryChart from './MemoryChart';
import StorageChart from './StorageChart';
import GpuChart from './GpuChart';

interface NodeCardProps {
  node: NodeSnapshot;
}

const NodeCard = ({ node }: NodeCardProps) => {
  const status = getNodeStatus(node.last_heartbeat);

  const statusLabels = {
    healthy: 'Healthy',
    stale: 'Stale',
    offline: 'Offline',
  };

  const capacities = node.capacities ?? { total: {}, available: {} };
  const capacityEntries = Object.entries(capacities.total || {});
  const assignedDeployments = node.assigned_deployments ?? [];

  return (
    <div className="bg-card border border-border rounded-xl overflow-hidden hover:border-primary/30 transition-colors card-glow animate-fade-in">
      {/* Header */}
      <div className="p-4 border-b border-border bg-secondary/20">
        <div className="flex items-start justify-between gap-2">
          <div className="flex items-center gap-2 min-w-0">
            <Server className="h-5 w-5 text-primary flex-shrink-0" />
            <span className="font-mono text-sm font-medium truncate" title={node.node_id}>
              {truncateId(node.node_id, 12)}
            </span>
          </div>
          <Badge variant={status}>
            {statusLabels[status]}
          </Badge>
        </div>

        {/* Groups */}
        {node.groups.length > 0 && (
          <div className="flex flex-wrap gap-1 mt-2">
            {node.groups.map((group) => (
              <Badge key={group} variant="outline" className="text-[10px] h-5">
                <Tag className="h-2.5 w-2.5 mr-1" />
                {group}
              </Badge>
            ))}
          </div>
        )}

        {/* Heartbeat */}
        <div className="flex items-center gap-1.5 mt-2 text-xs text-muted-foreground">
          <Clock className="h-3 w-3" />
          <span>{formatTimeAgo(node.last_heartbeat)}</span>
          <span className="text-muted-foreground/50">•</span>
          <span className="text-[10px]">{formatIsoTimestamp(node.last_heartbeat)}</span>
        </div>

        {/* Capacities */}
        {capacityEntries.length > 0 && (
          <div className="flex flex-wrap gap-1 mt-2">
            {capacityEntries.map(([key, total]) => {
              const available = capacities.available[key] || 0;
              return (
                <Badge key={key} variant="secondary" className="text-[10px] h-5">
                  {key}: {available}/{total}
                </Badge>
              );
            })}
          </div>
        )}

        <div className="mt-2">
          <div className="flex items-center gap-1.5 text-xs text-muted-foreground mb-1">
            <Layers className="h-3 w-3" />
            <span>Deployments ({assignedDeployments.length})</span>
          </div>
          {assignedDeployments.length > 0 ? (
            <div className="flex flex-wrap gap-1">
              {assignedDeployments.map((deployment) => (
                <Badge key={deployment.id} variant="secondary" className="text-[10px] h-5">
                  {deployment.name} ({truncateId(deployment.id)})
                </Badge>
              ))}
            </div>
          ) : (
            <Badge variant="outline" className="text-[10px] h-5">
              None assigned
            </Badge>
          )}
        </div>
      </div>

      {/* Metrics Grid */}
      <div className="p-4 grid gap-4 md:grid-cols-2 xl:grid-cols-4 items-start">
        {/* CPU */}
        <div className="space-y-2">
          <div className="flex items-center gap-2 text-xs font-medium text-muted-foreground">
            <Cpu className="h-3.5 w-3.5" />
            CPU
            <span className="text-[10px] ml-auto">
              {node.cpu.static.logical_cores} cores
            </span>
          </div>
          <CpuChart
            totalPercent={node.cpu.dynamic.total_percent}
            perCore={node.cpu.dynamic.per_core}
          />
        </div>

        {/* Memory */}
        <div className="space-y-2">
          <div className="flex items-center gap-2 text-xs font-medium text-muted-foreground">
            <MemoryStick className="h-3.5 w-3.5" />
            Memory
          </div>
          <MemoryChart
            usedPercent={node.memory.dynamic.used_percent}
            usedBytes={node.memory.dynamic.used_bytes}
            totalBytes={node.memory.dynamic.total_bytes}
            availableBytes={node.memory.dynamic.available_bytes}
          />
        </div>

        {/* Storage */}
        <div className="space-y-2">
          <div className="flex items-center gap-2 text-xs font-medium text-muted-foreground">
            <HardDrive className="h-3.5 w-3.5" />
            Storage
          </div>
          <StorageChart mounts={node.storage_mounts} />
        </div>

        {/* GPU */}
        <div className="space-y-2">
          <div className="flex items-center gap-2 text-xs font-medium text-muted-foreground">
            <Cpu className="h-3.5 w-3.5" />
            GPU
          </div>
          <GpuChart gpus={node.gpus} />
        </div>
      </div>
    </div>
  );
};

export default NodeCard;
