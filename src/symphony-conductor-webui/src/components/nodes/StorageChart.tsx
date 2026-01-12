import { HardDrive } from 'lucide-react';
import { StorageMount } from '@/lib/api';
import { formatBytes, formatPercent } from '@/lib/formatters';

interface StorageChartProps {
  mounts: StorageMount[];
}

const StorageChart = ({ mounts }: StorageChartProps) => {
  const getColor = (percent: number) => {
    if (percent >= 95) return 'bg-destructive';
    if (percent >= 85) return 'bg-warning';
    return 'bg-chart-4';
  };

  const getBgColor = (percent: number) => {
    if (percent >= 95) return 'bg-destructive/20';
    if (percent >= 85) return 'bg-warning/20';
    return 'bg-chart-4/20';
  };

  if (mounts.length === 0) {
    return (
      <div className="text-sm text-muted-foreground flex items-center gap-2">
        <HardDrive className="h-4 w-4" />
        No storage mounts
      </div>
    );
  }

  return (
    <div className="space-y-3">
      {mounts.slice(0, 3).map((mount, index) => (
        <div key={index} className="space-y-1">
          <div className="flex items-center justify-between text-xs">
            <span className="font-mono text-muted-foreground truncate max-w-[120px]" title={mount.mount_point}>
              {mount.mount_point}
            </span>
            <span className="font-medium">{formatPercent(mount.used_percent)}</span>
          </div>
          <div className={`h-2 rounded-full ${getBgColor(mount.used_percent)}`}>
            <div
              className={`h-full rounded-full transition-all duration-500 ${getColor(mount.used_percent)}`}
              style={{ width: `${Math.min(mount.used_percent, 100)}%` }}
            />
          </div>
          <div className="flex justify-between text-[10px] text-muted-foreground">
            <span>{formatBytes(mount.used_bytes)} used</span>
            <span>{formatBytes(mount.total_bytes)} total</span>
          </div>
        </div>
      ))}
      {mounts.length > 3 && (
        <p className="text-xs text-muted-foreground">
          +{mounts.length - 3} more mounts
        </p>
      )}
    </div>
  );
};

export default StorageChart;
