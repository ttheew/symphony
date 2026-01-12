import { Cpu } from 'lucide-react';
import { GpuInfo } from '@/lib/api';
import { formatBytes, formatPercent } from '@/lib/formatters';

interface GpuChartProps {
  gpus: GpuInfo[] | null;
}

const GpuChart = ({ gpus }: GpuChartProps) => {
  if (!gpus || gpus.length === 0) {
    return (
      <div className="text-sm text-muted-foreground flex items-center gap-2">
        <Cpu className="h-4 w-4" />
        No GPU
      </div>
    );
  }

  const getColor = (percent: number) => {
    if (percent >= 90) return 'bg-destructive';
    if (percent >= 70) return 'bg-warning';
    return 'bg-chart-4';
  };

  return (
    <div className="space-y-4">
      {gpus.map((gpu) => (
        <div key={gpu.index} className="space-y-2">
          <div className="flex items-center justify-between">
            <span className="text-xs font-medium truncate max-w-[150px]" title={gpu.name}>
              GPU {gpu.index}: {gpu.name}
            </span>
          </div>

          {/* Utilization */}
          <div className="space-y-1">
            <div className="flex justify-between text-[10px] text-muted-foreground">
              <span>Compute</span>
              <span>{formatPercent(gpu.util_percent)}</span>
            </div>
            <div className="h-1.5 rounded-full bg-muted">
              <div
                className={`h-full rounded-full transition-all duration-500 ${getColor(gpu.util_percent)}`}
                style={{ width: `${gpu.util_percent}%` }}
              />
            </div>
          </div>

          {/* Memory Utilization */}
          <div className="space-y-1">
            <div className="flex justify-between text-[10px] text-muted-foreground">
              <span>Memory</span>
              <span>{formatPercent(gpu.mem_util_percent)}</span>
            </div>
            <div className="h-1.5 rounded-full bg-muted">
              <div
                className={`h-full rounded-full transition-all duration-500 ${getColor(gpu.mem_util_percent)}`}
                style={{ width: `${gpu.mem_util_percent}%` }}
              />
            </div>
          </div>

          {/* Stats */}
          <div className="grid grid-cols-3 gap-1 text-[10px]">
            <div className="bg-secondary/30 rounded px-1.5 py-1 text-center">
              <span className="text-muted-foreground">Temp</span>
              <p className="font-medium">{gpu.temperature}°C</p>
            </div>
            <div className="bg-secondary/30 rounded px-1.5 py-1 text-center">
              <span className="text-muted-foreground">Power</span>
              <p className="font-medium">{gpu.power_watts}W</p>
            </div>
            <div className="bg-secondary/30 rounded px-1.5 py-1 text-center">
              <span className="text-muted-foreground">VRAM</span>
              <p className="font-medium">
                {formatBytes(gpu.memory_used_bytes)} / {formatBytes(gpu.memory_total_bytes)}
              </p>
            </div>
          </div>
        </div>
      ))}
    </div>
  );
};

export default GpuChart;
