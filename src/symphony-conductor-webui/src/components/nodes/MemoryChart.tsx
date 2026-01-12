import { formatBytes, formatPercent } from '@/lib/formatters';

interface MemoryChartProps {
  usedPercent: number;
  usedBytes: number;
  totalBytes: number;
  availableBytes: number;
}

const MemoryChart = ({
  usedPercent,
  usedBytes,
  totalBytes,
  availableBytes,
}: MemoryChartProps) => {
  const getColor = (value: number) => {
    if (value >= 90) return 'hsl(var(--destructive))';
    if (value >= 70) return 'hsl(var(--warning))';
    return 'hsl(var(--chart-2))';
  };

  return (
    <div className="space-y-3">
      {/* Memory Ring */}
      <div className="flex items-center gap-4">
        <div className="relative w-16 h-16">
          <svg className="w-16 h-16 -rotate-90" viewBox="0 0 36 36">
            <circle
              cx="18"
              cy="18"
              r="15.91549430918954"
              fill="none"
              stroke="hsl(var(--muted))"
              strokeWidth="3"
            />
            <circle
              cx="18"
              cy="18"
              r="15.91549430918954"
              fill="none"
              stroke={getColor(usedPercent)}
              strokeWidth="3"
              strokeDasharray={`${usedPercent} ${100 - usedPercent}`}
              strokeLinecap="round"
              className="transition-all duration-500"
            />
          </svg>
          <div className="absolute inset-0 flex items-center justify-center">
            <span className="text-xs font-semibold">{Math.round(usedPercent)}%</span>
          </div>
        </div>
        <div>
          <p className="text-sm font-medium">Memory</p>
          <p className="text-xs text-muted-foreground">
            {formatBytes(usedBytes)} / {formatBytes(totalBytes)}
          </p>
        </div>
      </div>
    </div>
  );
};

export default MemoryChart;
