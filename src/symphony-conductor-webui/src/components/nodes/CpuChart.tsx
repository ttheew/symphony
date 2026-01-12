import { useMemo } from 'react';
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  ResponsiveContainer,
  Cell,
} from 'recharts';
import { formatPercent } from '@/lib/formatters';

interface CpuChartProps {
  totalPercent: number;
  perCore: number[];
}

const CpuChart = ({ totalPercent, perCore }: CpuChartProps) => {
  const coreData = useMemo(() => {
    return perCore.map((value, index) => ({
      name: `${index}`,
      value,
    }));
  }, [perCore]);

  const getColor = (value: number) => {
    if (value >= 90) return 'hsl(var(--destructive))';
    if (value >= 70) return 'hsl(var(--warning))';
    return 'hsl(var(--primary))';
  };

  return (
    <div className="space-y-3">
      {/* Total CPU Ring */}
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
              stroke={getColor(totalPercent)}
              strokeWidth="3"
              strokeDasharray={`${totalPercent} ${100 - totalPercent}`}
              strokeLinecap="round"
              className="transition-all duration-500"
            />
          </svg>
          <div className="absolute inset-0 flex items-center justify-center">
            <span className="text-xs font-semibold">{Math.round(totalPercent)}%</span>
          </div>
        </div>
        <div>
          <p className="text-sm font-medium">Total CPU</p>
          <p className="text-xs text-muted-foreground">{formatPercent(totalPercent)}</p>
        </div>
      </div>

      {/* Per-Core Bar Chart */}
      {perCore.length > 0 && (
        <div className="h-20">
          <ResponsiveContainer width="100%" height="100%">
            <BarChart data={coreData} margin={{ top: 0, right: 0, left: 0, bottom: 0 }}>
              <XAxis 
                dataKey="name" 
                tick={{ fontSize: 8, fill: 'hsl(var(--muted-foreground))' }}
                axisLine={false}
                tickLine={false}
              />
              <YAxis hide domain={[0, 100]} />
              <Bar dataKey="value" radius={[2, 2, 0, 0]}>
                {coreData.map((entry, index) => (
                  <Cell key={`cell-${index}`} fill={getColor(entry.value)} />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </div>
      )}
    </div>
  );
};

export default CpuChart;
