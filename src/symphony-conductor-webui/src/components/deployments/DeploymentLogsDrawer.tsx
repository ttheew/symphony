import { useEffect, useMemo, useRef, useState } from 'react';
import { FileText, RefreshCw, Terminal } from 'lucide-react';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Sheet, SheetContent, SheetHeader, SheetTitle } from '@/components/ui/sheet';
import { WS_BASE_URL } from '@/lib/api';
import { formatTimestamp } from '@/lib/formatters';

type StreamFilter = 'ALL' | 'PROCESS' | 'CONTROL';

interface LogEntry {
  timestamp_unix_ms: number;
  stream: string;
  line: string;
}

interface DeploymentLogsDrawerProps {
  open: boolean;
  onClose: () => void;
  deploymentId: string | null;
  deploymentName?: string | null;
}

const DeploymentLogsDrawer = ({
  open,
  onClose,
  deploymentId,
  deploymentName,
}: DeploymentLogsDrawerProps) => {
  const [connected, setConnected] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [streamFilter, setStreamFilter] = useState<StreamFilter>('ALL');
  const [entries, setEntries] = useState<LogEntry[]>([]);
  const scrollRef = useRef<HTMLDivElement | null>(null);

  const title = useMemo(() => deploymentName || deploymentId || 'Logs', [deploymentId, deploymentName]);
  const filteredEntries = useMemo(() => {
    if (streamFilter === 'ALL') return entries;
    if (streamFilter === 'PROCESS') {
      return entries.filter((entry) => entry.stream === 'stdout' || entry.stream === 'stderr');
    }
    return entries.filter((entry) => entry.stream === 'system' || entry.stream === 'system-hc');
  }, [entries, streamFilter]);

  useEffect(() => {
    if (!open || !deploymentId) return;

    setEntries([]);
    setError(null);

    const query = new URLSearchParams({ tail: '200' });
    const ws = new WebSocket(`${WS_BASE_URL}/ws/deployments/${deploymentId}/logs?${query.toString()}`);

    ws.onopen = () => setConnected(true);
    ws.onerror = () => {
      setConnected(false);
      setError('Failed to connect log stream');
    };
    ws.onclose = () => setConnected(false);
    ws.onmessage = (event) => {
      try {
        const payload = JSON.parse(event.data);
        if (payload.error) {
          setError(payload.error);
          return;
        }
        const next = (payload.entries ?? []) as LogEntry[];
        if (next.length === 0) return;
        setEntries((prev) => {
          const merged = [...prev, ...next];
          return merged.length > 3000 ? merged.slice(merged.length - 3000) : merged;
        });
      } catch {
        // Ignore malformed frames.
      }
    };

    return () => ws.close();
  }, [open, deploymentId]);

  useEffect(() => {
    if (!scrollRef.current) return;
    scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
  }, [filteredEntries]);

  return (
    <Sheet open={open} onOpenChange={(isOpen) => !isOpen && onClose()}>
      <SheetContent className="sm:max-w-3xl bg-card border-border overflow-hidden flex flex-col">
        <SheetHeader className="space-y-3 pb-4 border-b border-border">
          <SheetTitle className="text-xl font-semibold flex items-center gap-2">
            <Terminal className="h-5 w-5" />
            {title}
          </SheetTitle>
          <div className="flex items-center gap-2">
            <Badge variant={connected ? 'healthy' : 'secondary'}>
              {connected ? 'Streaming' : 'Disconnected'}
            </Badge>
            <Button
              size="sm"
              variant={streamFilter === 'ALL' ? 'default' : 'outline'}
              onClick={() => setStreamFilter('ALL')}
            >
              All
            </Button>
            <Button
              size="sm"
              variant={streamFilter === 'PROCESS' ? 'default' : 'outline'}
              onClick={() => setStreamFilter('PROCESS')}
            >
              Exec
            </Button>
            <Button
              size="sm"
              variant={streamFilter === 'CONTROL' ? 'default' : 'outline'}
              onClick={() => setStreamFilter('CONTROL')}
            >
              Control
            </Button>
            <Button size="icon" variant="outline" onClick={() => setEntries([])}>
              <RefreshCw className="h-4 w-4" />
            </Button>
          </div>
        </SheetHeader>

        <div ref={scrollRef} className="flex-1 overflow-auto mt-4 rounded-lg border border-border bg-secondary/20 p-3">
          {error && (
            <div className="mb-2 text-sm text-destructive">{error}</div>
          )}
          {filteredEntries.length === 0 ? (
            <div className="text-sm text-muted-foreground flex items-center gap-2">
              <FileText className="h-4 w-4" />
              No logs yet...
            </div>
          ) : (
            <div className="space-y-1">
              {filteredEntries.map((entry, idx) => (
                <div key={`${entry.timestamp_unix_ms}-${idx}`} className="font-mono text-xs leading-5">
                  <span className="text-muted-foreground mr-2">{formatTimestamp(entry.timestamp_unix_ms)}</span>
                  <span className="text-primary mr-2">[{entry.stream}]</span>
                  <span>{entry.line}</span>
                </div>
              ))}
            </div>
          )}
        </div>
      </SheetContent>
    </Sheet>
  );
};

export default DeploymentLogsDrawer;
