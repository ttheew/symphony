import { X, Clock, Hash, Layers, Play, Square } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Sheet, SheetContent, SheetHeader, SheetTitle } from '@/components/ui/sheet';
import { DeploymentResponse } from '@/lib/api';
import { formatTimestamp, truncateId } from '@/lib/formatters';

interface DeploymentViewDrawerProps {
  open: boolean;
  onClose: () => void;
  deployment: DeploymentResponse | null;
}

const DeploymentViewDrawer = ({
  open,
  onClose,
  deployment,
}: DeploymentViewDrawerProps) => {
  if (!deployment) return null;

  const getStateVariant = (state: string) => {
    if (state === 'RUNNING') return 'success';
    if (state === 'STOPPED') return 'secondary';
    return 'warning';
  };

  return (
    <Sheet open={open} onOpenChange={(isOpen) => !isOpen && onClose()}>
      <SheetContent className="sm:max-w-lg bg-card border-border overflow-y-auto">
        <SheetHeader className="space-y-4 pb-6 border-b border-border">
          <SheetTitle className="text-xl font-semibold">{deployment.name}</SheetTitle>
          <div className="flex flex-wrap gap-2">
            <Badge variant={getStateVariant(deployment.desired_state)}>
              <Play className="h-3 w-3 mr-1" />
              Desired: {deployment.desired_state}
            </Badge>
            <Badge variant={getStateVariant(deployment.current_state)}>
              <Square className="h-3 w-3 mr-1" />
              Current: {deployment.current_state}
            </Badge>
            <Badge variant="outline">
              <Layers className="h-3 w-3 mr-1" />
              {deployment.kind}
            </Badge>
          </div>
        </SheetHeader>

        <div className="py-6 space-y-6">
          <div className="space-y-3">
            <h3 className="text-sm font-medium text-muted-foreground">Details</h3>
            <div className="space-y-2 bg-secondary/30 rounded-lg p-4">
              <div className="flex items-center justify-between">
                <span className="text-sm text-muted-foreground flex items-center gap-2">
                  <Hash className="h-4 w-4" />
                  ID
                </span>
                <span className="font-mono text-sm">{deployment.id}</span>
              </div>
              <div className="flex items-center justify-between">
                <span className="text-sm text-muted-foreground flex items-center gap-2">
                  <Clock className="h-4 w-4" />
                  Created
                </span>
                <span className="text-sm">{formatTimestamp(deployment.created_at_ms)}</span>
              </div>
              <div className="flex items-center justify-between">
                <span className="text-sm text-muted-foreground flex items-center gap-2">
                  <Clock className="h-4 w-4" />
                  Updated
                </span>
                <span className="text-sm">{formatTimestamp(deployment.updated_at_ms)}</span>
              </div>
            </div>
          </div>

          <div className="space-y-3">
            <h3 className="text-sm font-medium text-muted-foreground">Specification</h3>
            <pre className="bg-secondary/30 rounded-lg p-4 overflow-x-auto text-sm font-mono scrollbar-thin">
              {JSON.stringify(deployment.specification, null, 2)}
            </pre>
          </div>
        </div>
      </SheetContent>
    </Sheet>
  );
};

export default DeploymentViewDrawer;
