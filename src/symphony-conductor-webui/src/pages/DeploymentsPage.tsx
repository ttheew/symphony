import { useState, useEffect, useMemo, useCallback } from 'react';
import {
  Plus,
  Search,
  RefreshCw,
  MoreHorizontal,
  Eye,
  Pencil,
  Trash2,
  Layers,
  AlertCircle,
  Terminal,
} from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Badge } from '@/components/ui/badge';
import { Switch } from '@/components/ui/switch';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table';
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu';
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from '@/components/ui/alert-dialog';
import { useToast } from '@/hooks/use-toast';
import Layout from '@/components/Layout';
import DeploymentModal from '@/components/deployments/DeploymentModal';
import DeploymentViewDrawer from '@/components/deployments/DeploymentViewDrawer';
import DeploymentLogsDrawer from '@/components/deployments/DeploymentLogsDrawer';
import {
  deploymentsApi,
  DeploymentResponse,
  DeploymentCreate,
  DeploymentUpdate,
  subscribeSnapshotStream,
} from '@/lib/api';
import { formatTimestamp, truncateId } from '@/lib/formatters';

const DeploymentsPage = () => {
  const [deployments, setDeployments] = useState<DeploymentResponse[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [search, setSearch] = useState('');
  const [stateFilter, setStateFilter] = useState<string>('ALL');
  const [kindFilter, setKindFilter] = useState<string>('ALL');
  const [limit, setLimit] = useState(100);
  const [offset, setOffset] = useState(0);
  const [updatingStateById, setUpdatingStateById] = useState<Record<string, boolean>>({});

  const [modalOpen, setModalOpen] = useState(false);
  const [modalMode, setModalMode] = useState<'create' | 'edit'>('create');
  const [selectedDeployment, setSelectedDeployment] = useState<DeploymentResponse | null>(null);
  const [viewDrawerOpen, setViewDrawerOpen] = useState(false);
  const [logsDrawerOpen, setLogsDrawerOpen] = useState(false);
  const [deleteDialogOpen, setDeleteDialogOpen] = useState(false);
  const [deploymentToDelete, setDeploymentToDelete] = useState<DeploymentResponse | null>(null);

  const { toast } = useToast();

  const fetchDeployments = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const data = await deploymentsApi.list(limit, offset);
      setDeployments(data);
    } catch (err: any) {
      setError(err?.message || 'Failed to fetch deployments');
      toast({
        variant: 'destructive',
        title: 'Error',
        description: 'Failed to fetch deployments',
      });
    } finally {
      setLoading(false);
    }
  }, [limit, offset, toast]);

  useEffect(() => {
    fetchDeployments();
  }, [fetchDeployments]);

  useEffect(() => {
    const unsubscribe = subscribeSnapshotStream(
      (payload) => {
        if (payload.type !== 'snapshot') return;
        setDeployments(payload.deployments);
        setLoading(false);
        setError(null);
      },
      (connected) => {
        if (!connected) {
          setError('Live updates disconnected. You can still refresh manually.');
        }
      }
    );
    return unsubscribe;
  }, []);

  const filteredDeployments = useMemo(() => {
    return deployments.filter((d) => {
      const matchesSearch =
        d.name.toLowerCase().includes(search.toLowerCase()) ||
        d.id.toLowerCase().includes(search.toLowerCase());
      const matchesState = stateFilter === 'ALL' || d.desired_state === stateFilter;
      const matchesKind = kindFilter === 'ALL' || d.kind === kindFilter;
      return matchesSearch && matchesState && matchesKind;
    });
  }, [deployments, search, stateFilter, kindFilter]);

  const handleCreate = () => {
    setSelectedDeployment(null);
    setModalMode('create');
    setModalOpen(true);
  };

  const handleEdit = (deployment: DeploymentResponse) => {
    setSelectedDeployment(deployment);
    setModalMode('edit');
    setModalOpen(true);
  };

  const handleView = (deployment: DeploymentResponse) => {
    setSelectedDeployment(deployment);
    setViewDrawerOpen(true);
  };

  const handleDeleteClick = (deployment: DeploymentResponse) => {
    setDeploymentToDelete(deployment);
    setDeleteDialogOpen(true);
  };

  const handleLogs = (deployment: DeploymentResponse) => {
    setSelectedDeployment(deployment);
    setLogsDrawerOpen(true);
  };

  const handleDeleteConfirm = async () => {
    if (!deploymentToDelete) return;
    try {
      await deploymentsApi.delete(deploymentToDelete.id);
      toast({
        title: 'Deleted',
        description: `Deployment "${deploymentToDelete.name}" has been deleted`,
      });
      fetchDeployments();
    } catch (err: any) {
      toast({
        variant: 'destructive',
        title: 'Error',
        description: err?.detail || 'Failed to delete deployment',
      });
    } finally {
      setDeleteDialogOpen(false);
      setDeploymentToDelete(null);
    }
  };

  const handleToggleState = async (deployment: DeploymentResponse) => {
    const nextState = deployment.desired_state === 'RUNNING' ? 'STOPPED' : 'RUNNING';
    setUpdatingStateById((prev) => ({ ...prev, [deployment.id]: true }));
    try {
      setDeployments((prev) =>
        prev.map((item) =>
          item.id === deployment.id ? { ...item, desired_state: nextState } : item
        )
      );
      await deploymentsApi.update(deployment.id, { desired_state: nextState });
      toast({
        title: nextState === 'RUNNING' ? 'Started' : 'Stopped',
        description: `"${deployment.name}" set to ${nextState}`,
      });
    } catch (err: any) {
      setDeployments((prev) =>
        prev.map((item) =>
          item.id === deployment.id ? { ...item, desired_state: deployment.desired_state } : item
        )
      );
      toast({
        variant: 'destructive',
        title: 'Error',
        description: err?.detail || 'Failed to update deployment state',
      });
    } finally {
      setUpdatingStateById((prev) => ({ ...prev, [deployment.id]: false }));
    }
  };

  const handleModalSubmit = async (data: DeploymentCreate | DeploymentUpdate) => {
    if (modalMode === 'create') {
      await deploymentsApi.create(data as DeploymentCreate);
      toast({
        title: 'Created',
        description: 'Deployment created successfully',
      });
    } else if (selectedDeployment) {
      await deploymentsApi.update(selectedDeployment.id, data as DeploymentUpdate);
      toast({
        title: 'Updated',
        description: 'Deployment updated successfully',
      });
    }
    fetchDeployments();
  };

  const getStateVariant = (state: string) => {
    if (state === 'RUNNING') return 'success';
    if (state === 'STOPPED') return 'secondary';
    return 'warning';
  };

  return (
    <Layout>
      <div className="space-y-6">
        {/* Header */}
        <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
          <div>
            <h1 className="text-2xl font-bold tracking-tight flex items-center gap-3">
              <Layers className="h-7 w-7 text-primary" />
              Deployments
            </h1>
            <p className="text-muted-foreground mt-1">
              Manage your deployment configurations
            </p>
          </div>
          <div className="flex items-center gap-2">
            <Button variant="outline" size="icon" onClick={fetchDeployments}>
              <RefreshCw className={`h-4 w-4 ${loading ? 'animate-spin' : ''}`} />
            </Button>
            <Button onClick={handleCreate}>
              <Plus className="h-4 w-4 mr-2" />
              Create Deployment
            </Button>
          </div>
        </div>

        {/* Filters */}
        <div className="flex flex-col sm:flex-row gap-3">
          <div className="relative flex-1 max-w-md">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
            <Input
              placeholder="Search by name or ID..."
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              className="pl-9"
            />
          </div>
          <Select value={stateFilter} onValueChange={setStateFilter}>
            <SelectTrigger className="w-[140px]">
              <SelectValue placeholder="State" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="ALL">All States</SelectItem>
              <SelectItem value="RUNNING">RUNNING</SelectItem>
              <SelectItem value="STOPPED">STOPPED</SelectItem>
            </SelectContent>
          </Select>
          <Select value={kindFilter} onValueChange={setKindFilter}>
            <SelectTrigger className="w-[130px]">
              <SelectValue placeholder="Kind" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="ALL">All Kinds</SelectItem>
              <SelectItem value="EXEC">EXEC</SelectItem>
              <SelectItem value="DOCKER">DOCKER</SelectItem>
            </SelectContent>
          </Select>
        </div>

        {/* Error State */}
        {error && (
          <div className="flex items-center gap-2 p-4 rounded-lg bg-destructive/10 border border-destructive/20 text-destructive">
            <AlertCircle className="h-5 w-5" />
            <span>{error}</span>
            <Button variant="outline" size="sm" className="ml-auto" onClick={fetchDeployments}>
              Retry
            </Button>
          </div>
        )}

        {/* Table */}
        <div className="rounded-xl border border-border overflow-hidden bg-card">
          <Table>
            <TableHeader>
              <TableRow className="bg-secondary/30 hover:bg-secondary/30">
                <TableHead className="font-semibold">Name</TableHead>
                <TableHead className="font-semibold">ID</TableHead>
                <TableHead className="font-semibold">Assigned Node</TableHead>
                <TableHead className="font-semibold">Toggle</TableHead>
                <TableHead className="font-semibold">Desired State</TableHead>
                <TableHead className="font-semibold">Current State</TableHead>
                <TableHead className="font-semibold">Kind</TableHead>
                <TableHead className="font-semibold">Updated</TableHead>
                <TableHead className="w-[60px]"></TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {loading ? (
                <TableRow>
                  <TableCell colSpan={9} className="h-32 text-center">
                    <RefreshCw className="h-6 w-6 animate-spin mx-auto text-muted-foreground" />
                  </TableCell>
                </TableRow>
              ) : filteredDeployments.length === 0 ? (
                <TableRow>
                  <TableCell colSpan={9} className="h-32 text-center text-muted-foreground">
                    No deployments found
                  </TableCell>
                </TableRow>
              ) : (
                filteredDeployments.map((deployment) => (
                  <TableRow key={deployment.id} className="group">
                    <TableCell className="font-medium">{deployment.name}</TableCell>
                    <TableCell>
                      <code className="text-xs bg-secondary px-1.5 py-0.5 rounded font-mono">
                        {truncateId(deployment.id)}
                      </code>
                    </TableCell>
                    <TableCell>
                      {deployment.assigned_node_id ? (
                        <code className="text-xs bg-secondary px-1.5 py-0.5 rounded font-mono">
                          {truncateId(deployment.assigned_node_id)}
                        </code>
                      ) : (
                        <Badge variant="secondary">
                          UNASSIGNED{deployment.assignment_reason ? ` • ${deployment.assignment_reason}` : ''}
                        </Badge>
                      )}
                    </TableCell>
                    <TableCell>
                      <Switch
                        checked={deployment.desired_state === 'RUNNING'}
                        onCheckedChange={() => handleToggleState(deployment)}
                        disabled={!!updatingStateById[deployment.id]}
                        aria-label={`Toggle deployment ${deployment.name}`}
                      />
                    </TableCell>
                    <TableCell>
                      <Badge variant={getStateVariant(deployment.desired_state)}>
                        {deployment.desired_state}
                      </Badge>
                    </TableCell>
                    <TableCell>
                      <Badge variant={getStateVariant(deployment.current_state)}>
                        {deployment.current_state}
                      </Badge>
                    </TableCell>
                    <TableCell>
                      <Badge variant="outline">{deployment.kind}</Badge>
                    </TableCell>
                    <TableCell className="text-muted-foreground text-sm">
                      {formatTimestamp(deployment.updated_at_ms)}
                    </TableCell>
                    <TableCell>
                      <DropdownMenu>
                        <DropdownMenuTrigger asChild>
                          <Button
                            variant="ghost"
                            size="icon"
                            className="opacity-0 group-hover:opacity-100 transition-opacity"
                          >
                            <MoreHorizontal className="h-4 w-4" />
                          </Button>
                        </DropdownMenuTrigger>
                        <DropdownMenuContent align="end">
                          <DropdownMenuItem onClick={() => handleView(deployment)}>
                            <Eye className="h-4 w-4 mr-2" />
                            View
                          </DropdownMenuItem>
                          <DropdownMenuItem onClick={() => handleEdit(deployment)}>
                            <Pencil className="h-4 w-4 mr-2" />
                            Edit
                          </DropdownMenuItem>
                          <DropdownMenuItem onClick={() => handleLogs(deployment)}>
                            <Terminal className="h-4 w-4 mr-2" />
                            Logs
                          </DropdownMenuItem>
                          <DropdownMenuItem
                            onClick={() => handleDeleteClick(deployment)}
                            className="text-destructive focus:text-destructive"
                          >
                            <Trash2 className="h-4 w-4 mr-2" />
                            Delete
                          </DropdownMenuItem>
                        </DropdownMenuContent>
                      </DropdownMenu>
                    </TableCell>
                  </TableRow>
                ))
              )}
            </TableBody>
          </Table>
        </div>

        {/* Pagination */}
        <div className="flex items-center justify-between">
          <p className="text-sm text-muted-foreground">
            Showing {filteredDeployments.length} of {deployments.length} deployments
          </p>
          <div className="flex items-center gap-2">
            <Button
              variant="outline"
              size="sm"
              disabled={offset === 0}
              onClick={() => setOffset(Math.max(0, offset - limit))}
            >
              Previous
            </Button>
            <Button
              variant="outline"
              size="sm"
              disabled={deployments.length < limit}
              onClick={() => setOffset(offset + limit)}
            >
              Next
            </Button>
          </div>
        </div>
      </div>

      {/* Modals */}
      <DeploymentModal
        open={modalOpen}
        onClose={() => setModalOpen(false)}
        onSubmit={handleModalSubmit}
        deployment={selectedDeployment}
        mode={modalMode}
      />

      <DeploymentViewDrawer
        open={viewDrawerOpen}
        onClose={() => setViewDrawerOpen(false)}
        deployment={selectedDeployment}
      />

      <DeploymentLogsDrawer
        open={logsDrawerOpen}
        onClose={() => setLogsDrawerOpen(false)}
        deploymentId={selectedDeployment?.id || null}
        deploymentName={selectedDeployment?.name || null}
      />

      <AlertDialog open={deleteDialogOpen} onOpenChange={setDeleteDialogOpen}>
        <AlertDialogContent className="bg-card border-border">
          <AlertDialogHeader>
            <AlertDialogTitle>Delete Deployment</AlertDialogTitle>
            <AlertDialogDescription>
              Are you sure you want to delete "{deploymentToDelete?.name}"? This action cannot be
              undone.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>Cancel</AlertDialogCancel>
            <AlertDialogAction
              onClick={handleDeleteConfirm}
              className="bg-destructive text-destructive-foreground hover:bg-destructive/90"
            >
              Delete
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </Layout>
  );
};

export default DeploymentsPage;
