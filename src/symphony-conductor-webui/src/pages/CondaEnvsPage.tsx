import { useCallback, useEffect, useMemo, useState } from 'react';
import { Plus, RefreshCw, Search, Trash2, FlaskConical } from 'lucide-react';
import Layout from '@/components/Layout';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Badge } from '@/components/ui/badge';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table';
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
import { condaEnvsApi, CondaEnvCreate, CondaEnvResponse } from '@/lib/api';
import { formatTimestamp } from '@/lib/formatters';
import CondaEnvModal from '@/components/conda/CondaEnvModal';

const CondaEnvsPage = () => {
  const [envs, setEnvs] = useState<CondaEnvResponse[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [search, setSearch] = useState('');
  const [modalOpen, setModalOpen] = useState(false);
  const [deleteDialogOpen, setDeleteDialogOpen] = useState(false);
  const [envToDelete, setEnvToDelete] = useState<CondaEnvResponse | null>(null);

  const { toast } = useToast();

  const fetchEnvs = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const data = await condaEnvsApi.list();
      setEnvs(data);
    } catch (err: any) {
      setError(err?.message || 'Failed to fetch conda envs');
      toast({
        variant: 'destructive',
        title: 'Error',
        description: 'Failed to fetch conda envs',
      });
    } finally {
      setLoading(false);
    }
  }, [toast]);

  useEffect(() => {
    fetchEnvs();
  }, [fetchEnvs]);

  const filtered = useMemo(() => {
    const needle = search.trim().toLowerCase();
    if (!needle) return envs;
    return envs.filter((env) => env.name.toLowerCase().includes(needle));
  }, [envs, search]);

  const handleCreate = () => {
    setModalOpen(true);
  };

  const handleModalSubmit = async (data: CondaEnvCreate) => {
    try {
      await condaEnvsApi.create(data);
      toast({
        title: 'Created',
        description: `Conda env "${data.name}" has been created`,
      });
      setModalOpen(false);
      fetchEnvs();
    } catch (err: any) {
      toast({
        variant: 'destructive',
        title: 'Error',
        description: err?.detail || 'Failed to create conda env',
      });
    }
  };

  const handleDeleteClick = (env: CondaEnvResponse) => {
    setEnvToDelete(env);
    setDeleteDialogOpen(true);
  };

  const handleDeleteConfirm = async () => {
    if (!envToDelete) return;
    try {
      await condaEnvsApi.delete(envToDelete.name);
      toast({
        title: 'Deleted',
        description: `Conda env "${envToDelete.name}" has been deleted`,
      });
      fetchEnvs();
    } catch (err: any) {
      toast({
        variant: 'destructive',
        title: 'Error',
        description: err?.detail || 'Failed to delete conda env',
      });
    } finally {
      setDeleteDialogOpen(false);
      setEnvToDelete(null);
    }
  };

  return (
    <Layout>
      <div className="space-y-6">
        <div className="flex flex-col gap-4 md:flex-row md:items-center md:justify-between">
          <div>
            <div className="flex items-center gap-2">
              <FlaskConical className="h-5 w-5 text-primary" />
              <h1 className="text-2xl font-semibold">Conda Envs</h1>
            </div>
            <p className="text-sm text-muted-foreground mt-1">
              Manage Python conda environments required for deployments
            </p>
          </div>
          <div className="flex items-center gap-2">
            <Button variant="outline" onClick={fetchEnvs}>
              <RefreshCw className="h-4 w-4 mr-2" />
              Refresh
            </Button>
            <Button onClick={handleCreate}>
              <Plus className="h-4 w-4 mr-2" />
              Create Env
            </Button>
          </div>
        </div>

        <div className="flex flex-col gap-3 md:flex-row md:items-center md:justify-between">
          <div className="relative w-full md:max-w-sm">
            <Search className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
            <Input
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              placeholder="Search by env name"
              className="pl-9"
            />
          </div>
          <Badge variant="secondary" className="w-fit">
            {filtered.length} env{filtered.length === 1 ? '' : 's'}
          </Badge>
        </div>

        <div className="rounded-xl border border-border bg-card overflow-hidden">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Name</TableHead>
                <TableHead>Python</TableHead>
                <TableHead>Packages</TableHead>
                <TableHead>Created</TableHead>
                <TableHead className="text-right">Actions</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {loading && (
                <TableRow>
                  <TableCell colSpan={5} className="text-center text-muted-foreground">
                    Loading conda envs...
                  </TableCell>
                </TableRow>
              )}
              {!loading && filtered.length === 0 && (
                <TableRow>
                  <TableCell colSpan={5} className="text-center text-muted-foreground">
                    No conda envs found
                  </TableCell>
                </TableRow>
              )}
              {!loading &&
                filtered.map((env) => {
                  const packages = env.packages ?? [];
                  const preview =
                    packages.length > 0
                      ? `${packages.slice(0, 3).join(', ')}${
                          packages.length > 3 ? ` +${packages.length - 3} more` : ''
                        }`
                      : '—';
                  return (
                    <TableRow key={env.name}>
                      <TableCell className="font-medium">{env.name}</TableCell>
                      <TableCell>{env.python_version}</TableCell>
                      <TableCell className="text-muted-foreground">{preview}</TableCell>
                      <TableCell>{formatTimestamp(env.created_at_ms)}</TableCell>
                      <TableCell className="text-right">
                        <Button
                          variant="ghost"
                          size="icon"
                          onClick={() => handleDeleteClick(env)}
                        >
                          <Trash2 className="h-4 w-4" />
                        </Button>
                      </TableCell>
                    </TableRow>
                  );
                })}
              {!loading && error && (
                <TableRow>
                  <TableCell colSpan={5} className="text-center text-destructive">
                    {error}
                  </TableCell>
                </TableRow>
              )}
            </TableBody>
          </Table>
        </div>
      </div>

      <CondaEnvModal
        open={modalOpen}
        onClose={() => setModalOpen(false)}
        onSubmit={handleModalSubmit}
      />

      <AlertDialog open={deleteDialogOpen} onOpenChange={setDeleteDialogOpen}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Delete Conda Env</AlertDialogTitle>
            <AlertDialogDescription>
              {envToDelete
                ? `Are you sure you want to delete "${envToDelete.name}"?`
                : 'Are you sure you want to delete this env?'}
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>Cancel</AlertDialogCancel>
            <AlertDialogAction onClick={handleDeleteConfirm}>Delete</AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </Layout>
  );
};

export default CondaEnvsPage;
