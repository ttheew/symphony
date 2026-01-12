import { useState, useEffect, useMemo } from 'react';
import { X, AlertCircle, Code } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Textarea } from '@/components/ui/textarea';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import { DeploymentCreate, DeploymentResponse, DeploymentUpdate } from '@/lib/api';

interface DeploymentModalProps {
  open: boolean;
  onClose: () => void;
  onSubmit: (data: DeploymentCreate | DeploymentUpdate) => Promise<void>;
  deployment?: DeploymentResponse | null;
  mode: 'create' | 'edit';
}

const DeploymentModal = ({
  open,
  onClose,
  onSubmit,
  deployment,
  mode,
}: DeploymentModalProps) => {
  const [name, setName] = useState('');
  const [desiredState, setDesiredState] = useState<'RUNNING' | 'STOPPED'>('RUNNING');
  const [kind, setKind] = useState<'EXEC' | 'DOCKER'>('EXEC');
  const [specification, setSpecification] = useState('{}');
  const [jsonError, setJsonError] = useState<string | null>(null);
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [errors, setErrors] = useState<Record<string, string>>({});
  const [initialValues, setInitialValues] = useState<{
    name: string;
    desiredState: 'RUNNING' | 'STOPPED';
    kind: 'EXEC' | 'DOCKER';
    specification: string;
  } | null>(null);

  useEffect(() => {
    if (deployment && mode === 'edit') {
      setName(deployment.name);
      setDesiredState(deployment.desired_state);
      setKind(deployment.kind);
      const specString = JSON.stringify(deployment.specification, null, 2);
      setSpecification(specString);
      setInitialValues({
        name: deployment.name,
        desiredState: deployment.desired_state,
        kind: deployment.kind,
        specification: specString,
      });
    } else {
      setName('');
      setDesiredState('RUNNING');
      setKind('EXEC');
      setSpecification('{}');
      setInitialValues(null);
    }
    setJsonError(null);
    setErrors({});
  }, [deployment, mode, open]);

  const validateJson = (value: string): boolean => {
    try {
      JSON.parse(value);
      setJsonError(null);
      return true;
    } catch (e) {
      setJsonError((e as Error).message);
      return false;
    }
  };

  const formatJson = () => {
    try {
      const parsed = JSON.parse(specification);
      setSpecification(JSON.stringify(parsed, null, 2));
      setJsonError(null);
    } catch (e) {
      setJsonError((e as Error).message);
    }
  };

  const hasChanges = useMemo(() => {
    if (mode !== 'edit' || !initialValues) return true;
    if (name !== initialValues.name) return true;
    if (desiredState !== initialValues.desiredState) return true;
    if (specification !== initialValues.specification) return true;
    return false;
  }, [mode, initialValues, name, desiredState, specification]);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setErrors({});

    // Validate
    const newErrors: Record<string, string> = {};
    if (!name.trim()) {
      newErrors.name = 'Name is required';
    } else if (name.length > 200) {
      newErrors.name = 'Name must be less than 200 characters';
    }

    if (!validateJson(specification)) {
      newErrors.specification = 'Invalid JSON';
    }

    if (Object.keys(newErrors).length > 0) {
      setErrors(newErrors);
      return;
    }

    setIsSubmitting(true);
    try {
      let payload: DeploymentCreate | DeploymentUpdate;

      if (mode === 'create') {
        payload = {
          name,
          desired_state: desiredState,
          kind,
          specification: JSON.parse(specification),
        } as DeploymentCreate;
      } else {
        const update: DeploymentUpdate = {};
        if (initialValues) {
          if (name !== initialValues.name) {
            update.name = name;
          }
          if (desiredState !== initialValues.desiredState) {
            update.desired_state = desiredState;
          }
          if (specification !== initialValues.specification) {
            update.specification = JSON.parse(specification);
          }
        }
        payload = update;
      }

      await onSubmit(payload);
      onClose();
    } catch (error: any) {
      if (error?.detail) {
        // Handle validation errors from API
        const apiErrors: Record<string, string> = {};
        if (Array.isArray(error.detail)) {
          error.detail.forEach((err: any) => {
            const field = err.loc?.[1] || 'general';
            apiErrors[field] = err.msg;
          });
        } else {
          apiErrors.general = error.detail;
        }
        setErrors(apiErrors);
      }
    } finally {
      setIsSubmitting(false);
    }
  };

  return (
    <Dialog open={open} onOpenChange={(isOpen) => !isOpen && onClose()}>
      <DialogContent className="sm:max-w-lg bg-card border-border">
        <DialogHeader>
          <DialogTitle className="text-lg font-semibold">
            {mode === 'create' ? 'Create Deployment' : 'Edit Deployment'}
          </DialogTitle>
        </DialogHeader>

        <form onSubmit={handleSubmit} className="space-y-4">
          {errors.general && (
            <div className="flex items-center gap-2 p-3 rounded-lg bg-destructive/10 border border-destructive/20 text-destructive text-sm">
              <AlertCircle className="h-4 w-4 flex-shrink-0" />
              {errors.general}
            </div>
          )}

          <div className="space-y-2">
            <Label htmlFor="name">Name</Label>
            <Input
              id="name"
              value={name}
              onChange={(e) => setName(e.target.value)}
              placeholder="my-deployment"
              className={errors.name ? 'border-destructive' : ''}
            />
            {errors.name && (
              <p className="text-xs text-destructive">{errors.name}</p>
            )}
          </div>

          <div className="grid grid-cols-2 gap-4">
            <div className="space-y-2">
              <Label>Desired State</Label>
              <Select value={desiredState} onValueChange={(v: 'RUNNING' | 'STOPPED') => setDesiredState(v)}>
                <SelectTrigger>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="RUNNING">RUNNING</SelectItem>
                  <SelectItem value="STOPPED">STOPPED</SelectItem>
                </SelectContent>
              </Select>
            </div>

            {mode === 'create' && (
              <div className="space-y-2">
                <Label>Kind</Label>
                <Select value={kind} onValueChange={(v: 'EXEC' | 'DOCKER') => setKind(v)}>
                  <SelectTrigger>
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="EXEC">EXEC</SelectItem>
                    <SelectItem value="DOCKER">DOCKER</SelectItem>
                  </SelectContent>
                </Select>
              </div>
            )}
          </div>

          <div className="space-y-2">
            <div className="flex items-center justify-between">
              <Label htmlFor="specification">Specification (JSON)</Label>
              <Button
                type="button"
                variant="ghost"
                size="sm"
                onClick={formatJson}
                className="h-7 text-xs"
              >
                <Code className="h-3 w-3 mr-1" />
                Format
              </Button>
            </div>
            <Textarea
              id="specification"
              value={specification}
              onChange={(e) => {
                setSpecification(e.target.value);
                validateJson(e.target.value);
              }}
              className={`font-mono text-sm min-h-[150px] ${
                jsonError || errors.specification ? 'border-destructive' : ''
              }`}
              placeholder="{}"
            />
            {(jsonError || errors.specification) && (
              <p className="text-xs text-destructive">
                {jsonError || errors.specification}
              </p>
            )}
          </div>

          <div className="flex justify-end gap-3 pt-4">
            <Button type="button" variant="outline" onClick={onClose}>
              Cancel
            </Button>
            <Button
              type="submit"
              disabled={isSubmitting || (mode === 'edit' && !hasChanges)}
            >
              {isSubmitting ? 'Saving...' : mode === 'create' ? 'Create' : 'Update'}
            </Button>
          </div>
        </form>
      </DialogContent>
    </Dialog>
  );
};

export default DeploymentModal;
