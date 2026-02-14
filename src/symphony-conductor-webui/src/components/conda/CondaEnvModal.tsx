import { useEffect, useMemo, useState } from 'react';
import { X } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Textarea } from '@/components/ui/textarea';
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import { CondaEnvCreate, CondaEnvResponse, CondaEnvUpdate } from '@/lib/api';

interface CondaEnvModalProps {
  open: boolean;
  onClose: () => void;
  mode: 'create' | 'edit';
  initialEnv?: CondaEnvResponse | null;
  onSubmit: (data: CondaEnvCreate | CondaEnvUpdate) => Promise<void>;
}

const parsePackages = (value: string): string[] =>
  value
    .split(/[\n,]+/)
    .map((item) => item.trim())
    .filter(Boolean);

const CondaEnvModal = ({
  open,
  onClose,
  mode,
  initialEnv,
  onSubmit,
}: CondaEnvModalProps) => {
  const [name, setName] = useState('');
  const [pythonVersion, setPythonVersion] = useState('');
  const [packages, setPackages] = useState('');
  const [customScript, setCustomScript] = useState('');
  const [errors, setErrors] = useState<Record<string, string>>({});
  const [isSubmitting, setIsSubmitting] = useState(false);

  useEffect(() => {
    if (!open) return;
    if (mode === 'edit' && initialEnv) {
      setName(initialEnv.name);
      setPythonVersion(initialEnv.python_version);
      setPackages((initialEnv.packages ?? []).join('\n'));
      setCustomScript(initialEnv.custom_script ?? '');
    } else {
      setName('');
      setPythonVersion('');
      setPackages('');
      setCustomScript('');
    }
    setErrors({});
  }, [open, mode, initialEnv]);

  const parsedPackages = useMemo(() => parsePackages(packages), [packages]);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setErrors({});

    const newErrors: Record<string, string> = {};
    if (mode === 'create') {
      if (!name.trim()) {
        newErrors.name = 'Name is required';
      } else if (name.length > 200) {
        newErrors.name = 'Name must be less than 200 characters';
      }
      if (!pythonVersion.trim()) {
        newErrors.pythonVersion = 'Python version is required';
      } else if (pythonVersion.length > 20) {
        newErrors.pythonVersion = 'Python version must be less than 20 characters';
      }
    }
    if (customScript.length > 2000) {
      newErrors.customScript = 'Custom script must be less than 2000 characters';
    }

    if (Object.keys(newErrors).length > 0) {
      setErrors(newErrors);
      return;
    }

    setIsSubmitting(true);
    try {
      if (mode === 'edit') {
        await onSubmit({
          packages: parsedPackages,
          custom_script: customScript.trim(),
        });
      } else {
        await onSubmit({
          name: name.trim(),
          python_version: pythonVersion.trim(),
          packages: parsedPackages,
          custom_script: customScript.trim(),
        });
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
            {mode === 'edit' ? 'Edit Conda Env' : 'Create Conda Env'}
          </DialogTitle>
        </DialogHeader>

        <form onSubmit={handleSubmit} className="space-y-4">
          <div className="space-y-2">
            <Label htmlFor="conda-env-name">Env Name</Label>
            <Input
              id="conda-env-name"
              value={name}
              onChange={(e) => setName(e.target.value)}
              placeholder="e.g. py310-data"
              disabled={mode === 'edit'}
            />
            {errors.name && (
              <p className="text-xs text-destructive flex items-center gap-1">
                <X className="h-3 w-3" /> {errors.name}
              </p>
            )}
          </div>

          <div className="space-y-2">
            <Label htmlFor="conda-env-python">Python Version</Label>
            <Input
              id="conda-env-python"
              value={pythonVersion}
              onChange={(e) => setPythonVersion(e.target.value)}
              placeholder="e.g. 3.10"
              disabled={mode === 'edit'}
            />
            {errors.pythonVersion && (
              <p className="text-xs text-destructive flex items-center gap-1">
                <X className="h-3 w-3" /> {errors.pythonVersion}
              </p>
            )}
          </div>

          <div className="space-y-2">
            <Label htmlFor="conda-env-packages">Packages</Label>
            <Textarea
              id="conda-env-packages"
              value={packages}
              onChange={(e) => setPackages(e.target.value)}
              placeholder="numpy, pandas\nscikit-learn"
              rows={4}
            />
            <p className="text-xs text-muted-foreground">
              {parsedPackages.length} package{parsedPackages.length === 1 ? '' : 's'} detected
            </p>
          </div>

          <div className="space-y-2">
            <Label htmlFor="conda-env-custom-script">Custom Script (Optional)</Label>
            <Textarea
              id="conda-env-custom-script"
              value={customScript}
              onChange={(e) => setCustomScript(e.target.value)}
              placeholder={'echo "setup before package install"'}
              rows={4}
            />
            {errors.customScript && (
              <p className="text-xs text-destructive flex items-center gap-1">
                <X className="h-3 w-3" /> {errors.customScript}
              </p>
            )}
          </div>

          <div className="flex items-center justify-end gap-2">
            <Button
              type="button"
              variant="outline"
              onClick={onClose}
              disabled={isSubmitting}
            >
              Cancel
            </Button>
            <Button type="submit" disabled={isSubmitting}>
              {mode === 'edit'
                ? isSubmitting
                  ? 'Saving...'
                  : 'Save'
                : isSubmitting
                  ? 'Creating...'
                  : 'Create'}
            </Button>
          </div>
        </form>
      </DialogContent>
    </Dialog>
  );
};

export default CondaEnvModal;
