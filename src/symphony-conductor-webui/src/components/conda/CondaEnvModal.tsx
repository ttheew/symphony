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
import { CondaEnvCreate } from '@/lib/api';

interface CondaEnvModalProps {
  open: boolean;
  onClose: () => void;
  onSubmit: (data: CondaEnvCreate) => Promise<void>;
}

const parsePackages = (value: string): string[] =>
  value
    .split(/[\n,]+/)
    .map((item) => item.trim())
    .filter(Boolean);

const CondaEnvModal = ({ open, onClose, onSubmit }: CondaEnvModalProps) => {
  const [name, setName] = useState('');
  const [pythonVersion, setPythonVersion] = useState('');
  const [packages, setPackages] = useState('');
  const [errors, setErrors] = useState<Record<string, string>>({});
  const [isSubmitting, setIsSubmitting] = useState(false);

  useEffect(() => {
    if (!open) return;
    setName('');
    setPythonVersion('');
    setPackages('');
    setErrors({});
  }, [open]);

  const parsedPackages = useMemo(() => parsePackages(packages), [packages]);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setErrors({});

    const newErrors: Record<string, string> = {};
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

    if (Object.keys(newErrors).length > 0) {
      setErrors(newErrors);
      return;
    }

    setIsSubmitting(true);
    try {
      await onSubmit({
        name: name.trim(),
        python_version: pythonVersion.trim(),
        packages: parsedPackages,
      });
    } finally {
      setIsSubmitting(false);
    }
  };

  return (
    <Dialog open={open} onOpenChange={(isOpen) => !isOpen && onClose()}>
      <DialogContent className="sm:max-w-lg bg-card border-border">
        <DialogHeader>
          <DialogTitle className="text-lg font-semibold">Create Conda Env</DialogTitle>
        </DialogHeader>

        <form onSubmit={handleSubmit} className="space-y-4">
          <div className="space-y-2">
            <Label htmlFor="conda-env-name">Env Name</Label>
            <Input
              id="conda-env-name"
              value={name}
              onChange={(e) => setName(e.target.value)}
              placeholder="e.g. py310-data"
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
              {isSubmitting ? 'Creating...' : 'Create'}
            </Button>
          </div>
        </form>
      </DialogContent>
    </Dialog>
  );
};

export default CondaEnvModal;
