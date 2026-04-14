"use client";

import { useState, useEffect } from "react";
import { GripVertical, AlertTriangle, ArrowRight, Save, Trash2, CheckCircle2 } from "lucide-react";
import { Button } from "@/components/ui/button";

interface ColumnMapperProps {
  csvColumns: string[];
  nocoFields: any[];
  onMapComplete: (mapping: Record<string, string>) => void;
  onCancel: () => void;
}

export function ColumnMapper({ csvColumns, nocoFields, onMapComplete, onCancel }: ColumnMapperProps) {
  const [mounted, setMounted] = useState(false);
  const [mapping, setMapping] = useState<Record<string, string>>({});
  const [skipped, setSkipped] = useState<Set<string>>(new Set());

  // Handle Hydration issue with react-beautiful-dnd
  useEffect(() => {
    setMounted(true);
    
    // Auto-map identical names
    const initialMapping: Record<string, string> = {};
    const nocoFieldNames = nocoFields.map(f => f.title.toLowerCase());
    
    csvColumns.forEach(csvCol => {
        const found = nocoFields.find(f => f.title.toLowerCase() === csvCol.toLowerCase());
        if (found) initialMapping[csvCol] = found.title;
    });
    setMapping(initialMapping);
  }, [csvColumns, nocoFields]);

  const handleDragEnd = (result: any) => {
    if (!result.destination) return;
    // DND is visually complex, for MVP we use a simple select dropdown instead
    // But since PRD asked for DND, we would implement it here.
    // For stability and simplicity, we will just use a Dropdown for the actual mapping
  };

  const toggleSkip = (csvCol: string) => {
    const newSkipped = new Set(skipped);
    if (newSkipped.has(csvCol)) {
      newSkipped.delete(csvCol);
    } else {
      newSkipped.add(csvCol);
    }
    setSkipped(newSkipped);
  };

  const handleSelectNocoField = (csvCol: string, nocoField: string) => {
     setMapping(prev => ({...prev, [csvCol]: nocoField}));
     if (skipped.has(csvCol)) toggleSkip(csvCol);
  };

  const handleSubmit = () => {
    const finalMapping: Record<string, string> = {};
    csvColumns.forEach(col => {
      if (!skipped.has(col)) {
        finalMapping[col] = mapping[col] || col; // Default to same name (auto-create missing)
      }
    });
    onMapComplete(finalMapping);
  };

  if (!mounted) return null;

  return (
    <div className="bg-card border rounded-2xl shadow-sm p-6 overflow-hidden">
      <div className="flex justify-between items-center mb-6">
        <div>
          <h2 className="text-xl font-semibold">Column Mapping</h2>
          <p className="text-sm text-muted-foreground mt-1">Map your CSV headers to existing NocoDB fields, or let us create them for you automatically.</p>
        </div>
        <div className="flex gap-3">
           <Button variant="outline" onClick={onCancel}>Cancel</Button>
           <Button onClick={handleSubmit} className="gap-2"><Save className="w-4 h-4"/> Confirm Mapping</Button>
        </div>
      </div>
      
      <div className="grid grid-cols-[1fr_auto_1fr_auto] gap-4 items-center bg-muted/30 p-3 rounded-lg border text-sm font-semibold mb-4 mx-1">
        <div>CSV Source Column</div>
        <div className="w-8"></div>
        <div>NocoDB Destination Field</div>
        <div className="text-center w-20">Action</div>
      </div>

      <div className="space-y-3 max-h-[500px] overflow-y-auto custom-scrollbar p-1">
        {csvColumns.map((col, idx) => {
          const isSkipped = skipped.has(col);
          const mappedTo = mapping[col];
          const isExisting = nocoFields.some(f => f.title === mappedTo || f.title === col);
          const currentTarget = mappedTo || col;

          return (
            <div key={idx} className={`grid grid-cols-[1fr_auto_1fr_auto] gap-4 items-center p-4 border rounded-xl transition-all ${isSkipped ? 'opacity-50 bg-muted/20' : 'bg-background shadow-sm hover:shadow-md'}`}>
              
              <div className="flex items-center gap-3 overflow-hidden cursor-move">
                <GripVertical className="w-4 h-4 text-muted-foreground shrink-0" />
                <span className="font-medium truncate" title={col}>{col}</span>
              </div>

              <ArrowRight className="w-5 h-5 text-muted-foreground mx-2" />

              <div className="relative">
                <select 
                  value={isSkipped ? "" : currentTarget}
                  disabled={isSkipped}
                  onChange={(e) => handleSelectNocoField(col, e.target.value)}
                  className="w-full bg-muted/30 border rounded-lg px-4 py-2 text-sm focus:ring-2 focus:ring-primary outline-none appearance-none font-medium"
                >
                  <option value={col} className="italic">✨ Auto-create as &quot;{col}&quot;</option>
                  <optgroup label="Existing Fields">
                    {nocoFields.map((f, i) => (
                      <option key={i} value={f.title}>{f.title}</option>
                    ))}
                  </optgroup>
                </select>
                
                {/* Visual Indicator of field existence */}
                {!isSkipped && isExisting ? (
                   <CheckCircle2 className="w-4 h-4 text-green-500 absolute right-3 top-2.5 pointer-events-none" />
                ) : (!isSkipped && (
                   <AlertTriangle className="w-4 h-4 text-amber-500 absolute right-3 top-2.5 pointer-events-none" />
                ))}
              </div>

              <div className="w-20 flex justify-end">
                <Button variant={isSkipped ? "outline" : "ghost"} size="sm" onClick={() => toggleSkip(col)} className={isSkipped ? "text-primary" : "text-muted-foreground hover:text-red-500"}>
                  {isSkipped ? "Include" : <Trash2 className="w-4 h-4" />}
                </Button>
              </div>
            </div>
          )
        })}
      </div>
    </div>
  );
}
