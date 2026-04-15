"use client";

import { useState, useEffect } from "react";
import { FileUpload } from "@/components/FileUpload";
import { DataPreview } from "@/components/DataPreview";
import { ColumnMapper } from "@/components/ColumnMapper";
import { ImportProgress } from "@/components/ImportProgress";
import { QuickImportOptions } from "@/components/QuickImportOptions";
import { getPreview, getBases, getTables, getFields, createJob, createTable } from "@/lib/api";
import { Database } from "lucide-react";
import { Button } from "@/components/ui/button";
import { toast } from "sonner";

export default function Home() {
  const [step, setStep] = useState<"upload" | "map" | "importing">("upload");
  
  // File Context
  const [fileId, setFileId] = useState("");
  const [filename, setFilename] = useState("");
  const [previewData, setPreviewData] = useState<any>({ columns: [], rows: [] });
  
  // NocoDB Target Context
  const [bases, setBases] = useState<any[]>([]);
  const [tables, setTables] = useState<any[]>([]);
  const [fields, setFields] = useState<any[]>([]);
  
  const [selectedBase, setSelectedBase] = useState("");
  const [selectedTable, setSelectedTable] = useState("");
  const [isCreatingTable, setIsCreatingTable] = useState(false);
  const [newTableName, setNewTableName] = useState("");
  const [isCreatingInBackend, setIsCreatingInBackend] = useState(false);
  const [jobId, setJobId] = useState("");

  // Load Bases on Map step mount
  useEffect(() => {
    if (step === "map") {
      getBases()
        .then(setBases)
        .catch(() => toast.error("Configure NocoDB in settings first."));
    }
  }, [step]);

  // Load Tables when Base changes
  useEffect(() => {
    if (selectedBase) {
      getTables(selectedBase).then(setTables).catch(console.error);
    } else {
      setTables([]);
      setSelectedTable("");
    }
  }, [selectedBase]);

  // Load Fields when Table changes
  useEffect(() => {
    if (isCreatingTable && previewData?.columns) {
        // Mock fields from CSV columns for mapping, matching backend sanitization
        const mockFields = [
            { title: "Id", column_name: "id", uidt: "ID" },
            ...previewData.columns.map((col: string) => {
                // Ensure unique and clean column names
                let safeName = col.trim()
                    .replace(/\s+/g, "_")
                    .replace(/[^a-zA-Z0-9_]/g, "")
                    .toLowerCase();
                
                if (safeName === "id" || !safeName) safeName = `csv_${safeName || "field"}`;
                
                return { 
                    title: col, 
                    column_name: safeName, 
                    uidt: "SingleLineText" 
                };
            })
        ];
        setFields(mockFields);
    } else if (selectedTable && selectedTable !== "new") {
      getFields(selectedTable, selectedBase).then(setFields).catch(console.error);
    } else {
      setFields([]);
    }
  }, [selectedTable, isCreatingTable, previewData.columns, selectedBase]);

  const handleMappingComplete = async (mapping: Record<string, string>) => {
    if (!selectedBase || (!selectedTable && !isCreatingTable)) {
      toast.warning("Select a target Base and Table before importing.");
      return;
    }
    
    let targetTableId = selectedTable;

    try {
      if (isCreatingTable) {
          // Now create the table with ONLY the columns that were mapped
          const mappedColumns = Object.values(mapping);
          const res = await createTable(selectedBase, {
            table_name: newTableName,
            columns: mappedColumns
          });
          targetTableId = res.id;
      }

      const job = await createJob({
        filename,
        file_path: `${fileId}_${filename}`,
        file_size: previewData.stats?.total_rows || previewData.rows.length,
        total_rows: previewData.stats?.total_rows || previewData.rows.length,
        file_format: filename.split('.').pop(),
        nocodb_base_id: selectedBase,
        nocodb_table_id: targetTableId,
        column_mapping: mapping,
        options: {}
      });
      
      setJobId(job.id);
      setStep("importing");
      toast.success("Import job started.");
    } catch (e) {
      toast.error("Failed to start job or create table.");
    }
  };

  const handleUploadComplete = async (id: string, name: string) => {
    setFileId(id);
    setFilename(name);
    try {
      const data = await getPreview(id, name);
      setPreviewData(data);
      setStep("map");
    } catch (e) {
      toast.error("Failed to generate file preview.");
    }
  };



  return (
    <div className="space-y-8 animate-in fade-in duration-500">
      
      {step === "upload" && (
        <div className="max-w-3xl mx-auto space-y-6">
          <div className="text-center mb-10">
            <h1 className="text-4xl font-extrabold tracking-tight mb-3">Quick Import</h1>
            <p className="text-lg text-muted-foreground">Upload large CSV files without crashing your browser. DataBridge chunks and streams your payload natively.</p>
          </div>
          <QuickImportOptions />
          <FileUpload onUploadComplete={handleUploadComplete} />
        </div>
      )}

      {step === "map" && (
        <div className="space-y-6">
          <div className="flex items-center justify-between">
            <h1 className="text-3xl font-bold tracking-tight">Configure Import</h1>
          </div>
          
          <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
            <div className="lg:col-span-1 space-y-6">
               <div className="bg-card border rounded-2xl shadow-sm p-6 backdrop-blur-xl space-y-4">
                 <h2 className="font-semibold flex items-center"><Database className="w-4 h-4 mr-2 text-primary" /> Target Destination</h2>
                 
                 <div>
                   <label className="text-sm font-medium mb-1 block text-muted-foreground">Select Base</label>
                   <select 
                     value={selectedBase} 
                     onChange={e => setSelectedBase(e.target.value)}
                     className="w-full bg-background border rounded-lg px-4 py-2.5 text-sm outline-none focus:ring-2 focus:ring-primary"
                   >
                     <option value="">-- Choose Base --</option>
                     {bases.map(b => <option key={b.id} value={b.id}>{b.title}</option>)}
                   </select>
                 </div>

                 {selectedBase && (
                   <div className="space-y-4">
                     <div>
                       <label className="text-sm font-medium mb-1 block text-muted-foreground">Select Table</label>
                       <select 
                         value={isCreatingTable ? "new" : selectedTable} 
                         onChange={e => {
                           if (e.target.value === "new") {
                             setIsCreatingTable(true);
                             setSelectedTable("");
                           } else {
                             setIsCreatingTable(false);
                             setSelectedTable(e.target.value);
                           }
                         }}
                         className="w-full bg-background border rounded-lg px-4 py-2.5 text-sm outline-none focus:ring-2 focus:ring-primary"
                       >
                         <option value="">-- Choose Table --</option>
                         <option value="new" className="font-bold text-primary">+ Create New Table</option>
                         {tables.map(t => <option key={t.id} value={t.id}>{t.title}</option>)}
                       </select>
                     </div>
                     
                     {isCreatingTable && (
                       <div className="bg-muted/50 p-4 rounded-lg border space-y-3">
                         <h3 className="text-sm font-semibold">New Table Details</h3>
                         <input 
                           type="text" 
                           placeholder="Table Name" 
                           value={newTableName}
                           onChange={e => setNewTableName(e.target.value)}
                           className="w-full bg-background border rounded-lg px-3 py-2 text-sm outline-none"
                         />
                         <p className="text-xs text-muted-foreground">Map columns below. The table will be created when you start the import.</p>
                       </div>
                     )}
                   </div>
                 )}
               </div>
            </div>

            <div className="lg:col-span-2 space-y-6">
               <DataPreview columns={previewData.columns} rows={previewData.rows} stats={previewData.stats} />
               
               {(selectedTable || (isCreatingTable && newTableName)) ? (
                  <ColumnMapper 
                    csvColumns={previewData.columns} 
                    nocoFields={fields} 
                    onMapComplete={handleMappingComplete}
                    onCancel={() => setStep("upload")}
                  />
               ) : (
                  <div className="bg-muted/30 border border-dashed rounded-2xl p-8 text-center text-muted-foreground">
                    Select a target Base and Table to map columns before importing.
                  </div>
               )}
            </div>
          </div>
        </div>
      )}

      {step === "importing" && (
        <div className="pt-10">
          <ImportProgress jobId={jobId} />
          
          <div className="mt-10 text-center">
             <Button variant="outline" onClick={() => setStep("upload")}>Quick Import Another File</Button>
          </div>
        </div>
      )}

    </div>
  );
}
