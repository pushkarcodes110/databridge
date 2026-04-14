import axios from "axios";

// Using a custom env if needed, fallback to localhost
export const API_BASE_URL = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000/api";

export const api = axios.create({
  baseURL: API_BASE_URL,
});

export const getSettings = () => api.get("/settings").then((res) => res.data);
export const saveSettings = (data: any) => api.put("/settings", data).then((res) => res.data);
export const testNocoDBConnection = (data: any) => api.post("/settings/test", data).then((res) => res.data);

export const getBases = () => api.get("/nocodb/bases").then((res) => res.data);
export const getTables = (baseId: string) => api.get(`/nocodb/tables/${baseId}`).then((res) => res.data);
export const createTable = (baseId: string, data: any) => api.post(`/nocodb/tables/${baseId}`, data).then((res) => res.data);
export const getFields = (tableId: string, baseId?: string) => 
  api.get(`/nocodb/fields/${tableId}${baseId ? `?base_id=${baseId}` : ''}`).then((res) => res.data);

export const uploadChunk = (formData: FormData, onUploadProgress: any) =>
  api.post("/upload/chunk", formData, {
    headers: { "Content-Type": "multipart/form-data" },
    onUploadProgress,
  }).then((res) => res.data);

export const getPreview = (fileId: string, filename: string) =>
  api.get(`/upload/${fileId}/preview`, { params: { filename } }).then((res) => res.data);

export const createJob = (data: any) => api.post("/jobs/", data).then((res) => res.data);
export const getJobs = () => api.get("/jobs/").then((res) => res.data);
export const getJob = (jobId: string) => api.get(`/jobs/${jobId}`).then((res) => res.data);
export const getJobProgress = (jobId: string) => api.get(`/jobs/${jobId}/progress`).then((res) => res.data);
export const cancelJob = (jobId: string) => api.post(`/jobs/${jobId}/cancel`).then((res) => res.data);
export const resumeJob = (jobId: string) => api.post(`/jobs/${jobId}/resume`).then((res) => res.data);
