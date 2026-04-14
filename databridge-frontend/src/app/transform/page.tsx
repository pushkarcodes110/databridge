import { TransformUpload } from "@/components/transform/TransformUpload";

export default function TransformPage() {
  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-3xl font-bold tracking-tight">Transform & Filter Data</h1>
      </div>

      <TransformUpload />
    </div>
  );
}
