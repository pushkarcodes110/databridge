import { Card, CardContent } from "@/components/ui/card";

export interface StatsBarProps {
  totalRows: number;
  rowsRemaining: number;
  rowsRemoved: number;
  duplicatesRemoved: number;
}

export default function StatsBar({
  totalRows,
  rowsRemaining,
  rowsRemoved,
  duplicatesRemoved,
}: StatsBarProps) {
  return (
    <div className="sticky top-0 z-10 mb-6 bg-background/95 pb-4 pt-4 backdrop-blur supports-[backdrop-filter]:bg-background/60">
      <div className="grid grid-cols-2 gap-4 md:grid-cols-4">
        <Card>
          <CardContent className="p-6 flex flex-col justify-center items-center text-center sm:items-start sm:text-left">
            <div className="text-3xl font-bold">{totalRows.toLocaleString()}</div>
            <div className="text-sm font-medium text-muted-foreground mt-1">Total Rows</div>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="p-6 flex flex-col justify-center items-center text-center sm:items-start sm:text-left">
            <div className="text-3xl font-bold text-green-500">{rowsRemaining.toLocaleString()}</div>
            <div className="text-sm font-medium text-muted-foreground mt-1">Rows Remaining</div>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="p-6 flex flex-col justify-center items-center text-center sm:items-start sm:text-left">
            <div className="text-3xl font-bold text-red-500">{rowsRemoved.toLocaleString()}</div>
            <div className="text-sm font-medium text-muted-foreground mt-1">Rows Removed</div>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="p-6 flex flex-col justify-center items-center text-center sm:items-start sm:text-left">
            <div className="text-3xl font-bold text-orange-500">{duplicatesRemoved.toLocaleString()}</div>
            <div className="text-sm font-medium text-muted-foreground mt-1">Duplicates Removed</div>
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
