import type { ObservationEvent } from "@/lib/observation/contracts";
import { memo, useCallback, useState } from "react";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { Button } from "@/components/ui/button";
import {
  Sheet,
  SheetClose,
  SheetContent,
  SheetHeader,
  SheetTitle,
} from "@/components/ui/sheet";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { record } from "@/lib/observation/contracts";
import { duration, label, time } from "@/lib/observation/format";
import { Empty, HashLink, Status } from "./common";

const EventRow = memo(({ event, onSelect }: {
  event: ObservationEvent;
  onSelect: (event: ObservationEvent) => void;
}) => {
  return (
    <TableRow>
      <TableCell className="text-xs whitespace-nowrap text-muted-foreground">
        {time(event.at_ms)}
      </TableCell>
      <TableCell>
        <strong>{label(event.kind)}</strong>
        <div className="text-xs text-muted-foreground">{label(event.step)}</div>
      </TableCell>
      <TableCell>
        <Status value={event.result} />
      </TableCell>
      <TableCell>
        {(event.context.hash != null && event.context.hash !== "")
          ? (
              <HashLink hash={event.context.hash} />
            )
          : (
              <code>
                {event.context.batch_id
                  ?? event.context.observation_id
                  ?? event.context.rpc_id
                  ?? "—"}
              </code>
            )}
      </TableCell>
      <TableCell>
        <Button
          variant="ghost"
          size="sm"
          onClick={() => onSelect(event)}
        >
          查看 #
          {event.sequence}
        </Button>
      </TableCell>
    </TableRow>
  );
});
const EventList = memo(({ events, onSelect }: {
  events: ObservationEvent[];
  onSelect: (event: ObservationEvent) => void;
}) => {
  return (
    <>
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead>时间</TableHead>
            <TableHead>领域 / 步骤</TableHead>
            <TableHead>结果</TableHead>
            <TableHead>关联对象</TableHead>
            <TableHead>证据</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {events.map(event => <EventRow key={`${event.run_id}:${event.sequence}`} event={event} onSelect={onSelect} />)}
        </TableBody>
      </Table>
      {events.length === 0 && <Empty />}
    </>
  );
});
const EventEvidence = memo(({ selected, onOpenChange }: {
  selected: ObservationEvent | undefined;
  onOpenChange: (open: boolean) => void;
}) => {
  return (
    <Sheet
      open={selected !== undefined}
      onOpenChange={onOpenChange}
    >
      <SheetContent
        side="right"
        showCloseButton={false}
        className="overflow-y-auto sm:max-w-xl"
      >
        <SheetHeader className="flex-row items-center justify-between">
          <SheetTitle>事件证据</SheetTitle>
          <SheetClose render={<Button variant="outline" size="sm" />}>
            关闭
          </SheetClose>
        </SheetHeader>
        {selected && (
          <div className="grid gap-3 px-4 pb-4">
            <div className="flex flex-wrap items-center gap-2.5">
              <Status value={selected.result} />
              <span>
                {label(selected.kind)}
                {" "}
                /
                {label(selected.step)}
              </span>
            </div>
            <p className="text-xs text-muted-foreground">
              {time(selected.at_ms)}
              {" "}
              · 阶段耗时
              {" "}
              {duration(record(selected.data).elapsed_ms)}
            </p>
            {selected.truncated && (
              <Alert>
                <AlertDescription>
                  该事件载荷已截断，不能视为完整记录。
                </AlertDescription>
              </Alert>
            )}
            <pre className="rounded-md bg-muted p-4 text-[11px] leading-7 wrap-break-word whitespace-pre-wrap">
              {JSON.stringify(selected, null, 2)}
            </pre>
          </div>
        )}
      </SheetContent>
    </Sheet>
  );
});
export const EventTable = memo(({ events }: { events: ObservationEvent[] }) => {
  const [selected, setSelected] = useState<ObservationEvent>();
  const onOpenChange = useCallback((open: boolean) => {
    if (!open)
      setSelected(undefined);
  }, []);
  return (
    <>
      <EventList events={events} onSelect={setSelected} />
      <EventEvidence selected={selected} onOpenChange={onOpenChange} />
    </>
  );
});
