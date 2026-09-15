import type { ObservationEvent } from "@/lib/observation/contracts";
import { useEffect, useRef, useState } from "react";
import { record } from "@/lib/observation/contracts";
import { duration, label, time } from "@/lib/observation/format";
import { Empty, HashLink, Status } from "./common";

export function EventTable({ events }: { events: ObservationEvent[] }) {
  const [selected, setSelected] = useState<ObservationEvent>();
  const dialogRef = useRef<HTMLDialogElement>(null);
  useEffect(() => {
    if (selected)
      dialogRef.current?.showModal();
  }, [selected]);
  return (
    <>
      <div className="table-scroll">
        <table>
          <thead>
            <tr>
              <th>时间</th>
              <th>领域 / 步骤</th>
              <th>结果</th>
              <th>关联对象</th>
              <th>证据</th>
            </tr>
          </thead>
          <tbody>
            {events.map(event => (
              <tr key={`${event.run_id}:${event.sequence}`}>
                <td className="time-cell">{time(event.at_ms)}</td>
                <td>
                  <strong>{label(event.kind)}</strong>
                  <div className="muted">{label(event.step)}</div>
                </td>
                <td>
                  <Status value={event.result} />
                </td>
                <td>
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
                </td>
                <td>
                  <button
                    className="quiet-button"
                    onClick={() => setSelected(event)}
                  >
                    查看 #
                    {event.sequence}
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {events.length === 0 && <Empty />}
      <dialog
        ref={dialogRef}
        className="evidence-sheet"
        onClose={() => setSelected(undefined)}
      >
        <div className="panel-heading">
          <h2>事件证据</h2>
          <button onClick={() => dialogRef.current?.close()}>关闭</button>
        </div>
        {selected && (
          <>
            <div className="row">
              <Status value={selected.result} />
              <span>
                {label(selected.kind)}
                {" "}
                /
                {label(selected.step)}
              </span>
            </div>
            <p className="muted">
              {time(selected.at_ms)}
              {" "}
              · 阶段耗时
              {" "}
              {duration(record(selected.data).elapsed_ms)}
            </p>
            {selected.truncated && (
              <div className="notice">该事件载荷已截断，不能视为完整记录。</div>
            )}
            <pre>{JSON.stringify(selected, null, 2)}</pre>
          </>
        )}
      </dialog>
    </>
  );
}
