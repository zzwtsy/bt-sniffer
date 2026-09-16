import type { pieces } from "./model";
import { Empty } from "@/components/observation/common";
import { count } from "@/lib/observation/format";
import { bucketPieces } from "./model";

const stateText: Record<string, string> = {
  received: "有效",
  requested: "已请求",
  pending: "未收到",
  invalid: "错误",
  unknown: "未知",
};
/** 连续分片进度条；超过 256 片聚合，桶内多状态按优先级取代表。 */
export function PieceBar({ model }: { model: ReturnType<typeof pieces> }) {
  return (
    <div className="piece-section">
      <div className="mb-4 flex items-start justify-between gap-3">
        <h3>本 peer 的分片</h3>
        <span>
          有效
          {count(model.received)}
          {" "}
          /
          {count(model.total)}
        </span>
      </div>
      <p className="muted">
        当前保留的重复接收
        {model.duplicates}
        {" "}
        次；空白表示状态未知，不能算作尚未请求。
      </p>
      {model.total !== undefined && model.total > 0
        ? (
            <>
              <div
                className="piece-bar"
                role="img"
                aria-label={`分片进度：有效 ${count(model.received)} / ${count(model.total)}`}
              >
                {bucketPieces(model.states, model.total).map(b => (
                  <span
                    key={b.start}
                    aria-hidden="true"
                    className={`piece-segment ${b.state}`}
                    title={
                      b.start === b.end
                        ? `分片 ${b.start}：${stateText[b.state] ?? b.state}`
                        : `分片 ${b.start}–${b.end}：${stateText[b.state] ?? b.state}`
                    }
                  />
                ))}
              </div>
              <div className="piece-legend" aria-hidden="true">
                {Object.entries(stateText).map(([state, text]) => (
                  <span key={state}>
                    <i className={`piece-segment ${state}`} />
                    {text}
                  </span>
                ))}
              </div>
              {model.total > 256 && (
                <p className="muted">
                  共
                  {count(model.total)}
                  {" "}
                  片，聚合为 256 段显示，每段约
                  {Math.ceil(model.total / 256)}
                  {" "}
                  片；段内多状态按 错误 &gt; 有效 &gt; 已请求 &gt; 未收到 &gt; 未知 取代表。
                </p>
              )}
            </>
          )
        : (
            <Empty>分片总数未知，等待保留事件或当前状态。</Empty>
          )}
    </div>
  );
}
