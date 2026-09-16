import { expect, it } from "vitest";
import { event } from "../../../tests/fixtures";
import { bucketPieces, generations, peerEvents, pieces, spans } from "./model";

it("generation 与 peer 独立，重复分片不增加有效接收", () => {
  const one = {
    ...event(1, { piece: 0, total_pieces: 2, received_pieces: 1 }),
    context: { generation: 2, peer_attempt_id: "p1" },
  };
  const duplicate = { ...one, sequence: "2", result: "duplicate" };
  const next = {
    ...event(3, { piece: 1, total_pieces: 2, received_pieces: 1 }),
    context: { generation: 3, peer_attempt_id: "p2" },
  };
  expect(generations([one, duplicate, next])).toEqual([3, 2]);
  const model = pieces(peerEvents([one, duplicate, next], 2, "p1"));
  expect(model.received).toBe(1);
  expect(model.duplicates).toBe(1);
  expect(model.states.get(1)).toBeUndefined();
  expect(pieces([next]).states.get(0)).toBeUndefined();
});
it("阶段只匹配真实起点与后端耗时，不为 Stale 生成成功", () => {
  const first = {
    ...event(1),
    kind: "commit",
    result: "started",
    context: { span_id: "a" },
  };
  const last = {
    ...event(2, { elapsed_ms: 123 }),
    kind: "commit",
    result: "stale",
    context: { span_id: "a" },
  };
  expect(spans([first, last])[0]).toMatchObject({
    elapsed: 123,
    result: "stale",
  });
  expect(spans([last])).toEqual([]);
});
it("分片聚合按优先级取代表状态，桶边界不越界", () => {
  const states = new Map<number, string>([[1, "invalid"], [2, "received"]]);
  const buckets = bucketPieces(states, 512, 256);
  expect(buckets).toHaveLength(256);
  expect(buckets[0]).toEqual({ start: 0, end: 1, state: "invalid" });
  expect(buckets[1]).toEqual({ start: 2, end: 3, state: "received" });
  expect(buckets[255]).toEqual({ start: 510, end: 511, state: "unknown" });
  expect(bucketPieces(new Map([[0, "requested"]]), 2, 256)).toEqual([
    { start: 0, end: 0, state: "requested" },
    { start: 1, end: 1, state: "unknown" },
  ]);
});
