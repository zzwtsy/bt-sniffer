import type { Snapshot } from "./contracts";
import { number, record, rows, source } from "./contracts";

export interface TrendPoint {
  at: number;
  commits: number | null;
}
export class Trends {
  points: TrendPoint[] = [];
  private previous?: { run: string; at: number; value: number };
  gap() {
    this.previous = undefined;
  }

  clear() {
    this.points = [];
    this.gap();
  }

  prune(now = Date.now()) {
    const before = this.points.length;
    if ((this.points.at(0)?.at ?? Infinity) < now - 900_000)
      this.points = this.points.filter(point => point.at >= now - 900_000);
    return this.points.length !== before;
  }

  add(snapshot: Snapshot) {
    const at = snapshot.runtime.sources.collector?.observed_at_ms;
    const metric = rows(
      record(source(snapshot, "collector").metrics).counters,
    ).find(row => row.counter === "metadata_committed");
    const value = number(metric?.value);
    if (at === undefined || value === undefined) {
      this.gap();
      return;
    }
    if (this.points.at(-1)?.at === at)
      return;
    const previous = this.previous;
    const valid
      = previous
        && previous.run === snapshot.window.run_id
        && at > previous.at
        && at - previous.at <= 5000
        && value >= previous.value;
    const point = {
      at,
      commits: valid
        ? ((value - previous.value) * 1000) / (at - previous.at)
        : null,
    };
    this.previous = { run: snapshot.window.run_id, at, value };
    if (
      this.points.at(-1)
      && Math.floor(this.points.at(-1)!.at / 1000) === Math.floor(at / 1000)
    ) {
      this.points = [...this.points.slice(0, -1), point];
    } else {
      this.points = [...this.points, point];
    }
    this.points = this.points.filter(p => p.at >= at - 900_000).slice(-900);
  }
}
