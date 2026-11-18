import { describe, expect, it } from "vitest";
import { Counter, Gauge, Histogram, MetricsRegistry } from "../../src/metrics/registry.js";

describe("Counter", () => {
  it("counts per label set, with sorted label keys", () => {
    const c = new Counter("jobs_total", "Jobs");
    c.inc();
    c.inc({ queue: "a" });
    c.inc({ state: "x", queue: "a" }, 3);
    c.inc({ queue: "a", state: "x" });
    expect(c.get()).toBe(1);
    expect(c.get({ queue: "a" })).toBe(1);
    expect(c.get({ queue: "a", state: "x" })).toBe(4);
    expect(c.get({ queue: "missing" })).toBe(0);
    expect(c.render()).toEqual(["jobs_total 1", 'jobs_total{queue="a"} 1', 'jobs_total{queue="a",state="x"} 4']);
  });

  it("escapes backslashes, quotes and newlines in label values", () => {
    const c = new Counter("m", "h");
    c.inc({ queue: 'a"b\\c\nd' });
    expect(c.render()).toEqual(['m{queue="a\\"b\\\\c\\nd"} 1']);
  });
});

describe("Gauge", () => {
  it("sets values and can be reset", () => {
    const g = new Gauge("depth", "Depth");
    g.set({ queue: "a" }, 5);
    g.set({ queue: "a" }, 2);
    g.set({}, 7);
    expect(g.render()).toEqual(['depth{queue="a"} 2', "depth 7"]);
    g.reset();
    expect(g.render()).toEqual([]);
  });
});

describe("Histogram", () => {
  it("renders cumulative buckets, +Inf, sum and count", () => {
    const h = new Histogram("dur", "Duration", [0.1, 1, 10]);
    for (const v of [0.05, 0.1, 0.5, 5, 50]) h.observe({}, v);
    expect(h.render()).toEqual([
      'dur_bucket{le="0.1"} 2',
      'dur_bucket{le="1"} 3',
      'dur_bucket{le="10"} 4',
      'dur_bucket{le="+Inf"} 5',
      "dur_sum 55.65",
      "dur_count 5",
    ]);
  });

  it("adds le after the other labels", () => {
    const h = new Histogram("dur", "Duration", [1]);
    h.observe({ queue: "q" }, 2);
    expect(h.render()).toEqual([
      'dur_bucket{queue="q",le="1"} 0',
      'dur_bucket{queue="q",le="+Inf"} 1',
      'dur_sum{queue="q"} 2',
      'dur_count{queue="q"} 1',
    ]);
  });

  it("has sensible default buckets", () => {
    expect(new Histogram("d", "h").bounds).toEqual([0.01, 0.05, 0.1, 0.5, 1, 5, 10, 30, 60, 300]);
  });
});

describe("MetricsRegistry", () => {
  it("renders HELP and TYPE for each metric in registration order, ending with a newline", async () => {
    const reg = new MetricsRegistry();
    reg.counter("a_total", "A things").inc();
    reg.gauge("b", "B level");
    reg.histogram("c", "C time", [1]);
    expect(await reg.render()).toBe(
      ["# HELP a_total A things", "# TYPE a_total counter", "a_total 1", "# HELP b B level", "# TYPE b gauge", "# HELP c C time", "# TYPE c histogram"].join("\n") + "\n",
    );
  });

  it("runs collectors before each render", async () => {
    const reg = new MetricsRegistry();
    const g = reg.gauge("g", "G");
    let n = 0;
    reg.collect(async () => g.set({}, ++n));
    expect(await reg.render()).toContain("\ng 1\n");
    expect(await reg.render()).toContain("\ng 2\n");
  });
});
