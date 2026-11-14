type Labels = Record<string, string>;

function key(labels: Labels): string {
  return Object.keys(labels)
    .sort()
    .map((k) => `${k}="${escape(labels[k]!)}"`)
    .join(",");
}

function escape(v: string): string {
  return v.replace(/\\/g, "\\\\").replace(/\n/g, "\\n").replace(/"/g, '\\"');
}

interface Metric {
  name: string;
  help: string;
  type: "counter" | "gauge" | "histogram";
  render(): string[];
}

export class Counter implements Metric {
  readonly type = "counter";
  private values = new Map<string, number>();
  constructor(
    readonly name: string,
    readonly help: string,
  ) {}

  inc(labels: Labels = {}, by = 1): void {
    const k = key(labels);
    this.values.set(k, (this.values.get(k) ?? 0) + by);
  }

  get(labels: Labels = {}): number {
    return this.values.get(key(labels)) ?? 0;
  }

  render(): string[] {
    return [...this.values].map(([k, v]) => `${this.name}${k ? `{${k}}` : ""} ${v}`);
  }
}

export class Gauge implements Metric {
  readonly type = "gauge";
  private values = new Map<string, number>();
  constructor(
    readonly name: string,
    readonly help: string,
  ) {}

  set(labels: Labels, value: number): void {
    this.values.set(key(labels), value);
  }

  reset(): void {
    this.values.clear();
  }

  render(): string[] {
    return [...this.values].map(([k, v]) => `${this.name}${k ? `{${k}}` : ""} ${v}`);
  }
}

export class Histogram implements Metric {
  readonly type = "histogram";
  private series = new Map<string, { buckets: number[]; sum: number; count: number }>();
  constructor(
    readonly name: string,
    readonly help: string,
    readonly bounds: number[] = [0.01, 0.05, 0.1, 0.5, 1, 5, 10, 30, 60, 300],
  ) {}

  observe(labels: Labels, value: number): void {
    const k = key(labels);
    let s = this.series.get(k);
    if (!s) {
      s = { buckets: this.bounds.map(() => 0), sum: 0, count: 0 };
      this.series.set(k, s);
    }
    for (let i = 0; i < this.bounds.length; i++) {
      if (value <= this.bounds[i]!) s.buckets[i]!++;
    }
    s.sum += value;
    s.count++;
  }

  render(): string[] {
    const lines: string[] = [];
    for (const [k, s] of this.series) {
      const sep = k ? "," : "";
      this.bounds.forEach((b, i) => lines.push(`${this.name}_bucket{${k}${sep}le="${b}"} ${s.buckets[i]}`));
      lines.push(`${this.name}_bucket{${k}${sep}le="+Inf"} ${s.count}`);
      lines.push(`${this.name}_sum${k ? `{${k}}` : ""} ${s.sum}`);
      lines.push(`${this.name}_count${k ? `{${k}}` : ""} ${s.count}`);
    }
    return lines;
  }
}

export class MetricsRegistry {
  private metrics: Metric[] = [];
  private collectors: (() => Promise<void>)[] = [];

  counter(name: string, help: string): Counter {
    return this.add(new Counter(name, help));
  }

  gauge(name: string, help: string): Gauge {
    return this.add(new Gauge(name, help));
  }

  histogram(name: string, help: string, bounds?: number[]): Histogram {
    return this.add(new Histogram(name, help, bounds));
  }

  /** Run before each scrape to refresh gauges. */
  collect(fn: () => Promise<void>): void {
    this.collectors.push(fn);
  }

  private add<M extends Metric>(m: M): M {
    this.metrics.push(m);
    return m;
  }

  async render(): Promise<string> {
    await Promise.all(this.collectors.map((c) => c()));
    const out: string[] = [];
    for (const m of this.metrics) {
      out.push(`# HELP ${m.name} ${m.help}`, `# TYPE ${m.name} ${m.type}`, ...m.render());
    }
    return out.join("\n") + "\n";
  }
}
