import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    include: ["test/**/*.test.ts"],
    environment: "node",
    testTimeout: 10000,
    coverage: {
      provider: "v8",
      include: ["src/**/*.ts"],
      // index.ts only re-exports; it has no behaviour of its own to test.
      exclude: ["src/index.ts"],
      reporter: ["text", "html", "lcov", "json-summary"],
      reportsDirectory: "coverage",
      // Enforced by `npm run test:coverage` (and CI): the run fails below this.
      thresholds: {
        lines: 85,
      },
    },
  },
});
