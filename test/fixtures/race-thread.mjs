// Worker-thread entry for race-thread.ts: registers tsx in this thread so the TypeScript
// sources (with their .js import specifiers) load, then runs the real thread code.
import { register } from "tsx/esm/api";

register();
await import("./race-thread.ts");
