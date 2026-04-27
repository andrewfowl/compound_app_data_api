import { defineConfig } from "@trigger.dev/sdk";

const project = process.env.TRIGGER_PROJECT_REF || "proj_blqgffiwaijtkcoyhzcr";

if (!project) {
  throw new Error("Missing TRIGGER_PROJECT_REF");
}

export default defineConfig({
  project,
  runtime: "node-22",
  dirs: ["./trigger"],
  maxDuration: 3600,
  machine: "medium-1x",
});
