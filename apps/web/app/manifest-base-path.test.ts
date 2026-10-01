// @vitest-environment node
import { afterEach, expect, it, vi } from "vitest";

afterEach(() => {
  vi.unstubAllEnvs();
  vi.resetModules();
});

it("keeps the installed PWA, shortcuts, and icons within its mount", async () => {
  vi.stubEnv("NEXT_PUBLIC_BASE_PATH", "/multica");
  vi.resetModules();
  const { default: manifest } = await import("./manifest");
  const result = manifest();
  expect(result.start_url).toBe("/multica/inbox");
  expect(result.scope).toBe("/multica/");
  expect(result.id).toBe("/multica/");
  for (const icon of result.icons ?? []) expect(icon.src).toMatch(/^\/multica\/icons\//);
  for (const shortcut of result.shortcuts ?? []) expect(shortcut.url).toMatch(/^\/multica\//);
});
