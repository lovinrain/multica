// @vitest-environment node
import { describe, expect, it } from "vitest";
import { resolveBasePath, withBasePath, routeHref } from "./base-path";

describe("web base path", () => {
  it.each([undefined, "", "/", "  "])("defaults %s to root", (raw) => {
    expect(resolveBasePath(raw)).toBe("");
    expect(withBasePath("/login", resolveBasePath(raw))).toBe("/login");
  });
  it("normalizes a nested mount", () => {
    expect(resolveBasePath(" /team/multica/ ")).toBe("/team/multica");
  });
  it.each(["multica", "//host", "/bad?query", "/../app", "https://host/app"])("rejects %s", (raw) => {
    expect(() => resolveBasePath(raw)).toThrow("NEXT_PUBLIC_BASE_PATH");
  });
  it.each([
    ["/login", "/multica/login"],
    ["/acme/issues?x=1#c", "/multica/acme/issues?x=1#c"],
    ["/multica/login", "/multica/login"],
    ["/multica?x=1", "/multica?x=1"],
    ["/multica-other", "/multica/multica-other"],
    ["https://example.com/a", "https://example.com/a"],
    ["//cdn.example/a", "//cdn.example/a"],
    ["#comment", "#comment"],
  ])("prefixes %s once", (path, expected) => {
    expect(withBasePath(path, "/multica")).toBe(expected);
  });
});

it("prefixes logical workspace routes even when the slug matches the mount", () => {
  expect(routeHref("/multica/issues/MUL-1", "/multica")).toBe("/multica/multica/issues/MUL-1");
  expect(routeHref("/multica/issues/MUL-1", "")).toBe("/multica/issues/MUL-1");
});
