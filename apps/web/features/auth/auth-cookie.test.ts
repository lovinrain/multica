import { afterEach, expect, it, vi } from "vitest";

afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
  vi.resetModules();
});

it.each([["", "/"], ["/multica", "/multica"]])("sets and clears auth markers at the same mount %s", async (mount, path) => {
  vi.stubEnv("NEXT_PUBLIC_BASE_PATH", mount);
  vi.resetModules();
  const setter = vi.spyOn(document, "cookie", "set");
  const { setLoggedInCookie, clearLoggedInCookie } = await import("./auth-cookie");
  setLoggedInCookie();
  clearLoggedInCookie();
  expect(setter).toHaveBeenNthCalledWith(1, `multica_logged_in=1; path=${path}; max-age=31536000; samesite=lax`);
  expect(setter).toHaveBeenNthCalledWith(2, `multica_logged_in=; path=${path}; max-age=0`);
});
