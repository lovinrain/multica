/** Next.js basePath is a build-time setting; keep browser URLs on that mount. */
export function resolveBasePath(raw: string | undefined): string {
  const value = raw?.trim().replace(/\/+$/, "") ?? "";
  if (!value) return "";
  if (!/^\/(?:[A-Za-z0-9_-]+)(?:\/[A-Za-z0-9_-]+)*$/.test(value)) {
    throw new Error("NEXT_PUBLIC_BASE_PATH must be an absolute path such as /multica");
  }
  return value;
}

export const WEB_BASE_PATH = resolveBasePath(process.env.NEXT_PUBLIC_BASE_PATH);

/** Prefix local root-relative URLs once; leave external and fragment URLs alone. */
export function withBasePath(path: string, basePath = WEB_BASE_PATH): string {
  if (!basePath || !path.startsWith("/") || path.startsWith("//")) return path;
  const pathname = path.split(/[?#]/)[0];
  if (pathname === basePath || pathname?.startsWith(`${basePath}/`)) return path;
  return `${basePath}${path}`;
}

/** Router paths are logical: a workspace may have the same name as the mount. */
export function routeHref(path: string, basePath = WEB_BASE_PATH): string {
  return path.startsWith("/") && !path.startsWith("//") ? `${basePath}${path}` : path;
}
