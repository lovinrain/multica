"use client";

import { useMemo } from "react";
import { useOptionalNavigation } from "./context";

/**
 * Public base URL of this deployment, or `null` when the platform can't
 * name one (server render, desktop before runtime config lands, or a component
 * mounted outside a NavigationProvider).
 *
 * Derived from the adapter's `getShareableUrl` rather than a separate adapter
 * field: "the public URL of this app" is already implemented per platform there
 * (web: the current origin and mount path, desktop: the connected environment's app URL), and a
 * second copy of the same fact is a copy that can drift.
 *
 * Used to tell an in-app destination written as an absolute URL
 * (`https://<app-host>/acme/issues/1`) from a genuinely external link. `null`
 * degrades to the old behaviour — every absolute URL reads as external — which
 * is why a missing provider is tolerated instead of thrown on.
 */
export function useAppOrigin(): string | null {
  const navigation = useOptionalNavigation();
  const getShareableUrl = navigation?.getShareableUrl;
  return useMemo(() => {
    if (!getShareableUrl) return null;
    try {
      return new URL(getShareableUrl("/")).toString();
    } catch {
      return null;
    }
  }, [getShareableUrl]);
}
