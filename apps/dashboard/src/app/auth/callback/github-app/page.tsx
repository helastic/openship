"use client";

import { useEffect, useState } from "react";
import { getApiOrigin } from "@/lib/api/urls";
import { GITHUB_CONNECT_ERROR_KEY } from "@/lib/github-connect-error";

/** GitHub App Setup URL landing page for an operator-owned self-hosted App. */
export default function GitHubAppSetupCallback() {
  const [message, setMessage] = useState("Verifying the GitHub App installation…");

  useEffect(() => {
    async function claim() {
      const query = new URLSearchParams(window.location.search);
      const flow = query.get("flow");
      const code = query.get("code");
      const installationId = query.get("installation_id");
      const state = query.get("state");
      const setupAction = query.get("setup_action");

      // GitHub owns this URL's query string, and it will not carry a `flow` of
      // ours: the manifest `redirect_url` must be bare (a query string there is
      // rejected outright), so state travels on the registration url instead and
      // comes back with `code`. Tell the two landings apart by what GitHub sent —
      // `code` is the manifest conversion, `installation_id` is the install. The
      // explicit `flow` is still honoured for a link issued before this change.
      const isManifest = flow === "manifest" || (!!code && !installationId);

      if (!state || (isManifest ? !code : !installationId)) {
        setMessage(
          "GitHub did not return the required installation details. Start again from Settings.",
        );
        return;
      }

      try {
        const base = getApiOrigin(window.location.origin);
        const endpoint = isManifest
          ? `${base}/api/github/sources/manifest/convert`
          : `${base}/api/github/installations/claim`;
        const response = await fetch(endpoint, {
          method: "POST",
          credentials: "include",
          headers: { "content-type": "application/json" },
          body: JSON.stringify(
            isManifest ? { code, state } : { installationId, state, setupAction },
          ),
        });
        const data = await response.json().catch(() => ({}));
        if (!response.ok) throw new Error(data?.message || "Could not verify the installation.");

        if (isManifest) {
          if (!data?.installUrl)
            throw new Error("GitHub App was created, but its install URL is missing.");
          setMessage("GitHub App created. Opening repository access…");
          window.location.replace(data.installUrl);
          return;
        }

        if (data?.pendingApproval) {
          setMessage("Installation requested. A GitHub organization owner must approve it.");
          return;
        }
        setMessage(
          `${data?.installation?.login || "GitHub"} is connected. You can close this window.`,
        );
        window.setTimeout(() => window.close(), 1200);
      } catch (error) {
        const detail =
          error instanceof Error ? error.message : "Could not verify the installation.";
        try {
          localStorage.setItem(GITHUB_CONNECT_ERROR_KEY, detail);
        } catch {
          /* unavailable */
        }
        setMessage(detail);
      }
    }
    void claim();
  }, []);

  return (
    <div className="flex h-screen items-center justify-center bg-background px-6 text-foreground">
      <p className="max-w-md text-center text-sm text-muted-foreground">{message}</p>
    </div>
  );
}
