import { ProviderDriverKind } from "@t3tools/contracts";
import { defineProviderClient } from "@t3tools/provider-core/client";

import { OmpSettings } from "./settings.ts";

export const ompClient = defineProviderClient({
  driverKind: ProviderDriverKind.make("omp"),
  label: "Oh My Pi",
  settingsSchema: OmpSettings,
  // Official OMP favicon, adapted to the shared path-only icon format.
  // https://github.com/can1357/oh-my-pi/blob/b7bac3d53e8907531bacbe1aeb673f0ffc6b804e/packages/collab-web/public/favicon.svg
  icon: {
    viewBox: "0 0 64 64",
    fill: { light: "#0f0a14", dark: "#0f0a14" },
    paths: [
      {
        d: "M12 0H52A12 12 0 0 1 64 12V52A12 12 0 0 1 52 64H12A12 12 0 0 1 0 52V12A12 12 0 0 1 12 0Z",
      },
      {
        d: "M14 16h36v8H40v32h-8V24h-6v22h-8V24h-4z",
        gradient: {
          // Equivalent to upstream's diagonal objectBoundingBox gradient.
          x1: 14,
          y1: 16,
          x2: 53.77900552486188,
          y2: 51.80110497237569,
          stops: [
            { offset: 0, color: "#ed4abf" },
            { offset: 0.5, color: "#9b4dff" },
            { offset: 1, color: "#5ad8e6" },
          ],
        },
      },
    ],
  },
});
