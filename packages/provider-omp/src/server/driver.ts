import { ProviderDriverKind, TextGenerationError } from "@t3tools/contracts";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import * as ProviderHost from "@t3tools/provider-core/server/ProviderHost";
import { ProviderDriverError } from "@t3tools/provider-core/server/errors";
import {
  defaultProviderContinuationIdentity,
  type ProviderDriver,
  type ProviderInstance,
} from "@t3tools/provider-core/server/driver";
import { withInstanceIdentity } from "@t3tools/provider-core/server/instanceIdentity";
import { mergeProviderInstanceEnvironment } from "@t3tools/provider-core/server/instanceEnvironment";
import { makeManagedServerProvider } from "@t3tools/provider-core/server/managedProvider";
import { makeManualOnlyProviderMaintenanceCapabilities } from "@t3tools/provider-core/server/maintenanceResolver";
import {
  haveProviderSnapshotSettingsChanged,
  makeProviderSnapshotSettingsSource,
} from "@t3tools/provider-core/server/snapshotSettings";
import type { ProviderTextGeneration } from "@t3tools/provider-core/server/textGeneration";
import { OmpSettings } from "../settings.ts";
import { OmpAdapterV2Driver, type OmpAdapterV2DriverEnv } from "./adapter.ts";
import { buildInitialOmpProviderSnapshot, checkOmpProviderStatus } from "./status.ts";

const decodeOmpSettings = Schema.decodeSync(OmpSettings);
const DRIVER = ProviderDriverKind.make("omp");
const MAINTENANCE = makeManualOnlyProviderMaintenanceCapabilities({
  provider: DRIVER,
  packageName: null,
});
const unsupported = (operation: string) =>
  Effect.fail(
    new TextGenerationError({
      operation,
      detail: "Oh My Pi does not provide application text generation in T3 Code.",
    }),
  );
const textGeneration: ProviderTextGeneration = {
  generateCommitMessage: () => unsupported("generateCommitMessage"),
  generatePrContent: () => unsupported("generatePrContent"),
  generateBranchName: () => unsupported("generateBranchName"),
  generateThreadTitle: () => unsupported("generateThreadTitle"),
  generateReviewGuide: () => unsupported("generateReviewGuide"),
};

export type OmpDriverEnv =
  | OmpAdapterV2DriverEnv
  | ChildProcessSpawner.ChildProcessSpawner
  | ProviderHost.ProviderHost;

export const OmpDriver: ProviderDriver<OmpSettings, OmpDriverEnv> = {
  driverKind: DRIVER,
  metadata: { displayName: "Oh My Pi", supportsMultipleInstances: true },
  configSchema: OmpSettings,
  defaultConfig: () => decodeOmpSettings({}),
  create: Effect.fn("OmpDriver.create")(function* (input) {
    const { instanceId, displayName, accentColor, enabled } = input;
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const host = yield* ProviderHost.ProviderHost;
    const environment = yield* mergeProviderInstanceEnvironment(
      input.environment,
      yield* HostProcess.Environment,
    );
    const config = { ...input.config, enabled };
    const continuationIdentity = defaultProviderContinuationIdentity({
      driverKind: DRIVER,
      instanceId,
    });
    const stampIdentity = withInstanceIdentity({
      instanceId,
      driverKind: DRIVER,
      displayName,
      accentColor,
      continuationGroupKey: continuationIdentity.continuationKey,
    });
    const settings = yield* makeProviderSnapshotSettingsSource(config);
    const snapshot = yield* makeManagedServerProvider({
      resolveMaintenance: () => Effect.succeed(MAINTENANCE),
      getSettings: settings.getSettings,
      streamSettings: settings.streamSettings,
      haveSettingsChanged: haveProviderSnapshotSettingsChanged,
      initialSnapshot: (current) =>
        buildInitialOmpProviderSnapshot(current.provider).pipe(Effect.map(stampIdentity)),
      checkProvider: checkOmpProviderStatus(config, environment, host.paths.cwd).pipe(
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
        Effect.map(stampIdentity),
      ),
    }).pipe(
      Effect.mapError(
        (cause) =>
          new ProviderDriverError({
            driver: DRIVER,
            instanceId,
            detail: "Failed to build Oh My Pi status.",
            cause,
          }),
      ),
    );
    const orchestrationAdapter = yield* OmpAdapterV2Driver.create(input).pipe(
      Effect.mapError(
        (cause) =>
          new ProviderDriverError({
            driver: DRIVER,
            instanceId,
            detail: "Failed to build Oh My Pi adapter.",
            cause,
          }),
      ),
    );
    return {
      instanceId,
      driverKind: DRIVER,
      continuationIdentity,
      displayName,
      accentColor,
      enabled,
      snapshot,
      orchestrationAdapter,
      textGeneration,
    } satisfies ProviderInstance;
  }),
};
