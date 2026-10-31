import {
  ProviderDriverKind,
  type ServerProviderModel,
  type ServerProviderSlashCommand,
} from "@t3tools/contracts";
import { createModelCapabilities } from "@t3tools/shared/model";
import { resolveSpawnCommand } from "@t3tools/shared/shell";
import { compareSemverVersions } from "@t3tools/shared/semver";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { buildOmpRpcLaunch, resolveOmpLaunchArgs } from "./launch.ts";
import { makeOmpRpcConnection, OmpRpcError } from "./rpc.ts";
import * as ChildProcess from "effect/process/ChildProcess";
import {
  AUTH_PROBE_TIMEOUT_MS,
  buildServerProvider,
  isCommandMissingCause,
  parseGenericCliVersion,
  providerModelsFromSettings,
  spawnAndCollect,
  type ProviderProbeResult,
} from "@t3tools/provider-core/server/snapshotProbe";
import type { OmpSettings } from "../settings.ts";

const MINIMUM_OMP_VERSION = "18.14.1";
const EMPTY_CAPABILITIES = createModelCapabilities({ optionDescriptors: [] });
const PRESENTATION = {
  displayName: "Oh My Pi",
  showInteractionModeToggle: false,
  reportsContextWindow: true,
  supportsConversationRollback: false,
  supportedRuntimeModes: ["approval-required", "auto-accept-edits", "full-access"],
} as const;

interface OmpCatalog {
  readonly models: ReadonlyArray<ServerProviderModel>;
  readonly slashCommands: ReadonlyArray<ServerProviderSlashCommand>;
}

const ModelInventory = Schema.Struct({
  models: Schema.Array(
    Schema.Struct({
      provider: Schema.NonEmptyString,
      id: Schema.NonEmptyString,
      name: Schema.optional(Schema.String),
    }),
  ),
});
const Commands = Schema.Struct({
  commands: Schema.Array(
    Schema.Struct({
      name: Schema.NonEmptyString,
      source: Schema.String,
      description: Schema.optional(Schema.String),
      input: Schema.optional(Schema.Struct({ hint: Schema.optional(Schema.String) })),
    }),
  ),
});

const decodeModelInventory = Schema.decodeUnknownSync(ModelInventory);
const decodeCommands = Schema.decodeUnknownSync(Commands);

export function parseOmpCatalog(models: unknown, commands: unknown): OmpCatalog {
  const inventory = decodeModelInventory(models);
  const available = decodeCommands(commands);
  const seen = new Set<string>();
  return {
    models: inventory.models.flatMap((model): ServerProviderModel[] => {
      const slug = `${model.provider}/${model.id}`;
      if (seen.has(slug)) return [];
      seen.add(slug);
      return [
        {
          slug,
          name: model.name ?? slug,
          subProvider: model.provider,
          isCustom: false,
          capabilities: EMPTY_CAPABILITIES,
        },
      ];
    }),
    // Provider discovery has server cwd, not the viewing project's command scope.
    slashCommands: available.commands
      .filter((command) => command.source === "builtin")
      .map(({ name, description, input }) => ({
        name,
        ...(description === undefined ? {} : { description }),
        ...(input?.hint === undefined ? {} : { input: { hint: input.hint } }),
      })),
  };
}

function snapshot(
  settings: OmpSettings,
  checkedAt: string,
  probe: ProviderProbeResult,
  catalog?: OmpCatalog,
) {
  return {
    ...buildServerProvider({
      driver: ProviderDriverKind.make("omp"),
      presentation: PRESENTATION,
      enabled: settings.enabled,
      checkedAt,
      models: providerModelsFromSettings(
        [
          {
            slug: "default",
            name: "Oh My Pi default",
            isCustom: false,
            capabilities: EMPTY_CAPABILITIES,
          },
          ...(catalog?.models ?? []),
        ],
        settings.customModels,
        EMPTY_CAPABILITIES,
      ),
      slashCommands: catalog?.slashCommands ?? [],
      probe,
    }),
    supportsTextGeneration: false,
  };
}

export const buildInitialOmpProviderSnapshot = Effect.fn("buildInitialOmpProviderSnapshot")(
  function* (settings: OmpSettings) {
    return snapshot(settings, DateTime.formatIso(yield* DateTime.now), {
      installed: settings.enabled,
      version: null,
      status: "warning",
      auth: { status: "unknown" },
      message: settings.enabled
        ? "Checking Oh My Pi CLI availability..."
        : "Oh My Pi is disabled in T3 Code settings.",
    });
  },
);

export const checkOmpProviderStatus = Effect.fn("checkOmpProviderStatus")(function* (
  settings: OmpSettings,
  environment: NodeJS.ProcessEnv,
  cwd?: string,
) {
  const checkedAt = DateTime.formatIso(yield* DateTime.now);
  const finish = (probe: ProviderProbeResult, catalog?: OmpCatalog) =>
    snapshot(settings, checkedAt, probe, catalog);
  if (!settings.enabled)
    return finish({
      installed: false,
      version: null,
      status: "warning",
      auth: { status: "unknown" },
      message: "Oh My Pi is disabled in T3 Code settings.",
    });
  const launchArgs = resolveOmpLaunchArgs(settings.launchArgs);
  if (!launchArgs.ok)
    return finish({
      installed: true,
      version: null,
      status: "error",
      auth: { status: "unknown" },
      message: launchArgs.message,
    });
  const command = settings.binaryPath || "omp";
  // Version exits before OMP loads extensions, MCP servers or a session.
  const result = yield* resolveSpawnCommand(command, ["--version"], { env: environment }).pipe(
    Effect.flatMap((spawn) =>
      spawnAndCollect(
        command,
        ChildProcess.make(spawn.command, spawn.args, {
          env: environment,
          extendEnv: false,
          shell: spawn.shell,
        }),
      ),
    ),
    Effect.timeoutOption(4_000),
    Effect.result,
  );
  if (Result.isFailure(result))
    return finish({
      installed: !isCommandMissingCause(result.failure),
      version: null,
      status: "error",
      auth: { status: "unknown" },
      message: isCommandMissingCause(result.failure)
        ? "Oh My Pi CLI (`omp`) is not installed or not on PATH. Install Oh My Pi and configure its binary path."
        : "Failed to execute the Oh My Pi version check.",
    });
  if (Option.isNone(result.success))
    return finish({
      installed: true,
      version: null,
      status: "error",
      auth: { status: "unknown" },
      message: "Oh My Pi timed out while running omp --version.",
    });
  const output = result.success.value;
  const version = parseGenericCliVersion(`${output.stdout}\n${output.stderr}`);
  if (
    output.code !== 0 ||
    version === null ||
    compareSemverVersions(version, MINIMUM_OMP_VERSION) < 0
  )
    return finish({
      installed: true,
      version,
      status: "error",
      auth: { status: "unknown" },
      message: `Oh My Pi ${MINIMUM_OMP_VERSION} or newer is required. Update Oh My Pi using its installer.`,
    });
  // The managed status check runs at startup, on settings changes and on the
  // existing health-check cadence, so the selector never needs a manual refresh.
  const discovery = yield* discoverOmpCatalog(settings, environment, cwd).pipe(
    Effect.timeoutOption(AUTH_PROBE_TIMEOUT_MS),
    Effect.result,
  );
  if (Result.isFailure(discovery) || Option.isNone(discovery.success))
    return finish({
      installed: true,
      version,
      status: "warning",
      auth: { status: "unknown", type: "omp" },
      message:
        "Could not discover Oh My Pi models. Sign in or configure an API key and select a default model using omp in a terminal. T3 Code will retry automatically; Refresh provider status retries now.",
    });
  const catalog = discovery.success.value;
  return finish(
    {
      installed: true,
      version,
      status: catalog.models.length === 0 ? "warning" : "ready",
      auth: {
        status: catalog.models.length > 0 ? "authenticated" : "unauthenticated",
        type: "omp",
      },
      ...(catalog.models.length === 0
        ? {
            message:
              "Oh My Pi has no available models. Sign in or configure an API key and select a default model using omp in a terminal. T3 Code will retry automatically; Refresh provider status retries now.",
          }
        : {}),
    },
    catalog,
  );
});

const discoverOmpCatalog = Effect.fn("discoverOmpCatalog")(function* (
  settings: OmpSettings,
  environment: NodeJS.ProcessEnv,
  cwd?: string,
) {
  const resolved = resolveOmpLaunchArgs(settings.launchArgs);
  if (!resolved.ok)
    return yield* new OmpRpcError({ operation: "launch", detail: resolved.message });
  const launch = buildOmpRpcLaunch({
    launchArgs: resolved.args,
    environment,
    ephemeral: true,
    disableTools: true,
  });
  const connection = yield* makeOmpRpcConnection({
    command: settings.binaryPath || "omp",
    args: launch.args,
    cwd,
    env: launch.env,
  });
  yield* Stream.fromQueue(connection.events).pipe(
    Stream.runForEach((event) =>
      event["type"] === "extension_ui_request" && typeof event["id"] === "string"
        ? connection.send({ type: "extension_ui_response", id: event["id"], cancelled: true })
        : Effect.void,
    ),
    Effect.ignore,
    Effect.forkScoped,
  );
  yield* connection.ready;
  const models = yield* connection.request({ type: "get_available_models" });
  const commands = yield* connection.request({ type: "get_available_commands" });
  return yield* Effect.try({
    try: () => parseOmpCatalog(models, commands),
    catch: (cause) => new OmpRpcError({ operation: "discover", cause }),
  });
}, Effect.scoped);
