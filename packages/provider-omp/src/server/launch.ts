import { tokenizeCliArgs } from "@t3tools/shared/cliArgs";
import type { RuntimeMode } from "@t3tools/contracts";

const VALUE_ARGUMENTS = new Set([
  "--profile",
  "--provider",
  "--model",
  "--models",
  "--thinking",
  "--api-key",
  "--extension",
  "-e",
  "--trusted-extension",
  "--plugin-dir",
  "--hook",
  "--config",
  "--tools",
  "--skills",
  "--add-dir",
  "--max-cost",
  "--warn-cost",
  "--max-time",
  "--service-tier",
  "--system-prompt",
  "--system-prompt-template",
  "--append-system-prompt",
]);
const BOOLEAN_ARGUMENTS = new Set([
  "--no-tools",
  "--no-lsp",
  "--no-skills",
  "--no-rules",
  "--no-context-files",
  "--no-extensions",
  "--allow-home",
  "--no-prewalk",
]);
const EXTENSION_ARGUMENTS = new Set([
  "--extension",
  "-e",
  "--trusted-extension",
  "--plugin-dir",
  "--hook",
]);

export type OmpLaunchArgsResolution =
  | { readonly ok: true; readonly args: ReadonlyArray<string> }
  | { readonly ok: false; readonly message: string };

export function resolveOmpLaunchArgs(launchArgs: string): OmpLaunchArgsResolution {
  const tokens = tokenizeCliArgs(launchArgs);
  const args: string[] = [];
  for (let index = 0; index < tokens.length; index += 1) {
    const token = tokens[index]!;
    const equalsIndex = token.indexOf("=");
    const option = equalsIndex < 0 ? token : token.slice(0, equalsIndex);
    if (VALUE_ARGUMENTS.has(option)) {
      const value = equalsIndex < 0 ? tokens[++index] : token.slice(equalsIndex + 1);
      if (value === undefined) {
        return { ok: false, message: `Oh My Pi launch argument '${option}' requires a value.` };
      }
      args.push(option, value);
    } else if (BOOLEAN_ARGUMENTS.has(option) && equalsIndex < 0) {
      args.push(option);
    } else {
      return {
        ok: false,
        message:
          "Unsupported Oh My Pi launch argument. T3 Code controls the protocol, permissions, session, and prompts.",
      };
    }
  }
  return { ok: true, args };
}

export function buildOmpRpcLaunch(input: {
  readonly launchArgs: ReadonlyArray<string>;
  readonly environment: NodeJS.ProcessEnv;
  readonly runtimeMode?: RuntimeMode;
  readonly sessionPath?: string;
  readonly ephemeral?: boolean;
  readonly disableTools?: boolean;
  readonly disableExtensions?: boolean;
}): { readonly args: ReadonlyArray<string>; readonly env: NodeJS.ProcessEnv } {
  const launchArgs: string[] = [];
  for (let index = 0; index < input.launchArgs.length; index += 1) {
    const option = input.launchArgs[index]!;
    if (
      (input.disableExtensions === true && EXTENSION_ARGUMENTS.has(option)) ||
      (input.disableTools === true && option === "--tools")
    ) {
      index += 1;
      continue;
    }
    launchArgs.push(option);
    if (VALUE_ARGUMENTS.has(option)) launchArgs.push(input.launchArgs[++index]!);
  }
  const approvalMode =
    input.runtimeMode === "full-access"
      ? "yolo"
      : input.runtimeMode === "auto-accept-edits"
        ? "write"
        : "always-ask";
  return {
    args: [
      ...launchArgs,
      "--mode",
      "rpc-ui",
      "--approval-mode",
      approvalMode,
      "--no-title",
      "--no-pty",
      ...(input.ephemeral === true
        ? ["--no-session"]
        : input.sessionPath === undefined
          ? []
          : ["--session", input.sessionPath]),
      ...(input.disableTools === true ? ["--no-tools"] : []),
      ...(input.disableExtensions === true ? ["--no-extensions"] : []),
    ],
    env: { ...input.environment },
  };
}
