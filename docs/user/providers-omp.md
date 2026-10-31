# Oh My Pi

T3 Code connects directly to your [Oh My Pi](https://github.com/can1357/oh-my-pi) installation
through its native RPC UI protocol. It does not use an ACP bridge.

## Set up Oh My Pi

1. Install Oh My Pi 18.14.1 or newer on the machine running the T3 Code server.
   With Bun 1.4.4 or newer, run:

   ```sh
   bun install -g @oh-my-pi/pi-coding-agent
   ```

2. Run `omp` in a terminal and use `/login`, or configure your model provider's API key.
   Select a default model in Oh My Pi.
3. In the web or desktop app, open **Settings → Providers → Add provider → Oh My Pi**.
   Enable it. T3 Code automatically discovers the authenticated models available to your
   installation and keeps the list up to date during provider health checks. The default model
   follows Oh My Pi's configuration; custom models use `provider/model` identifiers.

If `omp` is not on the server's `PATH`, set its **Binary path**. Login and updates happen on the
server machine, even when you connect from a phone or another computer. After setup in the web or
desktop app, the provider is also available for conversations on mobile. Update Oh My Pi through
the installer or package manager you used; T3 Code does not update it automatically.

Oh My Pi retains its own settings, authentication, extensions, and MCP configuration. Its default
settings file is `~/.omp/agent/config.yml`. Provider environment variables and launch arguments
can select a different configuration, including `PI_CODING_AGENT_DIR` for a separate agent
directory. Mark API keys and other secret environment values as sensitive. T3 Code owns the RPC
mode, session selection, and approval mode; launch arguments cannot override those controls.

Automatic model discovery opens a temporary, non-persistent Oh My Pi session and can load its
configured extensions and MCP servers. If you sign in or change credentials outside T3 Code,
the next provider health check discovers the updated models; **Refresh provider status** in
Settings → Providers retries immediately. A discovery failure displays a warning and retains
previously discovered models rather than replacing them with a fabricated catalog.

## Permissions and sessions

The composer permission mode selects Oh My Pi's native approval policy:

- **Supervised** uses `always-ask`: read-only tools proceed; writes and commands ask.
- **Auto-accept edits** uses `write`: reads and writes proceed; commands ask.
- **Full access** uses `yolo`. Explicit Oh My Pi policies can still require approval.

**Auto** is not offered; a thread previously set to it runs as **Supervised**. These are tool
approval policies, not an operating-system sandbox. Trusted extensions and MCP servers retain
their own behavior.

T3 Code supports streamed responses and tool activity, native approvals and questions, Stop,
steering, and resuming its Oh My Pi threads. Forks use portable conversation context rather than
a native session fork. Native conversation rollback, plan cards, historical usage import, and
automatic text-generation helpers are not available for this provider.
