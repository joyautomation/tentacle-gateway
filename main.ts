/**
 * tentacle-gateway
 *
 * Config-driven PLC runtime. Reads device/variable configuration from the
 * gateway_config NATS KV bucket and translates it into a tentacle-plc instance.
 *
 * When the config changes (via GUI or GraphQL), the gateway tears down the
 * current PLC instance and rebuilds from the updated config.
 */

import { connect } from "@nats-io/transport-deno";
import { jetstream } from "@nats-io/jetstream";
import { Kvm } from "@nats-io/kv";
import { createLogger, LogLevel, type Log } from "@joyautomation/coral";
import { createPlc, type Plc } from "@tentacle/plc";
import type { GatewayConfigKV, ServiceHeartbeat, ServiceLogEntry } from "@tentacle/nats-schema";
import type { PlcVariableConfig } from "@tentacle/plc";
import type { NatsConnection } from "@nats-io/transport-deno";
import { translateConfig } from "./config.ts";

let log: Log = createLogger("gateway", LogLevel.info);

function createNatsLogger(
  coralLog: Log,
  nc: NatsConnection,
  serviceType: string,
  moduleId: string,
  loggerName: string,
): Log {
  const subject = `service.logs.${serviceType}.${moduleId}`;
  const encoder = new TextEncoder();
  const formatArgs = (args: unknown[]): string =>
    args.map((a) => (typeof a === "string" ? a : JSON.stringify(a))).join(" ");
  const publish = (level: string, msg: string, ...args: unknown[]) => {
    try {
      const message = args.length > 0 ? `${msg} ${formatArgs(args)}` : msg;
      const entry: ServiceLogEntry = {
        timestamp: Date.now(),
        level: level as ServiceLogEntry["level"],
        message,
        serviceType,
        moduleId,
        logger: loggerName,
      };
      nc.publish(subject, encoder.encode(JSON.stringify(entry)));
    } catch { /* never break the service for logging */ }
  };
  return {
    info: (m: string, ...a: unknown[]) => { coralLog.info(m, ...a); publish("info", m, ...a); },
    warn: (m: string, ...a: unknown[]) => { coralLog.warn(m, ...a); publish("warn", m, ...a); },
    error: (m: string, ...a: unknown[]) => { coralLog.error(m, ...a); publish("error", m, ...a); },
    debug: (m: string, ...a: unknown[]) => { coralLog.debug(m, ...a); publish("debug", m, ...a); },
  } as Log;
}

const NATS_URL = Deno.env.get("NATS_URL") ?? "nats://localhost:4222";
const GATEWAY_ID = Deno.env.get("GATEWAY_ID") ?? "gateway";
const KV_BUCKET = "gateway_config";

/** Current running PLC instance — null when no config or between rebuilds */
let currentPlc: Plc<Record<string, PlcVariableConfig>> | null = null;

/** Stop the current PLC instance if running */
async function stopCurrentPlc(): Promise<void> {
  if (currentPlc) {
    log.info("Stopping current PLC instance...");
    await currentPlc.stop();
    currentPlc = null;
  }
}

/** Build and start a PLC from gateway config */
async function startPlc(
  config: GatewayConfigKV,
  natsUrl: string,
): Promise<void> {
  const atomicCount = Object.keys(config.variables).length;
  const udtCount = Object.keys(config.udtVariables ?? {}).length;
  const varCount = atomicCount + udtCount;
  const deviceCount = Object.keys(config.devices).length;

  if (varCount === 0) {
    log.info("No variables configured — PLC idle.");
    return;
  }

  log.info(
    `Building PLC from gateway config: ${deviceCount} device(s), ${atomicCount} atomic + ${udtCount} UDT variable(s)`,
  );

  const plcConfig = translateConfig(config, { servers: natsUrl });
  currentPlc = await createPlc(plcConfig);

  log.info(`Gateway PLC running as "${config.gatewayId}".`);
}

/** Main entry point */
async function main(): Promise<void> {
  log.info(`tentacle-gateway starting (id: ${GATEWAY_ID})`);
  log.info(`Connecting to NATS at ${NATS_URL}...`);

  const nc = await connect({ servers: NATS_URL });
  log.info("Connected to NATS.");

  // Enable NATS log streaming
  log = createNatsLogger(log, nc, "gateway", GATEWAY_ID, "gateway");

  const js = jetstream(nc);
  const kvm = new Kvm(js);
  const encoder = new TextEncoder();

  // ── Heartbeat publishing ──────────────────────────────────────────────
  const heartbeatsKv = await kvm.create("service_heartbeats", {
    history: 1,
    ttl: 60 * 1000,
  });
  const startedAt = Date.now();

  const publishHeartbeat = async () => {
    const heartbeat: ServiceHeartbeat = {
      serviceType: "gateway",
      moduleId: GATEWAY_ID,
      lastSeen: Date.now(),
      startedAt,
      metadata: {
        plcRunning: String(currentPlc !== null),
      },
    };
    try {
      await heartbeatsKv.put(
        GATEWAY_ID,
        encoder.encode(JSON.stringify(heartbeat)),
      );
    } catch (err) {
      log.warn(`Failed to publish heartbeat: ${err}`);
    }
  };

  await publishHeartbeat();
  const heartbeatInterval = setInterval(publishHeartbeat, 10000);
  log.info(`Service heartbeat started (moduleId: ${GATEWAY_ID})`);

  // Create or open the gateway_config KV bucket
  const kv = await kvm.create(KV_BUCKET, {
    history: 5,
    description: "Gateway configuration (devices and variables) per instance",
  });

  // Try to load existing config
  try {
    const entry = await kv.get(GATEWAY_ID);
    if (entry?.value) {
      const config = JSON.parse(
        new TextDecoder().decode(entry.value),
      ) as GatewayConfigKV;
      log.info("Found existing config in KV — starting PLC...");
      await startPlc(config, NATS_URL);
    } else {
      log.info("No config found in KV — waiting for configuration...");
    }
  } catch (err) {
    if (!(err instanceof Error && err.message.includes("no such key"))) {
      log.error("Error loading initial config:", err);
    } else {
      log.info("No config found in KV — waiting for configuration...");
    }
  }

  // Handle shutdown (must be registered before the blocking watch loop)
  const shutdown = async () => {
    clearInterval(heartbeatInterval);
    try { await heartbeatsKv.delete(GATEWAY_ID); } catch { /* best effort */ }
    await stopCurrentPlc();
    await nc.close();
    Deno.exit(0);
  };

  Deno.addSignalListener("SIGINT", async () => {
    log.info("Received SIGINT — shutting down...");
    await shutdown();
  });

  Deno.addSignalListener("SIGTERM", async () => {
    log.info("Received SIGTERM — shutting down...");
    await shutdown();
  });

  // Watch for config changes
  log.info(`Watching KV bucket "${KV_BUCKET}" key "${GATEWAY_ID}" for changes...`);
  const watch = await kv.watch({ key: GATEWAY_ID });

  for await (const entry of watch) {
    if (entry.operation === "DEL" || entry.operation === "PURGE") {
      log.info("Config deleted — stopping PLC...");
      await stopCurrentPlc();
      continue;
    }

    if (!entry.value) continue;

    try {
      const config = JSON.parse(
        new TextDecoder().decode(entry.value),
      ) as GatewayConfigKV;
      log.info("Config updated — rebuilding PLC...");
      await stopCurrentPlc();
      await startPlc(config, NATS_URL);
    } catch (err) {
      log.error("Failed to apply config update:", err);
    }
  }
}

main().catch((err) => {
  log.error("Fatal error:", err);
  Deno.exit(1);
});
