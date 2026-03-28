/**
 * Gateway Config → PlcConfig Translation
 *
 * Converts the JSON-based GatewayConfigKV into the PlcConfig object
 * that tentacle-plc's createPlc() expects.
 */

import type { PlcVariableConfig, VariableSource, NatsConfig } from "@tentacle/plc";
import type {
  GatewayConfigKV,
  GatewayDeviceConfig,
  GatewayVariableConfig,
} from "@tentacle/nats-schema";

/** Build a VariableSource from a gateway variable + its device config */
function buildSource(
  variable: GatewayVariableConfig,
  device: GatewayDeviceConfig,
): VariableSource {
  const base: VariableSource = {
    bidirectional: variable.bidirectional,
  };

  switch (device.protocol) {
    case "ethernetip":
      return {
        ...base,
        ethernetip: {
          deviceId: variable.deviceId,
          host: device.host,
          port: device.port ?? 44818,
          tag: variable.tag,
        },
      };
    case "opcua":
      return {
        ...base,
        opcua: {
          deviceId: variable.deviceId,
          endpointUrl: device.endpointUrl,
          nodeId: variable.tag,
        },
      };
    case "snmp":
      return {
        ...base,
        snmp: {
          deviceId: variable.deviceId,
          host: device.host,
          port: device.port ?? 161,
          version: device.version === "1" ? "v1" : device.version === "2c" ? "v2c" : "v3",
          ...(device.community ? { community: device.community } : {}),
          ...(device.v3Auth
            ? {
                v3Auth: {
                  username: device.v3Auth.username,
                  securityLevel: device.v3Auth.securityLevel,
                  ...(device.v3Auth.authProtocol
                    ? { authProtocol: device.v3Auth.authProtocol }
                    : {}),
                  ...(device.v3Auth.authPassword
                    ? { authPassword: device.v3Auth.authPassword }
                    : {}),
                  ...(device.v3Auth.privProtocol
                    ? { privProtocol: device.v3Auth.privProtocol }
                    : {}),
                  ...(device.v3Auth.privPassword
                    ? { privPassword: device.v3Auth.privPassword }
                    : {}),
                },
              }
            : {}),
          oid: variable.tag,
        },
      };
    case "modbus":
      return {
        ...base,
        modbus: {
          deviceId: variable.deviceId,
          host: device.host,
          port: device.port ?? 502,
          unitId: device.unitId ?? 1,
          tag: variable.tag,
          address: variable.address ?? 0,
          functionCode: (variable.functionCode === 1
            ? "coil"
            : variable.functionCode === 2
              ? "discrete"
              : variable.functionCode === 4
                ? "input"
                : "holding") as "coil" | "discrete" | "holding" | "input",
          modbusDatatype: variable.modbusDatatype ?? "uint16",
          byteOrder: variable.byteOrder ?? "big",
        },
      };
  }
}

/** Build a PlcVariableConfig from a gateway variable */
function buildVariable(
  variable: GatewayVariableConfig,
  device: GatewayDeviceConfig,
): PlcVariableConfig {
  const source = buildSource(variable, device);

  const base = {
    id: variable.id,
    description: variable.description ?? "",
    source,
    ...(variable.deadband ? { deadband: variable.deadband } : {}),
    ...(variable.disableRBE ? { disableRBE: true } : {}),
  };

  switch (variable.datatype) {
    case "boolean":
      return { ...base, datatype: "boolean" as const, default: (variable.default as boolean) ?? false };
    case "string":
      return { ...base, datatype: "string" as const, default: (variable.default as string) ?? "" };
    case "number":
    default:
      return { ...base, datatype: "number" as const, default: (variable.default as number) ?? 0 };
  }
}

/**
 * Translate a GatewayConfigKV into the variables map expected by PlcConfig.
 * Skips variables whose deviceId doesn't match any defined device.
 */
export function translateConfig(
  config: GatewayConfigKV,
  nats: NatsConfig,
): {
  projectId: string;
  variables: Record<string, PlcVariableConfig>;
  tasks: Record<string, never>;
  nats: NatsConfig;
  skipHeartbeat: boolean;
} {
  const variables: Record<string, PlcVariableConfig> = {};

  for (const [varId, varConfig] of Object.entries(config.variables)) {
    const device = config.devices[varConfig.deviceId];
    if (!device) continue;
    variables[varId] = buildVariable(varConfig, device);
  }

  return {
    projectId: config.gatewayId,
    variables,
    tasks: {},
    nats,
    skipHeartbeat: true,
  };
}
