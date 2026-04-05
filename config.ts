/**
 * Gateway Config → PlcConfig Translation
 *
 * Converts the JSON-based GatewayConfigKV into the PlcConfig object
 * that tentacle-plc's createPlc() expects.
 */

import type { PlcVariableConfig, VariableSource, NatsConfig, UdtTemplateDefinition, DeadBandConfig } from "@tentacle/plc";
import type {
  GatewayConfigKV,
  GatewayDeviceConfig,
  GatewayVariableConfig,
  GatewayUdtVariable,
  GatewayUdtTemplate,
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
          ...(device.scanRate ? { scanRate: device.scanRate } : {}),
        },
      };
    case "opcua":
      return {
        ...base,
        opcua: {
          deviceId: variable.deviceId,
          endpointUrl: device.endpointUrl,
          nodeId: variable.tag,
          ...(device.scanRate ? { scanRate: device.scanRate } : {}),
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
          ...(device.scanRate ? { scanRate: device.scanRate } : {}),
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
          ...(device.scanRate ? { scanRate: device.scanRate } : {}),
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

  // Variable-level RBE overrides device-level defaults
  const deadband = variable.deadband ?? device.deadband;
  const disableRBE = variable.disableRBE ?? device.disableRBE;

  const base = {
    id: variable.id,
    description: variable.description ?? "",
    source,
    ...(deadband ? { deadband } : {}),
    ...(disableRBE ? { disableRBE: true } : {}),
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

/** Map a gateway CIP type or generic type to a PlcVariableConfig datatype */
function mapCipTypeToDatatype(cipType: string): "number" | "boolean" | "string" {
  const upper = cipType.toUpperCase();
  if (upper === "BOOL" || upper === "BOOLEAN") return "boolean";
  if (upper === "STRING") return "string";
  return "number";
}

/** Build default value object for a UDT variable from its member tags and CIP types */
function buildUdtDefault(
  memberTags: Record<string, string>,
  memberCipTypes?: Record<string, string>,
): Record<string, unknown> {
  const result: Record<string, unknown> = {};
  for (const memberPath of Object.keys(memberTags)) {
    const dt = mapCipTypeToDatatype(memberCipTypes?.[memberPath] ?? "number");
    // Set nested paths in the default object
    const parts = memberPath.split(".");
    let obj = result;
    for (let i = 0; i < parts.length - 1; i++) {
      if (!(parts[i] in obj) || typeof obj[parts[i]] !== "object") {
        obj[parts[i]] = {};
      }
      obj = obj[parts[i]] as Record<string, unknown>;
    }
    const leaf = parts[parts.length - 1];
    obj[leaf] = dt === "boolean" ? false : dt === "string" ? "" : 0;
  }
  return result;
}

/** Convert a GatewayUdtTemplate to a PlcVariableConfig UdtTemplateDefinition */
function convertTemplate(
  tmpl: GatewayUdtTemplate,
  allTemplates: Record<string, GatewayUdtTemplate>,
): UdtTemplateDefinition {
  return {
    name: tmpl.name,
    version: tmpl.version,
    members: tmpl.members.map((m) => ({
      name: m.name,
      datatype: (m.datatype === "struct" || m.datatype === "STRUCT")
        ? "number" as const
        : (m.datatype === "boolean" ? "boolean" as const : m.datatype === "string" ? "string" as const : "number" as const),
      ...(m.templateRef ? { templateRef: m.templateRef } : {}),
    })),
  };
}

/** Build a PlcVariableConfig (UDT type) from a gateway UDT variable */
function buildUdtVariable(
  udtVar: GatewayUdtVariable,
  device: GatewayDeviceConfig,
  templates: Record<string, GatewayUdtTemplate>,
): PlcVariableConfig {
  const deadband = device.deadband;
  const disableRBE = device.disableRBE;

  // Build per-member sources
  const memberSources: Record<string, VariableSource> = {};
  for (const [memberPath, tagPath] of Object.entries(udtVar.memberTags)) {
    memberSources[memberPath] = buildMemberSource(tagPath, udtVar.deviceId, device);
  }

  const tmpl = templates[udtVar.templateName];

  // Resolve per-member deadbands: instance override → template default → device default
  const memberDeadbands: Record<string, DeadBandConfig> = {};
  if (tmpl) {
    for (const member of tmpl.members) {
      // Only numeric members need deadband config
      const dt = member.datatype.toUpperCase();
      if (dt === "BOOL" || dt === "BOOLEAN" || dt === "STRING") continue;

      const instOverride = udtVar.memberDeadbands?.[member.name];
      const tmplDefault = member.defaultDeadband;
      const resolved = instOverride ?? tmplDefault ?? deadband;
      if (resolved) {
        memberDeadbands[member.name] = {
          value: resolved.value,
          ...(resolved.minTime != null ? { minTime: resolved.minTime } : {}),
          ...(resolved.maxTime != null ? { maxTime: resolved.maxTime } : {}),
        };
      }
    }
  }

  return {
    id: udtVar.id,
    description: "",
    datatype: "udt" as const,
    default: buildUdtDefault(udtVar.memberTags, udtVar.memberCipTypes),
    ...(tmpl ? { udtTemplate: convertTemplate(tmpl, templates) } : {}),
    memberSources,
    ...(deadband ? { deadband } : {}),
    ...(disableRBE ? { disableRBE: true } : {}),
    ...(Object.keys(memberDeadbands).length > 0 ? { memberDeadbands } : {}),
  };
}

/** Build a VariableSource for a single UDT member tag */
function buildMemberSource(
  tagPath: string,
  deviceId: string,
  device: GatewayDeviceConfig,
): VariableSource {
  switch (device.protocol) {
    case "ethernetip":
      return {
        ethernetip: {
          deviceId,
          host: device.host,
          port: device.port ?? 44818,
          tag: tagPath,
          ...(device.scanRate ? { scanRate: device.scanRate } : {}),
        },
      };
    case "opcua":
      return {
        opcua: {
          deviceId,
          endpointUrl: device.endpointUrl,
          nodeId: tagPath,
          ...(device.scanRate ? { scanRate: device.scanRate } : {}),
        },
      };
    default:
      return {};
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

  // Translate atomic variables
  for (const [varId, varConfig] of Object.entries(config.variables)) {
    const device = config.devices[varConfig.deviceId];
    if (!device) continue;
    variables[varId] = buildVariable(varConfig, device);
  }

  // Translate UDT variables
  const udtTemplates = config.udtTemplates ?? {};
  const udtVariables = config.udtVariables ?? {};
  for (const [varId, udtVar] of Object.entries(udtVariables)) {
    const device = config.devices[udtVar.deviceId];
    if (!device) continue;
    variables[varId] = buildUdtVariable(udtVar, device, udtTemplates);
  }

  return {
    projectId: config.gatewayId,
    variables,
    tasks: {},
    nats,
    skipHeartbeat: true,
  };
}
