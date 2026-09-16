// Turn the contracts bundle into TypeScript types.
//
// The bundle is what the node validates against, so these types say exactly
// what a record may contain — a field this file does not know is a field the
// node will refuse. `src/generated/contracts.ts` is committed so that using the
// client needs no Python; CI regenerates it and fails on a difference.
//
//   make bundle && npm run generate:types
//
// Plain JavaScript on purpose: this runs on any Node the package supports,
// without a build step or a loader flag.

import { readFileSync, mkdirSync, writeFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const HERE = dirname(fileURLToPath(import.meta.url));
const OUT = resolve(HERE, "../src/generated/contracts.ts");

const bundlePath = resolve(process.cwd(), process.argv[2] ?? "../../build/bundle/contracts-bundle.json");
const bundle = JSON.parse(readFileSync(bundlePath, "utf8"));

/** `_CmdConfigure` is the wire name; `CmdConfigure` is the type name. */
const typeName = (contract) => contract.replace(/^_/, "");

/** JSON Schema to a TypeScript type, as far as the bundle's schemas go. */
function render(schema, indent = "  ") {
  if (!schema || typeof schema !== "object") return "unknown";

  if (Array.isArray(schema.enum)) {
    return schema.enum.map((value) => JSON.stringify(value)).join(" | ");
  }

  const types = Array.isArray(schema.type) ? schema.type : [schema.type];
  const rendered = types.filter(Boolean).map((type) => renderOne(type, schema, indent));
  return rendered.length > 0 ? [...new Set(rendered)].join(" | ") : "unknown";
}

function renderOne(type, schema, indent) {
  switch (type) {
    case "string":
      return "string";
    case "number":
    case "integer":
      return "number";
    case "boolean":
      return "boolean";
    case "null":
      return "null";
    case "array":
      return `${render(schema.items, indent)}[]`;
    case "object":
      return renderObject(schema, indent);
    default:
      return "unknown";
  }
}

function renderObject(schema, indent) {
  const properties = schema.properties ?? {};
  const names = Object.keys(properties);
  if (names.length === 0) {
    // An object without declared properties: anything JSON, but still an object.
    return schema.additionalProperties === false ? "Record<string, never>" : "Record<string, unknown>";
  }

  const required = new Set(schema.required ?? []);
  const inner = `${indent}  `;
  const lines = names.map((name) => {
    const optional = required.has(name) ? "" : "?";
    const key = /^[A-Za-z_$][\w$]*$/.test(name) ? name : JSON.stringify(name);
    return `${inner}${key}${optional}: ${render(properties[name], inner)};`;
  });
  if (schema.additionalProperties !== false) {
    lines.push(`${inner}[key: string]: unknown;`);
  }
  return `{\n${lines.join("\n")}\n${indent}}`;
}

const contracts = Object.entries(bundle.contracts).sort(([a], [b]) => a.localeCompare(b));

const header = `// Generated from the contracts bundle — do not edit.
//
// Source:  ${bundle.source?.package ?? "colca-data-contracts"} @ ${bundle.source?.git_sha ?? "unknown"}
// Bundle:  version ${bundle.bundle_version}, digest ${String(bundle.digest).slice(0, 16)}…
// Command: make bundle && npm run generate:types
`;

const types = contracts.map(([contract, entry]) => {
  const name = typeName(contract);
  const body = render(entry.schema, "");
  return `/** \`${contract}\` — stream class \`${entry.class}\`. */\nexport type ${name} = ${body};\n`;
});

const table = contracts
  .map(([contract, entry]) => {
    const tombstone = entry.tombstone ? "true" : "false";
    return `  "${contract}": { class: "${entry.class}", tombstone: ${tombstone} },`;
  })
  .join("\n");

const payloads = contracts.map(([contract]) => `  "${contract}": ${typeName(contract)};`).join("\n");

const body = `${header}
${types.join("\n")}
/** Every contract the node knows, with the stream it lands on. */
export const CONTRACTS = {
${table}
} as const;

/** The wire names, \`_Metric\` and friends. */
export type ContractName = keyof typeof CONTRACTS;

/** The payload type behind a contract name: \`PayloadOf<"_Metric">\`. */
export interface PayloadByContract {
${payloads}
}

export type PayloadOf<T extends ContractName> = PayloadByContract[T];
`;

mkdirSync(dirname(OUT), { recursive: true });
writeFileSync(OUT, body, "utf8");
console.log(`${contracts.length} contracts -> ${OUT}`);
