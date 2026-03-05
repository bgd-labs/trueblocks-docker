import openapiTS, { astToString } from "openapi-typescript";

const SPEC_URL =
  "https://raw.githubusercontent.com/TrueBlocks/trueblocks-core/develop/docs/content/api/openapi.yaml";

// Fetch the upstream spec and patch a broken $ref before generation.
// `destType` is referenced in the `destination` schema but never defined —
// it has no effect on the /blocks endpoint we use.
const yaml = await fetch(SPEC_URL).then((r) => r.text());
const patched = yaml.replace(
  '$ref: "#/components/schemas/destType"',
  "type: string",
);

const tmp = "/tmp/trueblocks-openapi.yaml";
await Bun.write(tmp, patched);

const ast = await openapiTS(new URL(`file://${tmp}`));
await Bun.write("trueblocks.d.ts", astToString(ast));
console.log("✅ trueblocks.d.ts generated");
