import createClient from "openapi-fetch";
import type { paths } from "./trueblocks.d.ts";

const TRUEBLOCKS_URL = process.env.TRUEBLOCKS_URL ?? "http://trueblocks:8080";

console.log(`TrueBlocks URL: ${TRUEBLOCKS_URL}`);

export const trueblocks = createClient<paths>({ baseUrl: TRUEBLOCKS_URL });
