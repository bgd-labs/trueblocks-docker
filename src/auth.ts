import { Elysia } from "elysia";
import users from "../users.json";

const tokens = new Set(Object.values(users));

export const auth = new Elysia({ name: "auth" }).onBeforeHandle(
  { as: "global" },
  ({ headers, status }) => {
    const authHeader = headers.authorization;
    const token = authHeader?.startsWith("Bearer ")
      ? authHeader.slice(7)
      : undefined;

    if (!token || !tokens.has(token)) {
      return status(401, "Unauthorized");
    }
  },
);
