import { Elysia } from "elysia";
import users from "../users.json";

const tokens = new Set(Object.values(users));

export const auth = new Elysia({ name: "auth" }).onBeforeHandle(
  { as: "global" },
  ({ query, status }) => {
    const token = (query as Record<string, string>).token;
    if (!token || !tokens.has(token)) {
      return status(401, "Unauthorized");
    }
  },
);
