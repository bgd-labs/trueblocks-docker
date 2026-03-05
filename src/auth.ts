import users from "../users.json";

const tokens = new Set(Object.values(users));

export const tokenSet = tokens;
