import { WEB_BASE_PATH } from "@/config/base-path";

const COOKIE_NAME = "multica_logged_in";

export function setLoggedInCookie() {
  document.cookie = `${COOKIE_NAME}=1; path=${WEB_BASE_PATH || "/"}; max-age=31536000; samesite=lax`;
}

export function clearLoggedInCookie() {
  document.cookie = `${COOKIE_NAME}=; path=${WEB_BASE_PATH || "/"}; max-age=0`;
}
