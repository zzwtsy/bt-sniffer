import type { ReactNode } from "react";
import type { DisplayStore } from "./context";
import { DisplayContext } from "./context";

export function DisplayProvider({
  store,
  children,
}: {
  store: DisplayStore;
  children: ReactNode;
}) {
  return <DisplayContext value={store}>{children}</DisplayContext>;
}
