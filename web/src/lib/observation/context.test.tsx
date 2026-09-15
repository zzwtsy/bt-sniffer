import { act, fireEvent, render, screen } from "@testing-library/react";
import { expect, it } from "vitest";
import { DisplayProvider } from "@/lib/observation/display-provider";
import { DisplayStore, useDisplayed } from "./context";

it("冻结当前画面、恢复最新数据，拒绝超过预算的冻结", () => {
  const store = new DisplayStore();
  function Example({ value }: { value: number }) {
    const displayed = useDisplayed("value", value);
    return (
      <>
        <span>
          值
          {displayed}
        </span>
        <button onClick={store.toggle}>切换</button>
      </>
    );
  }
  const view = render(
    <DisplayProvider store={store}>
      <Example value={1} />
    </DisplayProvider>,
  );
  fireEvent.click(screen.getByText("切换"));
  view.rerender(
    <DisplayProvider store={store}>
      <Example value={2} />
    </DisplayProvider>,
  );
  expect(screen.getByText(/值\s*1/)).toBeInTheDocument();
  fireEvent.click(screen.getByText("切换"));
  expect(screen.getByText(/值\s*2/)).toBeInTheDocument();
  store.register("large", "x".repeat(2 * 1024 * 1024));
  act(() => store.toggle());
  expect(store.getSnapshot().frozen).toBe(false);
  expect(store.getSnapshot().error).toContain("预算");
});
