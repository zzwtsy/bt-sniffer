import { render, screen } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";
import { TorrentDetailPage } from "./detail-page";

const mocks = vi.hoisted(() => ({ data: {} }));
vi.mock("@tanstack/react-query", () => ({
  useQuery: () => ({ data: mocks.data, isPending: false }),
  queryOptions: (value: unknown) => value,
  infiniteQueryOptions: (value: unknown) => value,
}));
vi.mock("@tanstack/react-router", () => ({ Link: () => <a href="/torrents">返回</a> }));
vi.mock("./file-tree", () => ({ FileTree: () => <div>文件列表</div> }));
vi.mock("./preview-images", () => ({ PreviewImages: ({ hash }: { hash: string }) => <div data-testid="preview">{hash}</div> }));
vi.mock("./copy-magnet", () => ({ CopyMagnet: ({ hash }: { hash: string }) => <span data-testid="magnet">{hash}</span> }));
afterEach(() => {
  mocks.data = {};
});

const base = {
  semantic_status: "valid",
  semantic_reason: null,
  parse_status: "parsed",
  name: "Fixture",
  name_truncated: false,
  encoding_lossy: false,
  private: false,
  fetched_at_ms: 1_700_000_000_000,
  total_length: "42",
  file_count: 250,
};

function identity(format: string, v1: string, v2: string) {
  return format === "v2" ? [{ kind: "v2", hash: v2 }] : format === "hybrid" ? [{ kind: "v1", hash: v1 }, { kind: "v2", hash: v2 }] : [{ kind: "v1", hash: v1 }];
}

for (const format of ["v1", "v2", "hybrid"]) {
  it(`${format} 展示身份行与 meta 行，valid 不渲染校验类 UI，预览仅对 v1 身份开放`, () => {
    const v1 = "a".repeat(40);
    const v2 = "b".repeat(64);
    mocks.data = {
      ...base,
      hash: format === "v2" ? v2 : v1,
      format,
      identities: identity(format, v1, v2),
      private: format === "hybrid",
    };
    render(<TorrentDetailPage hash={format === "v2" ? v2 : v1} />);

    const magnets = screen.getAllByTestId("magnet");
    expect(magnets).toHaveLength(format === "hybrid" ? 2 : 1);
    expect(magnets[0]).toHaveTextContent(format === "v2" ? v2 : v1);
    expect(screen.queryAllByText("v1")).toHaveLength(format === "v2" ? 0 : 1);
    expect(screen.queryAllByText("v2")).toHaveLength(format === "v1" ? 0 : 1);

    expect(screen.getByText(/采集于/)).toBeInTheDocument();
    expect(screen.getByText("42 B")).toBeInTheDocument();
    expect(screen.getByText("250 个文件")).toBeInTheDocument();
    if (format === "hybrid")
      expect(screen.getByText("私有")).toBeInTheDocument();

    expect(screen.queryByText("Piece 长度")).not.toBeInTheDocument();
    expect(screen.queryByText("协议字节空间")).not.toBeInTheDocument();
    expect(screen.queryByText("填充字节")).not.toBeInTheDocument();
    expect(screen.queryByText(/完整校验/)).not.toBeInTheDocument();
    expect(screen.queryByText(/piece layers/)).not.toBeInTheDocument();
    expect(screen.queryByRole("alert")).not.toBeInTheDocument();

    if (format === "v2")
      expect(screen.queryByTestId("preview")).not.toBeInTheDocument();
    else
      expect(screen.getByTestId("preview")).toHaveTextContent(v1);
  });
}

it("invalid 显示警告与后端原文原因", () => {
  const v1 = "a".repeat(40);
  mocks.data = {
    ...base,
    hash: v1,
    format: "v1",
    identities: identity("v1", v1, ""),
    semantic_status: "invalid",
    parse_status: "unavailable",
    semantic_reason: "piece_count",
  };
  render(<TorrentDetailPage hash={v1} />);
  expect(screen.getByRole("alert")).toBeInTheDocument();
  expect(screen.getByText("无效")).toBeInTheDocument();
  expect(screen.getByText("piece_count")).toBeInTheDocument();
});

it("pending 无后端原因时显示兜底说明", () => {
  const v1 = "a".repeat(40);
  mocks.data = {
    ...base,
    hash: v1,
    format: "v1",
    identities: identity("v1", v1, ""),
    semantic_status: "pending",
  };
  render(<TorrentDetailPage hash={v1} />);
  expect(screen.getByText("待校验")).toBeInTheDocument();
  expect(screen.getByText(/尚未完成语义校验/)).toBeInTheDocument();
});

it("无身份记录回退为单行定位 hash", () => {
  const v1 = "c".repeat(40);
  mocks.data = {
    ...base,
    hash: v1,
    format: "unknown",
    identities: [],
  };
  render(<TorrentDetailPage hash={v1} />);
  const magnets = screen.getAllByTestId("magnet");
  expect(magnets).toHaveLength(1);
  expect(magnets[0]).toHaveTextContent(v1);
  expect(screen.getByText("未知")).toBeInTheDocument();
});

for (const status of ["invalid", "unsupported", "pending"]) {
  it(`${status} 与 unavailable 同时出现时只显示语义提示`, () => {
    mocks.data = { ...base, hash: "a".repeat(40), identities: [], format: "unknown", semantic_status: status, parse_status: "unavailable", semantic_reason: null };
    render(<TorrentDetailPage hash={"a".repeat(40)} />);
    expect(screen.getAllByRole("alert")).toHaveLength(1);
    expect(screen.queryByText("数据源不可用")).not.toBeInTheDocument();
    expect(screen.queryByText("文件列表")).not.toBeInTheDocument();
  });
}
it("没有语义异常时仍保留通用不可解析提示", () => {
  mocks.data = { ...base, hash: "a".repeat(40), identities: [], format: "v1", parse_status: "unavailable" };
  render(<TorrentDetailPage hash={"a".repeat(40)} />);
  expect(screen.getByText("数据源不可用")).toBeInTheDocument();
});
