import { useNavigate } from "@tanstack/react-router";
import { Search } from "lucide-react";
import { useState } from "react";
import { HashLink } from "@/components/observation/common";
import { Records } from "@/components/observation/records";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { hashPattern, string } from "@/lib/observation/contracts";
import { time } from "@/lib/observation/format";

export function HashesPage() {
  const navigate = useNavigate();
  const [hash, setHash] = useState("");
  const [error, setError] = useState("");
  return (
    <Records
      title="已发现的 hash"
      eyebrow="HASH INDEX"
      description="SQLite 保存的业务事实，重启后仍可查询。输入完整 hash 可直接定位。"
      endpoint="/hashes"
      columns={[
        { name: "hash", cell: r => <HashLink hash={string(r.hash)} /> },
        { name: "首次发现", cell: r => time(r.first_seen_ms) },
        { name: "最近观察", cell: r => time(r.last_seen_ms) },
      ]}
      controls={(
        <form
          className="mb-3 flex flex-wrap items-center gap-3 py-3.5"
          onSubmit={(e) => {
            e.preventDefault();
            const value = hash.trim().toLowerCase();
            if (!hashPattern.test(value)) {
              setError("请输入 40 位十六进制 hash");
              return;
            }
            setError("");
            void navigate({ to: "/hashes/$hash", params: { hash: value } });
          }}
        >
          <Search size={16} aria-hidden="true" />
          <Input
            aria-label="完整 hash"
            placeholder="输入 40 位 hash，查看采集链路"
            className="min-w-45 max-w-110 flex-1"
            value={hash}
            onChange={e => setHash(e.target.value)}
          />
          <Button>定位 hash</Button>
          {error && <span role="alert">{error}</span>}
        </form>
      )}
    />
  );
}
