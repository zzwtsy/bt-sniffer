import type { FileItem } from "@/lib/api/torrents";

export interface TreeNode {
  name: string;
  /** 目录为完整前缀，文件为完整路径；同时作为展开状态的稳定键。 */
  path: string;
  depth: number;
  file?: FileItem;
  children: TreeNode[];
  /** 目录的后代文件总数；文件节点恒为 0。 */
  count: number;
}

/** 把扁平文件清单按 "/" 聚合为目录树；节点保持后端原始顺序，目录出现在首次引用位置。 */
export function buildFileTree(items: FileItem[]): TreeNode[] {
  const roots: TreeNode[] = [];
  const dirs = new Map<string, TreeNode>();
  for (const file of items) {
    const segments = file.path.split("/");
    let siblings = roots;
    let prefix = "";
    for (let depth = 0; depth < segments.length - 1; depth++) {
      prefix = depth === 0 ? segments[0] : `${prefix}/${segments[depth]}`;
      let dir = dirs.get(prefix);
      if (dir == null) {
        dir = { name: segments[depth], path: prefix, depth, children: [], count: 0 };
        dirs.set(prefix, dir);
        siblings.push(dir);
      }
      dir.count++;
      siblings = dir.children;
    }
    siblings.push({
      name: segments[segments.length - 1],
      path: file.path,
      depth: segments.length - 1,
      file,
      children: [],
      count: 0,
    });
  }
  return roots;
}
