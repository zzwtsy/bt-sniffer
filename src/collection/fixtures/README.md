# 元数据离线夹具

依据 BEP 3、47、52 的字段布局，由 `generate.py` 的独立 Python Bencode 编码器生成；摘要使用 Python 标准库 hashlib。测试读取固定 `.info` 与 `manifest.json`，不调用生产编码器。

`v2` 与 `hybrid` 包含一个单字节文件及一个空文件；`hybrid-padding` 在两个非空文件间显式填充至 16 KiB。BEP 47 夹具覆盖无路径 padding、无 length 符号链接、未知属性及 SHA-1 提示。无效夹具分别固定错误原因。这里只验证 info 结构与身份，不验证文件内容或 piece layers。

在仓库根执行 `python3 src/collection/fixtures/generate.py` 可重建固定夹具与预期摘要。

`v2-empty-directory` 和 `hybrid-empty-directory` 使用本地外部资料 `docs/bittorrent.org/beps/bep_0052_torrent_creator.py`（BEP 52 官方参考生成器）生成，输入为一个内容为 `a` 的文件及多层空目录；预期只有一个文件、一个字节、一个分片，两个格式均有效。固定摘要见 `manifest.json`。重建需先具备该参考文件，脚本不访问网络。
