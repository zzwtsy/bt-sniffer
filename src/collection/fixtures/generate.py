import hashlib, json
from pathlib import Path
out=Path('src/collection/fixtures')
def enc(v):
 if isinstance(v,int): return b'i'+str(v).encode()+b'e'
 if isinstance(v,bytes): return str(len(v)).encode()+b':'+v
 if isinstance(v,list): return b'l'+b''.join(map(enc,v))+b'e'
 return b'd'+b''.join(enc(k)+enc(v[k]) for k in sorted(v))+b'e'
root=hashlib.sha256(b'a').digest()
base={b'name':b'root',b'piece length':16384}
a={b'length':1,b'pieces root':root}
b={b'length':0}
tree={b'a':{b'':a},b'b':{b'':b}}
v2={**base,b'meta version':2,b'file tree':tree}
legacy=[{b'length':1,b'path':[b'a']},{b'length':0,b'path':[b'b']}]
hybrid={**v2,b'files':legacy,b'pieces':hashlib.sha1(b'a').digest()}
v1={**base,b'length':1,b'pieces':hashlib.sha1(b'a').digest()}
cases={
 'v1':(v1,'valid',None),
 'v2':(v2,'valid',None),
 'hybrid':(hybrid,'valid',None),
 'hybrid-mismatch':({**hybrid,b'files':[{b'length':1,b'path':[b'c']},legacy[1]]},'invalid','hybrid_layout'),
 'v2-bad-root':({**v2,b'file tree':{b'a':{b'':{**a,b'pieces root':b'x'}}}},'invalid','pieces_root'),
 'v2-tree-conflict':({**v2,b'file tree':{b'a':{b'':a,b'b':{b'':b}}}},'invalid','file_tree_conflict'),
 'v2-bad-piece':({**v2,b'piece length':8192},'invalid','piece_length'),
 'unknown-version':({**v1,b'meta version':3},'unsupported','unsupported_meta_version'),
 'v1-bad-pieces':({**v1,b'pieces':b''},'invalid','piece_count'),
 'v1-empty':({**v1,b'length':0,b'pieces':b''},'valid',None),
 'bep47':({**base,b'files':[{b'attr':b'hx?',b'length':1,b'path':[b'a'],b'sha1':hashlib.sha1(b'a').digest()},{b'attr':b'p',b'length':16383},{b'attr':b'l',b'path':[b'link'],b'symlink path':[b'a']}],b'pieces':hashlib.sha1(b'a'+bytes(16383)).digest()},'valid',None),
 'overflow':({**base,b'files':[{b'length':2**63-1,b'path':[str(i).encode()]} for i in range(3)],b'pieces':b''},'invalid','length_overflow'),
 'hybrid-padding':({**hybrid,b'file tree':{b'a':{b'':a},b'b':{b'':a}},b'files':[legacy[0],{b'attr':b'p',b'length':16383},{b'length':1,b'path':[b'b']}],b'pieces':hashlib.sha1(b'a'+bytes(16383)).digest()+hashlib.sha1(b'a').digest()},'valid',None),
}
manifest=[]
for name,(value,status,reason) in cases.items():
 data=enc(value);(out/(name+'.info')).write_bytes(data)
 manifest.append(dict(name=name,sha1=hashlib.sha1(data).hexdigest(),sha256=hashlib.sha256(data).hexdigest(),status=status,reason=reason))
(out/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')

# 使用 BEP 52 官方生成器生成目录夹具；外部参考资料不随本脚本下载。
import importlib.util
import sys
import tempfile
sys.dont_write_bytecode = True
reference = Path('docs/bittorrent.org/beps/bep_0052_torrent_creator.py')
spec = importlib.util.spec_from_file_location('bep52_reference', reference)
creator = importlib.util.module_from_spec(spec)
spec.loader.exec_module(creator)
with tempfile.TemporaryDirectory() as directory:
 root_path = Path(directory) / 'root'
 root_path.mkdir()
 (root_path / 'a').write_bytes(b'a')
 (root_path / 'empty' / 'nested').mkdir(parents=True)
 torrent = creator.Torrent(str(root_path), 16384)
 for hybrid in (False, True):
  name = 'hybrid-empty-directory' if hybrid else 'v2-empty-directory'
  data = creator.encode(torrent.create(b'', hybrid=hybrid)[b'info'])
  (out / (name + '.info')).write_bytes(data)
  manifest.append(dict(name=name, sha1=hashlib.sha1(data).hexdigest(), sha256=hashlib.sha256(data).hexdigest(), status='valid', reason=None))
(out/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
