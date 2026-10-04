import json, os, re
from pathlib import Path
obj=json.loads(Path('connectors/cdc.json').read_text())
for k,v in obj.items():
 if isinstance(v,str):
  obj[k]=re.sub(r'\$\{([A-Z_]+)\}',lambda m: os.environ[m[1]],v)
out=Path('.runtime'); out.mkdir(mode=0o700,exist_ok=True)
f=out/'cdc.json'; f.write_text(json.dumps(obj,indent=2)); f.chmod(0o600)
print('Config privada gerada; não publicar .runtime/')
