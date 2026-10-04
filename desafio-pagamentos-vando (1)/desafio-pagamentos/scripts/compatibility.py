# Executa contra subject isolado; não sobrescreve schemas do CDC.
import json, os, base64, urllib.request
from pathlib import Path
subject='desafio-contract-payment-value'
root=os.environ['SR_URL'].rstrip('/')
auth=base64.b64encode((os.environ['SR_KEY']+':'+os.environ['SR_SECRET']).encode()).decode()
def call(method,path,obj):
 req=urllib.request.Request(root+path,json.dumps(obj).encode(),method=method,headers={'Authorization':'Basic '+auth,'Content-Type':'application/vnd.schemaregistry.v1+json'})
 with urllib.request.urlopen(req,timeout=30) as f:return json.load(f)
def schema(name):return {'schemaType':'AVRO','schema':Path('schemas/'+name).read_text()}
print(call('PUT','/config/'+subject,{'compatibility':'BACKWARD'}))
print(call('POST','/subjects/'+subject+'/versions',schema('payment-v1.avsc')))
for name,expected in [('payment-v2-default.avsc',True),('payment-v2-incompatible.avsc',False),('payment-v2-remove.avsc',True)]:
 result=call('POST','/compatibility/subjects/'+subject+'/versions/latest?verbose=true',schema(name))
 print(name,result)
 assert result['is_compatible'] is expected,(name,result)
# Para registrar rejeição da remoção: política FORWARD, explicitamente documentada.
print(call('PUT','/config/'+subject,{'compatibility':'FORWARD'}))
result=call('POST','/compatibility/subjects/'+subject+'/versions/latest?verbose=true',schema('payment-v2-remove.avsc'))
print('FORWARD: remoção',result);assert result['is_compatible'] is False
print(call('PUT','/config/'+subject,{'compatibility':'BACKWARD'}))
