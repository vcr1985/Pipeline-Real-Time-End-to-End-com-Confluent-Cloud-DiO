"""Consumo Avro + efeito local durável antes de commit. Não é exactly-once global."""
import os,json,sqlite3
from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField
sr=SchemaRegistryClient({'url':os.environ['SR_URL'],'basic.auth.user.info':os.environ['SR_KEY']+':'+os.environ['SR_SECRET']})
decode=AvroDeserializer(sr)
decode_key=AvroDeserializer(sr)
c=Consumer({'bootstrap.servers':os.environ['BOOTSTRAP_SERVERS'],'security.protocol':'SASL_SSL','sasl.mechanism':'PLAIN','sasl.username':os.environ['CONSUMER_KEY'],'sasl.password':os.environ['CONSUMER_SECRET'],'group.id':'desafio-consumer-demo','auto.offset.reset':'earliest','enable.auto.commit':False})
db=sqlite3.connect('alerts.db')
db.execute('CREATE TABLE IF NOT EXISTS alerts(alert_key TEXT PRIMARY KEY,payload TEXT NOT NULL)')
c.subscribe(['desafio.fraud.detected'])
try:
 while True:
  m=c.poll(1)
  if m is None:continue
  if m.error():raise RuntimeError(str(m.error()))
  if m.value() is None:
   c.commit(message=m,asynchronous=False);continue
  obj=decode(m.value(),SerializationContext(m.topic(),MessageField.VALUE))
  if 'card_id' not in obj and m.key() is not None:
   chave=decode_key(m.key(),SerializationContext(m.topic(),MessageField.KEY))
   obj['card_id']=chave['card_id']
  # Chave de negócio: resiste a reprocessamento em offsets diferentes.
  key=json.dumps([obj['card_id'],obj['primeira_transacao'],obj['ultima_transacao'],'RULE_VELOCITY'])
  with db:db.execute('INSERT OR IGNORE INTO alerts VALUES (?,?)',(key,json.dumps(obj,default=str)))
  c.commit(message=m,asynchronous=False)
  print(json.dumps({'topic':m.topic(),'partition':m.partition(),'offset':m.offset(),'alert':obj},default=str),flush=True)
finally:
 c.close();db.close()
