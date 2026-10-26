import { uuid7 } from './id.js';
import { createServer } from 'node:http';
import { mkdirSync } from 'node:fs';
import { createHash, timingSafeEqual, createHmac } from 'node:crypto';
import { Store } from './store.js';
import { Service } from './service.js';
import { baileysAdapter } from './baileys.js';
const {MASTER_KEY,CLIENTS_JSON,WEBHOOK_URL,WEBHOOK_SECRET}=process.env;
const clients=JSON.parse(CLIENTS_JSON??'[]');
if(!clients.length||clients.some(c=>!c.ownerId||!c.token||c.token.length<32))throw Error('Configure CLIENTS_JSON with ownerId and token (32+ characters)');
if(!WEBHOOK_URL||new URL(WEBHOOK_URL).protocol!=='https:'||!WEBHOOK_SECRET||WEBHOOK_SECRET.length<32)throw Error('HTTPS WEBHOOK_URL and 32+ character WEBHOOK_SECRET required');
mkdirSync('./data',{recursive:true,mode:0o700});const store=new Store('./data/connector.sqlite',MASTER_KEY);
const svc=new Service(store,await baileysAdapter(store));
const digest=x=>createHash('sha256').update(x).digest();
async function body(req){let b='';for await(const chunk of req){b+=chunk;if(Buffer.byteLength(b)>65536)throw Object.assign(Error('body_too_large'),{status:413});}try{return JSON.parse(b||'{}');}catch{throw Object.assign(Error('invalid_json'),{status:400});}}
const server=createServer(async(req,res)=>{res.setHeader('Content-Type','application/json');res.setHeader('Cache-Control','no-store');const reply=(status,obj)=>{res.writeHead(status);res.end(JSON.stringify(obj));};try{
 const path=new URL(req.url,'http://localhost').pathname;
 if(path==='/health.json'&&req.method==='GET')return reply(200,{version:'0.1.0',date:'2026-10-26',message:'WhatsApp connector running'});
 const token=(req.headers.authorization??'').replace(/^Bearer /,'');const client=clients.find(c=>timingSafeEqual(digest(c.token),digest(token)));if(!client)return reply(401,{error:'unauthorized'});
 if(path==='/v1/sessions'&&req.method==='GET')return reply(200,{sessions:svc.list(client.ownerId)});
 if(path==='/v1/sessions'&&req.method==='POST'){const id=uuid7();store.db.prepare('INSERT INTO sessions(id,owner) VALUES(?,?)').run(id,client.ownerId);await svc.start(client.ownerId,id);return reply(201,{id});}
 const match=path.match(/^\/v1\/sessions\/([a-f0-9-]{36})(?:\/(qr|start|stop|messages|logout))?$/);if(!match)return reply(404,{error:'not_found'});
 const [,id,action]=match;svc.owned(client.ownerId,id);
 if(req.method==='GET'&&!action)return reply(200,svc.list(client.ownerId).find(s=>s.id===id));
 if(req.method==='GET'&&action==='qr'){const e=svc.live.get(id);return reply(200,{qr:e?.qr&&Date.now()-e.qrAt<60000?e.qr:null,status:e?.status??'stopped'});}
 if(req.method==='POST'&&action==='start'){await svc.start(client.ownerId,id);store.db.prepare('UPDATE sessions SET active=1 WHERE id=?').run(id);return reply(200,{ok:true});}
 if(req.method==='POST'&&action==='stop'){await svc.stop(client.ownerId,id);return reply(200,{ok:true});}
 if(req.method==='POST'&&action==='logout'){const e=svc.live.get(id);if(!e?.socket)throw Object.assign(Error('connect_before_logout'),{status:409});await e.socket.logout();await svc.stop(client.ownerId,id);store.db.prepare('DELETE FROM secrets WHERE session=?').run(id);return reply(200,{ok:true});}
 if(req.method==='POST'&&action==='messages'){const b=await body(req);if(typeof b.to!=='string'||!/^\d{5,20}@s\.whatsapp\.net$/.test(b.to)||typeof b.text!=='string'||!b.text.trim()||b.text.length>10000)throw Object.assign(Error('text_and_individual_jid_required'),{status:400});return reply(200,await svc.send(client.ownerId,id,req.headers['idempotency-key'],b));}
 return reply(405,{error:'method_not_allowed'});
 }catch(e){reply(e.status??500,{error:e.status?e.message:'internal_error'});}});
let delivering=false;async function deliver(){if(delivering)return;delivering=true;try{const rows=store.db.prepare('SELECT * FROM events WHERE due<=? ORDER BY due LIMIT 25').all(Date.now());for(const row of rows){try{const payload=store.open(row.body),stamp=String(Date.now()),signature=createHmac('sha256',WEBHOOK_SECRET).update(`${stamp}.${payload}`).digest('hex');const r=await fetch(WEBHOOK_URL,{method:'POST',redirect:'error',headers:{'content-type':'application/json','x-event-id':row.id,'x-timestamp':stamp,'x-signature':signature},body:payload,signal:AbortSignal.timeout(10000)});await r.body?.cancel();if(!r.ok)throw Error('delivery_failed');store.db.prepare('DELETE FROM events WHERE id=?').run(row.id);}catch{store.db.prepare('UPDATE events SET tries=tries+1,due=? WHERE id=?').run(Date.now()+Math.min(300000,1000*2**Math.min(row.tries,8))+Math.random()*1000,row.id);}}}finally{delivering=false;}}
const interval=setInterval(()=>deliver().catch(()=>{}),1000);
for(const s of store.db.prepare('SELECT * FROM sessions WHERE active=1').all())await svc.start(s.owner,s.id).catch(()=>{});
server.listen(Number(process.env.PORT??3080),process.env.HOST??'127.0.0.1');
async function shutdown(){clearInterval(interval);server.close();await svc.shutdown();while(delivering)await new Promise(r=>setTimeout(r,50));store.close();process.exit(0);}
process.once('SIGTERM',shutdown);process.once('SIGINT',shutdown);
