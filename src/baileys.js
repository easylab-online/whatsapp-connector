export async function baileysAdapter(store){
 const {default:makeWASocket,initAuthCreds,BufferJSON,proto,DisconnectReason}=await import('@whiskeysockets/baileys');
 const {default:pino}=await import('pino');
 return {async connect(id,onConnection,onEvent){
 const parse=x=>JSON.parse(x,BufferJSON.reviver),stringify=x=>JSON.stringify(x,BufferJSON.replacer);
 const creds=parse(store.secret(id,'creds')??stringify(initAuthCreds()));
 const keys={get:async(type,ids)=>Object.fromEntries(ids.map(k=>{const raw=store.secret(id,`${type}:${k}`);let v=raw?parse(raw):undefined;if(type==='app-state-sync-key'&&v)v=proto.Message.AppStateSyncKeyData.fromObject(v);return[k,v];})),set:async data=>{store.db.exec('BEGIN');try{for(const [type,items]of Object.entries(data))for(const [k,v]of Object.entries(items))store.secret(id,`${type}:${k}`,v==null?null:stringify(v));store.db.exec('COMMIT');}catch(e){store.db.exec('ROLLBACK');throw e;}}};
 const sock=makeWASocket({auth:{creds,keys},logger:pino({level:'silent'}),markOnlineOnConnect:false,syncFullHistory:false});
 sock.ev.on('creds.update',()=>store.secret(id,'creds',stringify(creds)));
 sock.ev.on('connection.update',u=>onConnection({qr:u.qr,open:u.connection==='open',closed:u.connection==='close',loggedOut:u.lastDisconnect?.error?.output?.statusCode===DisconnectReason.loggedOut}));
 sock.ev.on('messages.upsert',u=>{for(const m of u.messages){if(!m.message)continue;onEvent('message.upsert',{upsertType:u.type,message:parse(stringify(m))});}});
 sock.ev.on('messages.update',u=>onEvent('message.update',parse(stringify(u))));
 return sock;
 }};
}
