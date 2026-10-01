const express=require('express');
const http=require('http');
const {Server}=require('socket.io');
const app=express(),server=http.createServer(app),io=new Server(server);
app.use(express.static('public'));
const rooms=new Map();
const makeCode=()=>String(Math.floor(100000+Math.random()*900000));
function newRoom(){let c;do{c=makeCode()}while(rooms.has(c));return c}
function snapshot(r){return {code:r.code,teams:[...r.teams.values()].map(t=>({id:t.id,name:t.name,connected:t.connected,score:t.score})),phase:r.phase,winner:r.winner,question:r.question,questionNo:r.questionNo,locked:r.locked,timeLimit:r.timeLimit,buzzes:r.buzzes}}
function broadcast(r){io.to(r.code).emit('room:state',snapshot(r))}
function clearTimer(r){if(r.timer){clearTimeout(r.timer);r.timer=null}}
function startTimer(r){clearTimer(r);if(!r.timeLimit)return;r.timer=setTimeout(()=>{if(r.phase==='live'&&!r.locked){r.phase='ready';r.locked=true;r.timeout=true;broadcast(r)}},r.timeLimit*1000)}
io.on('connection',s=>{
 s.on('host:create',({question='',timeLimit=0}={})=>{const c=newRoom();const r={code:c,host:s.id,teams:new Map(),phase:'lobby',winner:null,question:String(question||'').slice(0,500),questionNo:1,locked:false,timeout:false,timeLimit:Number(timeLimit)||0,timer:null,buzzes:[]};rooms.set(c,r);s.join(c);s.data={role:'host',room:c};s.emit('host:created',snapshot(r))});
 s.on('team:join',({room,name})=>{room=String(room||'').trim();name=String(name||'').trim().slice(0,30);const r=rooms.get(room);if(!r||!name)return s.emit('join:error','Invalid room code or team name.');if([...r.teams.values()].some(t=>t.name.toLowerCase()===name.toLowerCase()))return s.emit('join:error','That team name is already taken.');const t={id:s.id,name,connected:true,score:0};r.teams.set(s.id,t);s.join(room);s.data={role:'team',room};s.emit('team:joined',{id:s.id,name,room});broadcast(r)});
 s.on('host:ready',({timeLimit}={})=>{const r=rooms.get(s.data.room);if(!r||r.host!==s.id)return;r.phase='live';r.winner=null;r.locked=false;r.timeout=false;r.buzzes=[];r.timeLimit=Number(timeLimit)||r.timeLimit||0;broadcast(r);startTimer(r)});
 s.on('host:reset',()=>{const r=rooms.get(s.data.room);if(!r||r.host!==s.id)return;clearTimer(r);r.phase='ready';r.winner=null;r.locked=false;r.timeout=false;r.buzzes=[];broadcast(r)});
 s.on('host:question',({question,timeLimit,increment=true}={})=>{const r=rooms.get(s.data.room);if(!r||r.host!==s.id)return;clearTimer(r);r.question=String(question||'').slice(0,500);if(increment)r.questionNo++;r.phase='ready';r.winner=null;r.locked=false;r.timeout=false;r.buzzes=[];if(timeLimit!==undefined)r.timeLimit=Number(timeLimit)||0;broadcast(r)});
 s.on('host:score',({teamId,delta})=>{const r=rooms.get(s.data.room);if(!r||r.host!==s.id)return;const t=r.teams.get(teamId);if(t)t.score=Math.max(0,t.score+(Number(delta)||0));broadcast(r)});
 s.on('team:buzz',()=>{const r=rooms.get(s.data.room),t=r?.teams.get(s.id);if(!r||!t||!t.connected||r.phase!=='live'||r.locked)return;r.locked=true;clearTimer(r);const at=Date.now();r.buzzes.push({id:t.id,name:t.name,at});r.winner={id:t.id,name:t.name,at};broadcast(r)});
 s.on('disconnect',()=>{const r=rooms.get(s.data.room);if(!r)return;if(r.host===s.id){clearTimer(r);io.to(r.code).emit('room:closed');rooms.delete(r.code);return}const t=r.teams.get(s.id);if(t){t.connected=false;broadcast(r)}})
});
server.listen(process.env.PORT||3000,()=>console.log('Sustainicity Buzzer running'));
