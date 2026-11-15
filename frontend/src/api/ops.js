import {api} from '../api/client.js';
export const backupApi={
 status:()=>api('/ops/status'),
 list:()=>api('/backups'),
 create:data=>api('/backups',{method:'POST',body:JSON.stringify(data)}),
 get:id=>api('/backups/'+id),
 verify:id=>api('/backups/'+id+'/verify',{method:'POST'}),
 restores:()=>api('/restores'),
 restore:data=>api('/restores',{method:'POST',body:JSON.stringify(data)}),
 restoreStatus:id=>api('/restores/'+id),
 resume:id=>api('/restores/'+id+'/resume',{method:'POST'}),
 cancel:id=>api('/restores/'+id+'/cancel',{method:'POST'}),
 rollback:id=>api('/restores/'+id+'/rollback',{method:'POST'})
};
