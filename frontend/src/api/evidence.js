import {api} from './client.js';
export const evidence={
 status:()=>api('/evidence/status'),
 overview:()=>api('/evidence'),
 retry:()=>api('/evidence/retry',{method:'POST'}),
 verify:()=>api('/evidence/verify',{method:'POST'}),
 repair:()=>api('/evidence/repair',{method:'POST'}),
 drillLostReceipt:()=>api('/evidence/drills/lost-receipt',{method:'POST'}),
 drillTamper:mode=>api('/evidence/drills/tamper?mode='+encodeURIComponent(mode),{method:'POST'}),
};
