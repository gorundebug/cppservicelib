// Workspace-only reuse of the existing isolated official broker fixture.
import {startFixture} from '../../../tsservicelib/test/native-probes/pulsar-auth-fixture.mjs';
import {spawn} from 'node:child_process';
import {fileURLToPath} from 'node:url';
import {resolve,dirname} from 'node:path';
const root=resolve(dirname(fileURLToPath(import.meta.url)),'../..');
const fixture=await startFixture();
async function admin(method,path,body){const response=await fetch(fixture.adminUrl+path,{method,headers:{authorization:'Bearer '+fixture.tokens.admin,'content-type':'application/json'},...(body===undefined?{}:{body:JSON.stringify(body)}),signal:AbortSignal.timeout(5000)});if(!response.ok)throw Error('fixture admin HTTP '+response.status);}
try {
 await admin('PUT','/admin/v2/tenants/acl',{adminRoles:['admin','tenant-admin'],allowedClusters:['standalone']});
 for(const ns of ['events','denied']){
  await admin('PUT','/admin/v2/namespaces/acl/'+ns,{});
  for(const [policy,field] of [['autoTopicCreation','allowAutoTopicCreation'],['autoSubscriptionCreation','allowAutoSubscriptionCreation']])await admin('POST',`/admin/v2/namespaces/acl/${ns}/${policy}`,{[field]:false});
  await admin('PUT',`/admin/v2/persistent/acl/${ns}/allowed`);
 }
 for(const [role,actions] of [['producer',['produce']],['consumer',['consume']]])await admin('POST','/admin/v2/namespaces/acl/events/permissions/'+role,actions);
 await admin('POST','/admin/v2/persistent/acl/events/allowed/permissions/topic-producer',['produce']);
 const child=spawn('docker',['run','--rm','-v','cppservicelib_cppservicelib-conan2:/conan:ro','-v',`${root}:/workspace:ro`,'-v',`${process.env.PULSAR_CPP_PROBE_ROOT??'/private/tmp/pulsar-cpp-eligibility'}:/probe:ro`,'-v',`${fixture.manifestPath}:/fixture.json:ro`,'--entrypoint','python3','cppservicelib-build:latest','/workspace/tests/pulsar_native/auth.py','/probe/userver-sdk/pulsar_native','/fixture.json'],{stdio:'inherit'});
 await new Promise((res,rej)=>{child.on('error',rej);child.on('exit',code=>code===0?res():rej(Error('userver ACL exit '+code)));});
 if(!await fixture.secretFreeBrokerLogs())throw Error('fixture auth values leaked');
}finally{await fixture.close();}
