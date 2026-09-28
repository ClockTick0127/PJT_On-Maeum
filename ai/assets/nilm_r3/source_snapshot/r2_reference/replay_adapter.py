"""Raw panel CSV -> unchanged simulator publisher -> real M1 forward -> state.

This executable never opens truth_sidecar or expected_scores. Each invocation is
a cold start in a fresh process. Longer-prefix runs use a distinct panel CSV.
"""
import argparse, asyncio, csv, hashlib, importlib, importlib.util, json, math, os, sys, time, types
from collections import deque
from datetime import datetime, timedelta
from pathlib import Path
from core import Decoder

FEATURES=('active_power','reactive_power','power_factor','current')

def sha(path):
    h=hashlib.sha256()
    with Path(path).open('rb') as f:
        for block in iter(lambda:f.read(8*1024*1024),b''):h.update(block)
    return h.hexdigest()

def checked(ref, field):
    p=Path(ref[field])
    if sha(p)!=ref[field+'_sha256']:raise ValueError('Hash mismatch: '+field)
    return p

def load_publisher(source, expected_sha):
    path=Path(source)/'engine/publisher.py'
    if sha(path)!=expected_sha:raise ValueError('Simulator publisher changed')
    if 'engine' in sys.modules:raise RuntimeError('Fresh process required')
    class DisabledClient:
        def __init__(self,*args,**kwargs):raise RuntimeError('Local transport only')
    shim=types.ModuleType('aiomqtt');shim.Client=DisabledClient;shim.MqttError=RuntimeError
    sys.modules['aiomqtt']=shim
    sys.path.insert(0,str(Path(source).resolve()))
    return importlib.import_module('engine.publisher').publish_house_power

class LocalSink:
    async def publish(self, topic, payload, qos=1):
        self.payload=json.loads(payload)
        self.topic=topic

async def execute(config_path):
    import numpy as np
    import torch
    config=json.loads(config_path.read_text(encoding='utf-8'))
    ref=json.loads(Path(config['model_ref']).read_text(encoding='utf-8'))
    checkpoint=checked(ref,'checkpoint');code=checked(ref,'model_code');normpath=checked(ref,'normalization')
    norm=json.loads(normpath.read_text())
    if ref['feature_order']!=list(FEATURES):raise ValueError('Feature axis mismatch')
    spec=importlib.util.spec_from_file_location('r2_preserved_model',code)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    torch.set_num_threads(2)
    torch.backends.cuda.matmul.allow_tf32=False;torch.backends.cudnn.allow_tf32=False
    device=torch.device('cuda:'+str(config['device']))
    model=module.CausalModel(ref['family']).to(device)
    bundle=torch.load(checkpoint,map_location='cpu',weights_only=False)
    model.load_state_dict(bundle['model']);model.eval()
    publisher=load_publisher(config['simulator_source'],config['publisher_sha256'])
    sink=LocalSink();panel=Path(config['panel'])
    if sha(panel)!=config['panel_sha256']:raise ValueError('Panel changed')
    output=Path(config['output'])
    output.mkdir(parents=True,exist_ok=False)
    mean=np.array(norm['mean']);std=np.array(norm['std'])
    if mean.shape!=(4,) or std.shape!=(4,) or np.any(std<=0):raise ValueError('Normalization invalid')
    on=float(np.float32(config['threshold']['on']));off=float(np.float32(config['threshold']['off']))
    decoder=Decoder(on,off,config['threshold']['confirm'])
    history=deque(maxlen=255);previous=None;first=None;count=0;forward_calls=0
    source_origin=datetime.fromisoformat(config['source_t0'])
    virtual_origin=datetime.fromisoformat(config['virtual_t0'])
    if virtual_origin.utcoffset() is None:raise ValueError('Explicit virtual timezone required')
    signature=dict(model_sha256=sha(checkpoint),model_code_sha256=sha(code),
                   normalization_sha256=sha(normpath),panel_sha256=sha(panel),
                   dtype='BF16',batch_size=1,tf32=False,feature_order=list(FEATURES),
                   torch_version=torch.__version__,device=str(device),
                   gpu_name=torch.cuda.get_device_name(device))
    begin=time.time()
    paths=[output/name for name in ('live_scores.jsonl','live_states.jsonl','trace.jsonl')]
    files=[p.with_suffix(p.suffix+'.partial').open('w',encoding='utf-8') for p in paths]
    def emit(file,row):file.write(json.dumps(row,allow_nan=False)+'\n')
    try:
        with panel.open(newline='',encoding='utf-8') as f:
            for row in csv.DictReader(f):
                seq=int(row['source_index'])
                if previous is not None and seq!=previous+1:raise ValueError('Panel must include missing seconds explicitly')
                if first is None:first=seq
                previous=seq
                if row['valid'] not in ('0','1') or row['context'] not in ('0','1'):raise ValueError('Boolean mask required')
                valid=row['valid']=='1';context=row['context']=='1'
                raw=np.array([float(row[k]) if row[k] else np.nan for k in FEATURES])
                if valid and not np.isfinite(raw).all():raise ValueError('Valid row has missing raw measurement')
                # Preserve the source normalization's mean fill on invalid rows.
                x=((raw if valid else mean)-mean)/std
                history.append(x.astype(np.float32))
                score=None;ready=bool(len(history)==255 and context)
                measured=(source_origin+timedelta(seconds=seq)).isoformat()
                virtual=(virtual_origin+timedelta(seconds=seq-first)).isoformat()
                envelope=None
                if valid:
                    metrics=dict(zip(FEATURES,map(float,raw)))
                    metrics.update(voltage=None,apparent_power=None,active_devices=None)
                    await publisher(sink,config['house'],virtual,allow_random=False,metrics=metrics)
                    envelope={k:v for k,v in sink.payload.items() if k!='message_id'}
                    assert all(envelope[k]==float(raw[i]) for i,k in enumerate(FEATURES))
                if ready:
                    xx=torch.from_numpy(np.stack(history)[None]).to(device)
                    with torch.inference_mode(),torch.autocast('cuda',dtype=torch.bfloat16):
                        score=float(torch.sigmoid(model(xx).float()).cpu().item())
                    forward_calls+=1
                    if not math.isfinite(score) or not 0<=score<=1:raise ValueError('Invalid model output')
                state=decoder.step(score)
                common=dict(run_id=config['run_id'],source_index=seq,source_time=measured,
                            virtual_time=virtual,source_mode='real_replay',transport='local')
                emit(files[0],dict(**common,on_score=score,ready=ready))
                emit(files[1],dict(**common,state=state))
                emit(files[2],dict(**common,event='measurement' if valid else 'missing',
                                   raw_envelope=envelope,generator_devices=None,ai_state=state,
                                   model_ready=ready,host_time=time.time()))
                count+=1
        emit(files[2],dict(run_id=config['run_id'],event='eof',force_off=False,source_index=previous))
    finally:
        for f in files:f.close()
    for p in paths:p.with_suffix(p.suffix+'.partial').replace(p)
    summary=dict(status='ACTUAL_FORWARD_COMPLETE',run_id=config['run_id'],count=count,
                 forward_calls=forward_calls,signature=signature,pid=os.getpid(),
                 seconds=time.time()-begin,truth_read=False,expected_scores_read=False,
                 batch_size=1,output_hashes={p.name:sha(p) for p in paths},
                 recommendation='external_review_pending')
    (output/'replay_run_summary.json').write_text(json.dumps(summary,indent=2))
    print(json.dumps(summary))

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('config',type=Path)
    asyncio.run(execute(parser.parse_args().config))
