"""Re-run a preserved candidate into a new output without altering evidence."""
import argparse,json,subprocess,sys
from pathlib import Path
def main():
 p=argparse.ArgumentParser();p.add_argument('candidate',type=Path);p.add_argument('--output',type=Path,required=True);p.add_argument('--device',type=int,default=0);a=p.parse_args()
 folder=a.candidate.resolve();output=a.output.resolve();config_path=output.with_name(output.name+'.config.json')
 if output.exists() or config_path.exists():p.error('Choose a new output path; existing evidence is never overwritten.')
 c=json.loads((folder/'run_config.json').read_text());c.update(panel=str(folder/'panel.csv'),model_ref=str(folder/'model_ref.json'),output=str(output),run_id=output.name,device=a.device)
 for key in ('panel','model_ref','simulator_source'):
  if not Path(c[key]).exists():p.error('Missing '+key+': '+c[key])
 output.parent.mkdir(parents=True,exist_ok=True);config_path.write_text(json.dumps(c,indent=2))
 subprocess.run([sys.executable,str(Path(__file__).with_name('replay_adapter.py')),str(config_path)],check=True)
if __name__=='__main__':main()
