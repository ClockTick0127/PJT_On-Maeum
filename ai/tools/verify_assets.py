"""Verify repository asset bytes against the committed manifest and frozen lock."""
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1] / 'assets/nilm_r3'

def main():
    manifest = json.loads((ROOT / 'ASSET_MANIFEST.json').read_text(encoding='utf-8'))
    for item in manifest['files']:
        path = (ROOT / item['path']).resolve()
        assert path.is_relative_to(ROOT.resolve())
        data = path.read_bytes()
        assert len(data) == item['bytes'] and hashlib.sha256(data).hexdigest() == item['sha256'], item['path']
    lock = json.loads((ROOT / 'profiles/selected_profiles.lock.json').read_text())
    vendored = ROOT.parents[1] / 'realtime-analysis-service/src/realtime_analysis/real_models'
    assert json.loads((vendored / 'profiles.lock.json').read_text()) == lock
    for p in lock['profiles']:
        for key, digest in [('local_checkpoint_relative', 'checkpoint_sha256'), ('normalization_relative', 'normalization_sha256'), ('panel_relative', 'panel_sha256'), ('model_code_relative', 'model_code_sha256'), ('decoder_reference', 'decoder_reference_sha256')]:
            assert hashlib.sha256((ROOT / p[key]).read_bytes()).hexdigest() == p[digest], p[key]
        assert hashlib.sha256((vendored / 'models.py').read_bytes()).hexdigest() == p['model_code_sha256']
    print(json.dumps({'status': 'MATCH', 'files': len(manifest['files']), 'checkpoints': len(lock['profiles']), 'bytes': sum(i['bytes'] for i in manifest['files'])}))

if __name__ == '__main__': main()
