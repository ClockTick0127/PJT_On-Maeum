"""Inject verified image digests without evaluating the runtime env as shell code."""
from release import read_expected, update_env
import argparse
import shutil

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    data = read_expected("release.json")
    shutil.copyfile(args.input, args.output)
    update_env(args.output, data)
