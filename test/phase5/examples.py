#!/usr/bin/env python3
import json
import pathlib

import jsonschema
import yaml

root = pathlib.Path(__file__).resolve().parents[2]
crd = yaml.safe_load((root / "src/manifests/crds/orchestration.gputpu.io_heterogeneousclusters.yaml").read_text())
schema = next(version for version in crd["spec"]["versions"] if version["name"] == "v1alpha1")["schema"]["openAPIV3Schema"]
example = yaml.safe_load((root / "src/manifests/workloads/example-cluster.yaml").read_text())
jsonschema.Draft202012Validator(schema).validate(example)

checkpoint_schema = json.loads((root / "docs/schemas/checkpoint-manifest-v2.schema.json").read_text())
checkpoint_example = json.loads((root / "docs/schemas/checkpoint-manifest-v2.example.json").read_text())
jsonschema.Draft202012Validator(checkpoint_schema).validate(checkpoint_example)
