"""Actual checkpoint inference on raw P/Q/PF/I windows, without fake fallback."""

from collections.abc import Sequence
from contextlib import nullcontext
import hashlib
import json
from pathlib import Path

from realtime_analysis.buffer import FeatureRow
from realtime_analysis.predictor import APPLIANCE_ORDER
from realtime_analysis.schemas import AppliancePrediction
from realtime_analysis.selected_scene import float32

MODEL_DIRECTORY = Path(__file__).with_name("real_models")


def load_profile(appliance: str) -> dict:
    """Use the reviewed lock shipped with this service, not editable asset profiles."""
    lock = json.loads((MODEL_DIRECTORY / "profiles.lock.json").read_text(encoding="utf-8"))
    for profile in lock["profiles"]:
        if profile["appliance_type"] == appliance.lower():
            return profile
    raise ValueError(f"No selected-scene profile for {appliance!r}")


def load_profiles() -> list[dict]:
    """Return profiles in the public six-appliance contract order."""

    return [load_profile(appliance) for appliance in APPLIANCE_ORDER]


def verified_asset(root: Path, relative: str, sha256: str) -> Path:
    path = (root / relative).resolve()
    if not path.is_relative_to(root.resolve()):
        raise ValueError("Asset path escapes asset root")
    if hashlib.sha256(path.read_bytes()).hexdigest() != sha256:
        raise ValueError(f"Asset SHA-256 mismatch: {relative}")
    return path


class SelectedScenePredictor:
    """Predict exactly one selected appliance; input is raw, never pre-normalized.

    Compatible with Predictor.predict's return type, but deliberately returns only
    the selected appliance. Legacy six-appliance handlers must not consume this
    until their UNKNOWN/not_inferred contracts are implemented.
    """

    def __init__(self, asset_root: str | Path, appliance: str, *,
                 device: str = "cpu", dtype: str = "float32") -> None:
        if dtype not in {"float32", "bfloat16"}:
            raise ValueError("dtype must be float32 or bfloat16")
        self.profile = load_profile(appliance)
        root = Path(asset_root).resolve()
        profile = self.profile
        checkpoint = verified_asset(root, profile["local_checkpoint_relative"],
                                    profile["checkpoint_sha256"])
        norm_path = verified_asset(root, profile["normalization_relative"],
                                   profile["normalization_sha256"])
        # The exact reviewed architecture is vendored; external Python is not executed.
        verified_asset(MODEL_DIRECTORY, "models.py", profile["model_code_sha256"])
        import numpy as np
        import torch
        from realtime_analysis.real_models.models import CausalModel

        self._np, self._torch = np, torch
        self.device = torch.device(device)
        if self.device.type not in {"cpu", "cuda"}:
            raise ValueError("Only explicit cpu or cuda devices are supported")
        self.dtype = dtype
        norm = json.loads(norm_path.read_text(encoding="utf-8"))
        if norm["features"] != ["P", "Q", "PF", "I"]:
            raise ValueError("Normalization feature order mismatch")
        self._mean = np.asarray(norm["mean"], dtype=np.float64)
        self._std = np.asarray(norm["std"], dtype=np.float64)
        if (self._mean.shape != (4,) or self._std.shape != (4,)
                or not np.isfinite(self._mean).all()
                or not np.isfinite(self._std).all() or (self._std <= 0).any()):
            raise ValueError("Invalid normalization parameters")
        self._model = CausalModel(profile["family"])
        bundle = torch.load(checkpoint, map_location="cpu", weights_only=True)
        self._model.load_state_dict(bundle[profile["checkpoint_state_dict_key"]], strict=True)
        self._model.to(self.device).eval()
        if self.device.type == "cuda":
            torch.backends.cuda.matmul.allow_tf32 = False
            torch.backends.cudnn.allow_tf32 = False
        self.runtime = {
            "profile_id": profile["profile_id"],
            "checkpoint_sha256": profile["checkpoint_sha256"],
            "model_code_sha256": profile["model_code_sha256"],
            "normalization_sha256": profile["normalization_sha256"],
            "torch_version": torch.__version__, "device": str(self.device),
            "dtype": dtype, "batch_size": 1, "input_shape": [1, 255, 4],
            "reference_device": profile["gpu_reference"],
            "reference_dtype": profile["dtype_reference"],
            "reference_parity_verified": False,
        }

    @property
    def missing_feature_row(self) -> FeatureRow:
        """Reference mean-fill: normalizes to four zeroes on a missing second."""
        return tuple(float(value) for value in self._mean)

    def predict(self, window: Sequence[FeatureRow]) -> list[AppliancePrediction]:
        np, torch = self._np, self._torch
        raw = np.asarray(window, dtype=np.float64)
        if raw.shape != (255, 4) or not np.isfinite(raw).all():
            raise ValueError("Expected 255 finite raw P/Q/PF/I rows")
        # Match reference: double precision normalization, then one float32 cast.
        normalized = ((raw - self._mean) / self._std).astype(np.float32)
        if not np.isfinite(normalized).all():
            raise ValueError("Non-finite normalized input")
        tensor = torch.from_numpy(normalized).unsqueeze(0).to(self.device)
        autocast = (torch.autocast(self.device.type, dtype=torch.bfloat16)
                    if self.dtype == "bfloat16" else nullcontext())
        with torch.inference_mode(), autocast:
            # Model.forward starts recurrent hidden state afresh for each window.
            logits = self._model(tensor)
            if logits.shape != (1,) or not torch.isfinite(logits).all():
                raise ValueError("Expected one finite model logit")
            probability = torch.sigmoid(logits.float()).item()
        return [AppliancePrediction(appliance_type=self.profile["appliance_type"].upper(),
                                    probability=probability)]


class RealtimeModelPredictor:
    """Run all reviewed appliance checkpoints for the realtime contract.

    Each checkpoint remains an independent, stateless one-target model. This
    adapter only combines their six probabilities in ``APPLIANCE_ORDER``; it
    never replaces a failed checkpoint with fake output.
    """

    window_size = 255

    def __init__(self, asset_root: str | Path, *, device: str = "cpu",
                 dtype: str = "float32") -> None:
        self._predictors = tuple(
            SelectedScenePredictor(
                asset_root,
                appliance,
                device=device,
                dtype=dtype,
            )
            for appliance in APPLIANCE_ORDER
        )
        actual_order = tuple(
            predictor.profile["appliance_type"].upper()
            for predictor in self._predictors
        )
        if actual_order != APPLIANCE_ORDER:
            raise ValueError("Real model profile order does not match service contract")

        window_sizes = {
            int(predictor.profile["fixed_window_steps"])
            for predictor in self._predictors
        }
        if window_sizes != {self.window_size}:
            raise ValueError(f"Real model window mismatch: {sorted(window_sizes)}")

        confirmations = {
            int(predictor.profile["threshold"]["confirm"])
            for predictor in self._predictors
        }
        if len(confirmations) != 1:
            raise ValueError(
                "Realtime pipeline requires one shared confirmation count"
            )
        self.confirmation_samples = confirmations.pop()
        self.on_thresholds = {
            predictor.profile["appliance_type"].upper(): float32(
                predictor.profile["threshold"]["on"]
            )
            for predictor in self._predictors
        }
        self.off_thresholds = {
            predictor.profile["appliance_type"].upper(): float32(
                predictor.profile["threshold"]["off"]
            )
            for predictor in self._predictors
        }
        version_material = ":".join(
            predictor.profile["checkpoint_sha256"]
            for predictor in self._predictors
        ).encode("ascii")
        self.model_version = (
            f"nilm-r3-{hashlib.sha256(version_material).hexdigest()[:12]}"
        )

    def predict(self, window: Sequence[FeatureRow]) -> list[AppliancePrediction]:
        if len(window) != self.window_size:
            raise ValueError(
                f"Expected {self.window_size} realtime model input rows"
            )
        return [
            predictor.predict(window)[0]
            for predictor in self._predictors
        ]
