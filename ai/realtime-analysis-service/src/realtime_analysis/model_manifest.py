"""Model manifest schema and loader for the realtime analysis service."""

import json
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from realtime_analysis.predictor import APPLIANCE_ORDER

FEATURE_ORDER = (
    "active_power",
    "reactive_power",
    "power_factor",
    "current",
)


class InputFeature(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)

    index: int = Field(ge=0)
    name: str
    unit: str | None
    mean: float
    std: float = Field(gt=0)

class ModelInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    dtype: Literal["float32"]
    shape: tuple[int, int, int]
    sample_hz: Literal[1]
    time_order: Literal["oldest_to_latest"]
    features: list[InputFeature]

    @model_validator(mode="after")
    def input_contract_must_match(self) -> "ModelInput":
        if self.shape != (-1, 4, 299):
            raise ValueError("input shape must be [-1, 4, 299]")

        actual = tuple(
            feature.name
            for feature in sorted(self.features, key=lambda feature: feature.index)
        )
        indices = tuple(sorted(feature.index for feature in self.features))
        if indices != tuple(range(len(FEATURE_ORDER))) or actual != FEATURE_ORDER:
            raise ValueError(
                "input features must follow active_power, reactive_power, "
                "power_factor, current order"
            )
        return self


class Normalization(BaseModel):
    model_config = ConfigDict(extra="forbid")

    method: Literal["standardization"]
    formula: Literal["(x - mean) / std"]
    applied_by: Literal["realtime-analysis-service"]


class Preprocessing(BaseModel):
    model_config = ConfigDict(extra="forbid")

    normalization: Normalization


class OutputAppliance(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)

    index: int = Field(ge=0)
    appliance_type: str
    threshold: float = Field(ge=0, le=1)


class ModelOutput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    dtype: Literal["float32"]
    shape: tuple[int, int]
    activation: Literal["sigmoid"]
    appliances: list[OutputAppliance]

    @model_validator(mode="after")
    def output_contract_must_match(self) -> "ModelOutput":
        if self.shape != (-1, 6):
            raise ValueError("output shape must be [-1, 6]")

        actual = tuple(
            appliance.appliance_type
            for appliance in sorted(
                self.appliances,
                key=lambda appliance: appliance.index,
            )
        )
        indices = tuple(sorted(appliance.index for appliance in self.appliances))
        if indices != tuple(range(len(APPLIANCE_ORDER))) or actual != APPLIANCE_ORDER:
            raise ValueError("output appliances do not match APPLIANCE_ORDER")
        return self


class ModelManifest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    model_name: str = Field(min_length=1)
    version: str = Field(min_length=1)
    framework: Literal["pytorch"]
    input: ModelInput
    preprocessing: Preprocessing
    output: ModelOutput

    @classmethod
    def from_json_file(cls, path: str | Path) -> "ModelManifest":
        with Path(path).open(encoding="utf-8") as file:
            return cls.model_validate(json.load(file))

    @property
    def thresholds(self) -> dict[str, float]:
        return {
            appliance.appliance_type: appliance.threshold
            for appliance in self.output.appliances
        }

    @property
    def normalization_parameters(self) -> tuple[tuple[float, float], ...]:
        """Feature 순서대로 학습 데이터의 mean과 std를 반환한다."""

        return tuple(
            (feature.mean, feature.std)
            for feature in sorted(
                self.input.features,
                key=lambda feature: feature.index,
            )
        )
