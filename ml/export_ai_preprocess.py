import json
import sys
from pathlib import Path

import numpy as np

PROJECT_ROOT = Path(__file__).resolve().parents[1]

THIRD_PARTY = (
    PROJECT_ROOT
    / "third_party"
    / "ts-anomaly"
)

MODEL_DIR = (
    PROJECT_ROOT
    / "models"
    / "ai"
    / "skab_valve_v1"
)

sys.path.insert(0, str(THIRD_PARTY))

from src.config import load
from src.data.skab import SENSOR_COLS, load_skab_runs
from src.data.scaling import Standardizer


CONFIG_PATH = (
    THIRD_PARTY
    / "configs"
    / "default.yaml"
)


def main():
    cfg = load(CONFIG_PATH)

    # 与训练阶段完全相同：
    # scaler 只使用 train split 拟合。
    train_runs = load_skab_runs("train")

    train_values = np.concatenate(
        [
            run[SENSOR_COLS].to_numpy(dtype=np.float32)
            for run in train_runs
        ],
        axis=0,
    )

    scaler = Standardizer.fit(train_values)

    metadata = {
        "model_id": "skab_valve_v1",

        "input_shape": [
            1,
            int(cfg.data.window),
            len(SENSOR_COLS),
        ],

        "window_size": int(cfg.data.window),
        "stride": int(cfg.data.stride),

        # 顺序非常重要。
        # gatewayd 后续必须严格按照此顺序构造每一行。
        "features": list(SENSOR_COLS),

        "mean": [
            float(value)
            for value in scaler.mu
        ],

        "std": [
            float(value)
            for value in scaler.sd
        ],

        # 当前 epoch17 checkpoint
        # 在 validation 上选择出的最佳阈值。
        "threshold": 0.1505887508392334,

        "normalization": "standard",
    }

    MODEL_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    output_path = (
        MODEL_DIR
        / "preprocess.json"
    )

    with output_path.open(
        "w",
        encoding="utf-8",
    ) as file:
        json.dump(
            metadata,
            file,
            indent=2,
            ensure_ascii=False,
        )

    print("AI preprocessing metadata exported")
    print("Output:", output_path)
    print("Input shape:", metadata["input_shape"])
    print("Window:", metadata["window_size"])
    print("Stride:", metadata["stride"])
    print("Threshold:", metadata["threshold"])

    print("\nFeatures:")
    for index, name in enumerate(metadata["features"]):
        print(
            f"{index}: {name} "
            f"mean={metadata['mean'][index]:.8f} "
            f"std={metadata['std'][index]:.8f}"
        )


if __name__ == "__main__":
    main()