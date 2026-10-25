import json
import sys
from pathlib import Path

import numpy as np
import torch


PROJECT_ROOT = Path(__file__).resolve().parents[1]

THIRD_PARTY = (
    PROJECT_ROOT / "third_party" / "ts-anomaly"
)

MODEL_DIR = (
    PROJECT_ROOT / "models" / "ai" / "skab_valve_v1"
)

CHECKPOINT = (
    THIRD_PARTY / "checkpoints" / "skab_hybrid.pt"
)

CONFIG = (
    THIRD_PARTY / "configs" / "default.yaml"
)


# 复用开源项目自己的数据处理和模型代码
sys.path.insert(0, str(THIRD_PARTY))

from src.config import load
from src.data.factory import build_loaders
from src.models.hybrid import HybridAnomaly


def main():
    cfg = load(CONFIG)

    # 避免测试程序创建额外 worker
    cfg.data.num_workers = 0

    _, val_loader, in_dim = build_loaders(cfg)

    # 直接从验证集取第一个真实窗口
    x, y = next(iter(val_loader))

    sample = (
        x[:1]
        .contiguous()
        .float()
    )

    label = int(y[0].item())

    if tuple(sample.shape) != (1, 64, 8):
        raise RuntimeError(
            f"Unexpected input shape: {tuple(sample.shape)}"
        )

    model = HybridAnomaly(
        in_dim,
        cfg.model.hidden,
        cfg.model.rnn_layers,
        cfg.model.heads,
        cfg.model.attn_layers,
        cfg.model.dropout
    )

    checkpoint = torch.load(
        CHECKPOINT,
        map_location="cpu",
        weights_only=True
    )

    model.load_state_dict(
        checkpoint["model"]
    )

    model.eval()

    with torch.inference_mode():
        output = model(sample)

        logit = float(
            output.reshape(-1)[0].item()
        )

        probability = float(
            torch.sigmoid(output)
            .reshape(-1)[0]
            .item()
        )

    input_array = (
        sample.cpu()
        .numpy()
        .astype(np.float32)
    )

    MODEL_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    # C++ 直接读取的二进制输入
    input_array.tofile(
        MODEL_DIR / "reference_input_f32.bin"
    )

    # 方便本地查看
    np.save(
        MODEL_DIR / "reference_input.npy",
        input_array
    )

    metadata = {
        "shape": list(input_array.shape),
        "dtype": "float32",
        "label": label,
        "checkpoint_epoch": int(
            checkpoint["epoch"]
        ),
        "pytorch_logit": logit,
        "pytorch_probability": probability,
    }

    with (
        MODEL_DIR / "reference_output.json"
    ).open("w", encoding="utf-8") as f:
        json.dump(
            metadata,
            f,
            indent=2
        )

    print("Reference generated")
    print("Shape:", input_array.shape)
    print("Label:", label)
    print("Checkpoint epoch:", checkpoint["epoch"])
    print("PyTorch logit:", logit)
    print("PyTorch probability:", probability)
    print("Saved to:", MODEL_DIR)


if __name__ == "__main__":
    main()