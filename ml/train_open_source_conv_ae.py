
import csv
import inspect
import json
import sys
from pathlib import Path

import numpy as np
import torch
import onnx


# =========================================================
# 1. 路径与实验配置
# =========================================================

LAB_DIR = Path.home() / "linux/rknn-m4-lab"

REPO_DIR = (
    LAB_DIR
    / "third_party/skab-anomaly-detection-pytorch-main"
)

CSV_FILE = REPO_DIR / "data/valve1/0.csv"

OUTPUT_DIR = (
    LAB_DIR / "experiments/conv_ae_valve1_0"
)

WINDOW_SIZE = 60
TRAIN_ROWS = 400
SEED = 42


# =========================================================
# 2. 导入开源模型，不修改原始网络
# =========================================================

if not (REPO_DIR / "core/Conv_AE_pytorch.py").is_file():
    raise FileNotFoundError(
        f"Open-source model not found: {REPO_DIR}"
    )

sys.path.insert(0, str(REPO_DIR))

from core.Conv_AE_pytorch import Conv_AE


# =========================================================
# 3. 将连续采样构造成时序窗口
# =========================================================

def create_windows(values):
    if len(values) < WINDOW_SIZE:
        raise ValueError("Not enough samples")

    return np.stack([
        values[i:i + WINDOW_SIZE]
        for i in range(len(values) - WINDOW_SIZE + 1)
    ]).astype(np.float32)


def calculate_metrics(y, pred):
    tp = int(np.sum((y == 1) & (pred == 1)))
    tn = int(np.sum((y == 0) & (pred == 0)))
    fp = int(np.sum((y == 0) & (pred == 1)))
    fn = int(np.sum((y == 1) & (pred == 0)))

    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    fpr = fp / (fp + tn) if fp + tn else 0.0

    f1 = (
        2 * precision * recall / (precision + recall)
        if precision + recall else 0.0
    )

    return {
        "TP": tp,
        "TN": tn,
        "FP": fp,
        "FN": fn,
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "fpr": fpr,
    }


# =========================================================
# 4. 主程序
# =========================================================

def main():
    torch.manual_seed(SEED)
    np.random.seed(SEED)
    torch.set_num_threads(4)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # ---------- 4.1 读取作者仓库中的真实 CSV ----------

    with CSV_FILE.open(
        "r", encoding="utf-8-sig", newline=""
    ) as f:
        reader = csv.DictReader(f, delimiter=";")

        columns = reader.fieldnames or []

        features = [
            name for name in columns
            if name not in {
                "datetime", "anomaly", "changepoint"
            }
        ]

        rows = list(reader)

    if len(features) != 8:
        raise ValueError(
            f"Expected 8 features, found: {features}"
        )

    values = np.asarray([
        [float(row[name]) for name in features]
        for row in rows
    ], dtype=np.float64)

    labels = np.asarray([
        int(float(row["anomaly"]))
        for row in rows
    ], dtype=np.int64)

    if not np.isfinite(values).all():
        raise ValueError("Invalid sensor values")

    if len(values) < TRAIN_ROWS + WINDOW_SIZE:
        raise ValueError("Not enough test samples")

    if np.any(labels[:TRAIN_ROWS] != 0):
        raise ValueError(
            "Training segment contains anomalies"
        )

    # ---------- 4.2 按作者方法标准化 ----------

    # 与 StandardScaler 相同：训练集均值和总体标准差
    mean = values[:TRAIN_ROWS].mean(axis=0)
    scale = values[:TRAIN_ROWS].std(axis=0)

    scale = np.where(scale < 1e-12, 1.0, scale)

    normalized = (
        (values - mean) / scale
    ).astype(np.float32)

    # ---------- 4.3 生成 60×8 窗口 ----------

    train_x = create_windows(
        normalized[:TRAIN_ROWS]
    )

    test_x = create_windows(
        normalized[TRAIN_ROWS:]
    )

    # 第一个测试窗口对应测试段第60个采样
    test_y = labels[
        TRAIN_ROWS + WINDOW_SIZE - 1:
    ]

    print("Features:", features)
    print("Train:", train_x.shape)
    print("Test:", test_x.shape)

    # ---------- 4.4 调用作者原始训练函数 ----------

    model = Conv_AE(
        lr=1e-3,
        batch_size=128,
        epochs=40
    )

    model.fit(train_x)
    model.model.eval()

    # ---------- 4.5 保存 PyTorch 权重 ----------

    torch.save(
        model.model.state_dict(),
        OUTPUT_DIR / "conv_ae_valve1_0.pth"
    )

    # ---------- 4.6 使用作者的残差评分公式 ----------

    train_reconstructed = model.predict(train_x)

    train_scores = (
        np.abs(train_x - train_reconstructed)
        .mean(axis=1)
        .sum(axis=1)
    )

    threshold = float(
        np.quantile(train_scores, 0.999) * 4.0 / 3.0
    )

    test_reconstructed = model.predict(test_x)

    test_scores = (
        np.abs(test_x - test_reconstructed)
        .mean(axis=1)
        .sum(axis=1)
    )

    raw_predictions = (
        test_scores > threshold
    ).astype(np.int64)

    # ---------- 4.7 连续异常确认 ----------
    # 连续60个窗口超过阈值才产生确认结果

    confirmed = np.zeros_like(raw_predictions)
    consecutive = 0

    for i, alarm in enumerate(raw_predictions):
        consecutive = consecutive + 1 if alarm else 0
        confirmed[i] = int(
            consecutive >= WINDOW_SIZE
        )

    raw_metrics = calculate_metrics(
        test_y, raw_predictions
    )

    confirmed_metrics = calculate_metrics(
        test_y, confirmed
    )

    # ---------- 4.8 保存预处理与评估参数 ----------

    metadata = {
        "source": "valve1/0.csv",
        "model": "upstream Conv_AE_pytorch.py",
        "features": features,
        "window_size": WINDOW_SIZE,
        "train_rows": TRAIN_ROWS,
        "mean": mean.tolist(),
        "scale": scale.tolist(),
        "score_type": "sum_feature_mean_time_abs_error",
        "threshold": threshold,
        "raw_metrics": raw_metrics,
        "confirmed_60_metrics": confirmed_metrics
    }

    with (
        OUTPUT_DIR / "preprocess_and_threshold.json"
    ).open("w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2)

    # ---------- 4.9 保存 RKNN 一致性测试样本 ----------

    # [1, 60, 8] -> [1, 8, 60]
    sample_input = np.ascontiguousarray(
        test_x[:1].transpose(0, 2, 1)
    )

    sample_output = np.ascontiguousarray(
        test_reconstructed[:1].transpose(0, 2, 1)
    )

    np.save(
        OUTPUT_DIR / "sample_input_nct.npy",
        sample_input
    )

    np.save(
        OUTPUT_DIR / "sample_output_nct.npy",
        sample_output
    )

    # ---------- 4.10 导出 ONNX ----------

    input_tensor = torch.from_numpy(sample_input)

    onnx_file = OUTPUT_DIR / "conv_ae_valve1_0.onnx"

    export_options = {
        "input_names": ["input"],
        "output_names": ["reconstruction"],
        "opset_version": 12,
    }

    # 兼容新旧 PyTorch 的 ONNX 导出接口
    if "dynamo" in inspect.signature(
        torch.onnx.export
    ).parameters:
        export_options["dynamo"] = False

    torch.onnx.export(
        model.model,
        input_tensor,
        str(onnx_file),
        **export_options
    )

    # ---------- 4.11 验证 ONNX 文件结构 ----------

    onnx_model = onnx.load(str(onnx_file))
    onnx.checker.check_model(onnx_model)

    print()
    print("Training and export completed")
    print("Threshold:", threshold)
    print("Raw metrics:", raw_metrics)
    print("Confirmed metrics:", confirmed_metrics)
    print("ONNX:", onnx_file)
    print("Saved to:", OUTPUT_DIR)


if __name__ == "__main__":
    main()
