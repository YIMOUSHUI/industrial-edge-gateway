import argparse
import json
import socket
import struct
import sys
import time
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]

THIRD_PARTY = (
    PROJECT_ROOT
    / "third_party"
    / "ts-anomaly"
)

sys.path.insert(0, str(THIRD_PARTY))

from src.data.skab import SENSOR_COLS, load_skab_runs


DEVICE_ID = "skab-valve-demo"


def send_all(sock: socket.socket, data: bytes) -> None:
    view = memoryview(data)

    while view:
        sent = sock.send(view)

        if sent <= 0:
            raise RuntimeError("TCP send failed")

        view = view[sent:]


def send_json_frame(
    sock: socket.socket,
    payload: dict,
) -> None:
    body = json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")

    # Gateway现有TCP协议：
    #
    # [4字节网络字节序Payload长度][JSON Payload]
    header = struct.pack(
        "!I",
        len(body),
    )

    send_all(
        sock,
        header + body,
    )


def choose_validation_run(runs):
    """
    优先选择包含异常标签的一个完整实验run。

    只发送一个run，
    不跨CSV拼接时序窗口。
    """
    for index, run in enumerate(runs):
        if "anomaly" not in run.columns:
            continue

        anomaly_count = int(
            run["anomaly"]
            .fillna(0)
            .astype(float)
            .sum()
        )

        if anomaly_count > 0:
            return index, run

    return 0, runs[0]


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Send one raw SKAB validation run "
            "to the industrial edge gateway."
        )
    )

    parser.add_argument(
        "--host",
        default="192.168.137.118",
        help="RK3576 gateway IP",
    )

    parser.add_argument(
        "--port",
        type=int,
        default=9000,
        help="Gateway TCP port",
    )

    parser.add_argument(
        "--interval-ms",
        type=float,
        default=5.0,
        help="Delay between two sensor samples",
    )

    args = parser.parse_args()

    runs = load_skab_runs("valid")

    if not runs:
        raise RuntimeError(
            "SKAB validation split is empty"
        )

    run_index, run = choose_validation_run(runs)

    missing = [
        name
        for name in SENSOR_COLS
        if name not in run.columns
    ]

    if missing:
        raise RuntimeError(
            "SKAB run missing features: "
            + ", ".join(missing)
        )

    has_label = (
        "anomaly" in run.columns
    )

    anomaly_count = 0

    if has_label:
        anomaly_count = int(
            run["anomaly"]
            .fillna(0)
            .astype(float)
            .sum()
        )

    print(
        f"Selected validation run: {run_index}"
    )

    print(
        f"Rows: {len(run)}"
    )

    print(
        f"Ground-truth anomaly rows: "
        f"{anomaly_count}"
    )

    print(
        "Device:",
        DEVICE_ID,
    )

    print(
        f"Target: {args.host}:{args.port}"
    )

    print(
        "Features:"
    )

    for index, name in enumerate(SENSOR_COLS):
        print(
            f"  {index}: {name}"
        )

    with socket.create_connection(
        (args.host, args.port),
        timeout=5.0,
    ) as sock:

        print("TCP connected")
        print("Sending raw SKAB sensor data...")

        for sequence, (_, row) in enumerate(
            run.iterrows(),
            start=1,
        ):
            metrics = []

            # 这里发送的是原始SKAB值。
            # 不在Python端标准化。
            for feature in SENSOR_COLS:
                metrics.append(
                    {
                        "name": feature,
                        "value": float(
                            row[feature]
                        ),
                        "unit": "",
                    }
                )

            # 测试阶段额外携带真实标签。
            #
            # AiAnomalyDetector不会把它作为模型输入，
            # 只是方便最终通过MQTT对比AI结果。
            if has_label:
                metrics.append(
                    {
                        "name": "skab_ground_truth",
                        "value": float(
                            row["anomaly"]
                        ),
                        "unit": "",
                    }
                )

            payload = {
                "device_id": DEVICE_ID,

                # 模型不使用timestamp。
                # 这里使用当前发送时间表示实时进入网关的时间。
                "timestamp_ms": (
                    time.time_ns()
                    // 1_000_000
                ),

                "sequence": sequence,

                "metrics": metrics,
            }

            send_json_frame(
                sock,
                payload,
            )

            if (
                sequence == 1
                or sequence % 100 == 0
                or sequence == len(run)
            ):
                print(
                    f"Sent "
                    f"{sequence}/{len(run)}"
                )

            if args.interval_ms > 0:
                time.sleep(
                    args.interval_ms
                    / 1000.0
                )

    print("SKAB run sent successfully")


if __name__ == "__main__":
    main()