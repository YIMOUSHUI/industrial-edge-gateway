
from pathlib import Path

from rknn.api import RKNN


# 项目根目录
PROJECT_ROOT = Path(__file__).resolve().parents[1]

MODEL_DIR = PROJECT_ROOT / "models/ai/skab_valve_v1"

ONNX_MODEL = MODEL_DIR / "skab_hybrid_static.onnx"
RKNN_MODEL = MODEL_DIR / "skab_hybrid.rknn"


def check_result(ret, operation):
    if ret != 0:
        raise RuntimeError(
            f"{operation} failed, return code: {ret}"
        )


def main():
    if not ONNX_MODEL.is_file():
        raise FileNotFoundError(ONNX_MODEL)

    rknn = RKNN(verbose=True)

    try:
        # 1. 指定 RK3576
        print("[1/4] Configuring RK3576")

        ret = rknn.config(
            target_platform="rk3576"
        )
        check_result(ret, "rknn.config")

        # 2. 加载已导出的 ONNX
        print("[2/4] Loading ONNX")

        ret = rknn.load_onnx(
            model=str(ONNX_MODEL)
        )
        check_result(ret, "rknn.load_onnx")

        # 3. 构建 RKNN 模型
        # 第一轮不进行 INT8 量化，减少干扰因素
        print("[3/4] Building RKNN")

        ret = rknn.build(
            do_quantization=False
        )
        check_result(ret, "rknn.build")

        # 4. 导出 RKNN
        print("[4/4] Exporting RKNN")

        ret = rknn.export_rknn(
            str(RKNN_MODEL)
        )
        check_result(ret, "rknn.export_rknn")

        print("Conversion succeeded:", RKNN_MODEL)

    finally:
        rknn.release()


if __name__ == "__main__":
    main()
