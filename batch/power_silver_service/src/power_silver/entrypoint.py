"""spark-submit에 넘길 파일.

콘솔 스크립트(``power-silver``)는 드라이버를 로컬에서 띄울 때 쓰고, 클러스터에 제출할
때는 이 파일을 넘긴다.

    spark-submit --master spark://<host>:7077 entrypoint.py run --date 2026-09-19
"""

import sys

from power_silver.cli import main


if __name__ == "__main__":
    sys.exit(main())
