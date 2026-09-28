# 실제 모델 재실행

검토 ZIP을 승인된 기존 연구 서버에 풀고 패키지 루트에서 실행합니다. 아래 출력 경로는 아직 존재하지 않는 이름을 사용하세요.

```bash
/home/j-j15d201/conda-envs/nilm/bin/python reproduce/rerun_candidate.py candidates/candidate_008b808cf17505534886 --output new_runs/candidate_008b808cf17505534886_review_01 --device 0
```

원 서버의 가중치·모델 코드·정규화 및 simulator_source 경로가 필요하며 모두 해시를 확인합니다. 운영 브로커 없이 local transport로 실행합니다. 기존 결과를 덮어쓰지 않고 새 설정과 출력을 만듭니다. 정답·기대점수는 추론 중 읽지 않습니다. 추가 반복 시 출력 이름을 바꾸세요. 모델 채택은 pending입니다.
