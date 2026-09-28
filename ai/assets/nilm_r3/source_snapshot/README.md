# 참조 코드의 상태

m1/models.py와 normalization.json은 보존 M1 ZIP에서 추출했으며 여섯 R2 model_ref의 SHA와 일치한다. r2_reference의 원본 코드는 R2_01 ZIP에서 그대로 복사했다.

**자동 실행하지 않는다.** 특히 replay_adapter.py는 aiomqtt shim + LocalSink로 의도적으로 외부 통신을 막는다. 원본 서버 절대경로·CUDA를 가정한다. 모델/checkpoint/정규화와 원 state semantics의 기준으로 읽고, 실제 팀 코드와 로컬 환경에 맞춘 새 adapter를 별도로 작성한다. 원본을 수정해 expected SHA를 바꾸지 않는다.

가중치 .pt는 이 폴더에 없다. hash 검증을 끝낸 승인된 여섯 파일만 로드한다. 소스와 .pt를 공유했다고 실사용자 데이터·서버 인증을 함께 공유하지 않는다.
