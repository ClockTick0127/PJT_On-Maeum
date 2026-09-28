# 저장소 내 실제 추론 자산

선정된 체크포인트 6개(총 5,069,170 bytes), 잠긴 프로파일, 정규화 통계,
모델 원본, raw panel을 포함한다. 별도 Drive/원격 서버 없이 CPU 추론할 수 있다.
`ASSET_MANIFEST.json`의 SHA256과 byte 크기로 배포 후 무결성을 확인한다.

- `weights/`: 실제 PyTorch 체크포인트. Git LFS 포인터가 아닌 파일 본체.
- `profiles/`: 선정 당시 잠긴 설정. 원본의 전송 상태 필드는 당시 기록이며 현재 상태는 ASSET_MANIFEST와 검증 명령으로 확인한다.
- `source_snapshot/m1/`: 모델/정규화의 동결 원본. 실행 모델 코드는 서비스 패키지에 동일 SHA로 탑재된다.
- `source_snapshot/r2_reference/`: 대조용 과거 구현. 이 폴더의 LocalSink replay_adapter는 실제 통신 실행에 사용하지 않는다.
- `fixtures/*/panel.csv`: prefix 254초를 포함하는 실제 raw P/Q/PF/I와 valid/context.
- `fixtures/*/evaluation_only/`: 정답, H200 기준 출력, 긴 prefix/OFF/다른 날짜 시험 입력. 추론 서비스와 데모 화면은 정답/기준 출력을 읽지 않는다.

6개 장면은 서로 다른 가정의 **개별 목표 가전** 시연이다. 선정 구간에 대한 구현 검증과
미선정 환경의 성능을 구분한다. 원본 checkpoint, threshold, normalization은 변경하지 않았다.
CSV/JSON 포함 모든 동결 파일은 `.gitattributes`로 개행 변환을 막는다.
