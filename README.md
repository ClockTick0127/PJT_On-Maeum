# On:마음 | 안심 알림

가전 전력 사용 패턴으로 홀로 사는 어르신의 이상 징후를 감지하고, 담당자와 대상자에게 실시간으로 알리는 서비스입니다.
클램프 센서(CT)로 측정한 전력 파형만으로 가전별 ON/OFF를 판별하고(NILM), 평소 생활 패턴과 오늘의 사용을 비교해 위험을 판단합니다.

> 로컬 실행·배포 방법과 Git 컨벤션은 [깃플로우및실행안내.md](깃플로우및실행안내.md)에 있습니다.

<br />

## 주요 기능

### 1. 평소 생활 패턴과 오늘 전력 사용 비교
평소 기록된 전자레인지 사용 파형을 재생하고, 같은 시간대 오늘 측정값을 실시간 MQTT로 발행해 두 패턴을 나란히 비교합니다.

![평소 생활 패턴과 오늘 전력 사용 비교](docs/readme/01-routine-simulator.gif)

### 2. 위험 감지 시 담당자 대시보드 실시간 알림
분석 서비스가 평소와 다른 사용을 감지하면 담당자 대시보드에 위험 배너와 새 알림이 즉시 뜨고, 대상자의 위험 단계와 점수가 갱신됩니다.

![위험 감지 시 담당자 대시보드 실시간 알림](docs/readme/02-risk-alert.gif)

### 3. 대상자에게 안전 확인 요청
위험이 감지되면 대상자 화면과 브라우저 푸시로 "지금 상태를 알려주세요" 안전 확인 요청을 보내고, 대상자는 '도움이 필요해요' 또는 '괜찮아요'로 응답합니다.

![대상자에게 안전 확인 요청](docs/readme/03-safety-check.gif)

### 4. 도움 요청 즉시 담당자에게 전달
대상자가 '도움이 필요해요'를 누르면 담당자 대시보드에 도움 요청 알림과 배지가 바로 표시되어 담당자가 대상자 확인·해결 처리를 할 수 있습니다.

![도움 요청 즉시 담당자에게 전달](docs/readme/04-help-request.gif)

<br />

## 시스템 아키텍처

![시스템 아키텍처](docs/readme/architecture.webp)

<br />

## ERD

서비스마다 DB가 분리되어 있어 서비스 간에는 FK가 없습니다. 실선은 FK, 점선은 id로만 잇는 논리적 참조입니다.
가구는 `house_id`(= 다른 서비스의 `household_id`), 사용자는 Keycloak `sub`(`keycloak_user_id` / `auth_sub`)로 서비스 사이를 연결합니다.

### 기기 인증 서비스 (iot-device-service · `device_db`)
가구·구성원·초대와 기기, 기기별 MQTT 인증 정보를 관리합니다.

```mermaid
erDiagram
    households ||--o{ household_members : "구성원"
    households ||--o{ household_invites : "초대 코드"
    households ||--o{ devices : "설치 기기"
    user_profiles ||..o{ household_members : "keycloak_user_id"
    user_profiles ||..o| manager_registration_outbox : "담당자 등록"
    devices ||--o{ install_history : "설치 이력"
    devices ||--|| device_credentials : "MQTT 인증"
    devices ||--o{ device_acl : "토픽 권한"

    households {
        string house_id PK
    }
    household_members {
        string house_id PK,FK
        uuid keycloak_user_id PK
    }
    household_invites {
        string code PK
        string house_id FK
    }
    user_profiles {
        uuid keycloak_user_id PK
    }
    devices {
        bigint device_id PK
        string house_id FK
    }
    install_history {
        bigint history_id PK
        bigint device_id FK
    }
    device_credentials {
        bigint device_id PK,FK
    }
    device_acl {
        bigint acl_id PK
        bigint device_id FK
    }
    manager_registration_outbox {
        uuid event_id PK
        uuid keycloak_user_id
    }
```

### 모니터링 서비스 (monitoring-service · `monitoring_db`)
담당자·대상자, 분석 이벤트와 알림, 위험 평가, 배치로 받은 가구 프로필을 관리합니다.

```mermaid
erDiagram
    managers ||--o{ subjects : "담당"
    managers ||--o{ notification_settings : "알림 설정"
    subjects ||--o{ analysis_events : "분석 이벤트"
    subjects ||..o{ risk_assessments : "위험 평가"
    subjects ||..o{ notifications : "알림"
    analysis_events ||..o{ notifications : "event_id"
    risk_assessments ||..o| notifications : "assessment_id"
    subjects ||..o{ push_subscriptions : "auth_sub"
    subjects ||..o{ household_profiles : "household_id"
    household_profiles ||--o{ household_routine_baselines : "루틴 기준선"
    household_profiles ||--o{ household_profile_statistics : "지표 통계"

    managers {
        bigint id PK
        string auth_sub
    }
    subjects {
        bigint id PK
        bigint manager_id FK
        string household_id
    }
    analysis_events {
        uuid id PK
        bigint subject_id FK
    }
    notifications {
        bigint id PK
        uuid event_id
        bigint subject_id
        uuid assessment_id
    }
    notification_settings {
        bigint id PK
        bigint manager_id FK
    }
    push_subscriptions {
        bigint id PK
        string auth_sub
    }
    risk_assessments {
        uuid id PK
        bigint subject_id
    }
    household_profiles {
        bigint id PK
        string household_id
    }
    household_routine_baselines {
        bigint id PK
        bigint profile_id FK
    }
    household_profile_statistics {
        bigint id PK
        bigint profile_id FK
    }
```

> 이 외에 `household_id` 기준 집계 테이블(`appliance_states`, `appliance_usage_episodes`, `hourly_power_usage`, `hourly_appliance_power_usage`, `household_daily_appliance_usage`, `household_observations`, `household_device_connectivity`)과 Gold 배치가 쓰는 `reporting` 스키마(`report_runs` 1:N `report_rows`)가 있습니다.

### 실시간 패턴 분석 서비스 (realtime-analysis-service · `analysis_db`)
가전 ON/OFF 세션과 일별 관측, 이벤트 발행 기록, 데이터 레이크 적재·배치 실행 이력을 관리합니다.

```mermaid
erDiagram
    household_observation_daily ||--o{ household_activity_daily : "가전별 활동"
    household_activity_daily ||--o{ appliance_usage_session : "사용 세션"
    appliance_usage_session ||..o{ session_lake_outbox : "session_id"
    session_lake_batch ||--o{ session_lake_outbox : "적재 배치"
    analysis_processing_receipt ||--|| analysis_receipt_lake_outbox : "처리 영수증"
    analysis_receipt_lake_batch ||--o{ analysis_receipt_lake_outbox : "적재 배치"
    lake_batch_run ||--o{ lake_dataset_version : "데이터셋 버전"
    lake_batch_run ||--o{ lake_dataset_dependency : "선행 의존"
    batch_run ||--o{ batch_step_run : "단계 실행"

    household_observation_daily {
        uuid id PK
        string household_id
    }
    household_activity_daily {
        uuid id PK
        uuid observation_daily_id FK
    }
    appliance_usage_session {
        uuid id PK
        uuid activity_daily_id FK
    }
    session_lake_batch {
        uuid batch_id PK
    }
    session_lake_outbox {
        bigint event_id PK
        uuid batch_id FK
        uuid session_id
    }
    analysis_processing_receipt {
        uuid receipt_id PK
        string household_id
    }
    analysis_receipt_lake_batch {
        uuid batch_id PK
    }
    analysis_receipt_lake_outbox {
        bigint event_id PK
        uuid receipt_id FK
        uuid batch_id FK
    }
    lake_batch_run {
        uuid run_id PK
    }
    lake_dataset_version {
        uuid version_id PK
        uuid run_id FK
    }
    lake_dataset_dependency {
        uuid dependency_id PK
        uuid consumer_run_id FK
    }
    batch_run {
        uuid run_id PK
    }
    batch_step_run {
        uuid step_run_id PK
        uuid run_id FK
    }
```

> 독립 테이블: `routine_baseline`, `analysis_policy`, `model_artifact`, `analysis_event_emission`, `household_outing_state`, `analysis_daily_completion`, `gold_profile_delivery_outbox`, `selected_scene_*`

### 서비스 간 연결

```mermaid
flowchart LR
    subgraph device_db
        D1[households.house_id]
        D2[devices.device_id]
        D3[user_profiles.keycloak_user_id]
    end
    subgraph monitoring_db
        M1[subjects.household_id]
        M2[managers.auth_sub]
        M3[analysis_events.id]
        M4[household_profiles]
    end
    subgraph analysis_db
        A1[analysis_event_emission.event_id]
        A2[gold_profile_delivery_outbox]
        A3[analysis_processing_receipt.device_id]
    end
    D1 -.-> M1
    D3 -. "Kafka 담당자 등록" .-> M2
    D2 -.-> A3
    A1 -. "Kafka analysis.*" .-> M3
    A2 -. "Kafka 프로필 전달" .-> M4
```
