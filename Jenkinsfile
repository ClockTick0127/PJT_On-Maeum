// Sends a Discord embed from the node that is already running (no new executor).
// result: 'started' | 'success' | 'failure'
def sendDiscord(String result) {
    withCredentials([string(credentialsId: 'discord-webhook', variable: 'DISCORD_WEBHOOK')]) {
        withEnv(["BUILD_RESULT=${result}",
                 "BUILD_DURATION=${currentBuild.durationString.replace(' and counting', '')}"]) {
            sh '''
                python3 - <<'EOF'
import json
import os
import urllib.request

env = os.environ
result = env["BUILD_RESULT"]
started = result == "started"
ok = result == "success"
deploy = env.get("DEPLOY") == "true"
sha = env.get("IMAGE_TAG", "")[:7] or "unknown"
subject = env.get("GIT_COMMIT_SUBJECT") or "-"
author = env.get("GIT_AUTHOR_NAME") or "-"
build_url = env.get("BUILD_URL") or ""

if started:
    title = "🚀 배포 시작" if deploy else "🔨 빌드 시작"
    note = ("> ⏳ 빌드·테스트 후 EC2에 배포합니다. 완료되면 결과를 다시 알립니다." if deploy
            else "> ⏳ 빌드 및 테스트를 진행합니다. 완료되면 결과를 다시 알립니다.")
    color = 0x3498DB
elif ok and deploy:
    title = "✅ 배포 성공"
    note = "> ✨ 최신 변경 사항이 서버에 정상적으로 배포되었습니다."
    color = 0x28A745
elif ok:
    title = "✅ 빌드 성공"
    note = "> ✨ 빌드 및 테스트가 정상적으로 통과했습니다."
    color = 0x28A745
else:
    title = "🚨 배포 실패" if deploy else "🚨 빌드 실패"
    note = ("> ⚠️ 빌드 도중 에러가 발생하여 배포가 중단되었습니다. Jenkins 콘솔 로그를 확인하세요." if deploy
            else "> ⚠️ 빌드 도중 에러가 발생했습니다. Jenkins 콘솔 로그를 확인하세요.")
    color = 0xDC3545

lines = [
    f"* **작업자:** `{author}`",
    f"* **커밋:** `{sha}` - {subject}",
]
if not started:
    lines.append(f"* **소요 시간:** {env.get('BUILD_DURATION') or '-'}")
if build_url:
    lines.append(f"* **Jenkins:** {build_url}")
lines += ["", note]

payload = {"embeds": [{"title": title, "description": chr(10).join(lines), "color": color}]}
request = urllib.request.Request(
    env["DISCORD_WEBHOOK"], data=json.dumps(payload).encode(),
    headers={"Content-Type": "application/json", "User-Agent": "nilm-jenkins/1.0"})
urllib.request.urlopen(request, timeout=20).read()
EOF
            '''
        }
    }
}

// For post{} blocks, which run outside any stage agent and therefore need a node.
def notifyDiscord(String result) {
    node('ci') {
        sendDiscord(result)
    }
}

pipeline {
    agent none
    options {
        disableConcurrentBuilds()
        skipDefaultCheckout(true)
        timestamps()
        timeout(time: 60, unit: 'MINUTES')
    }
    parameters {
        booleanParam(name: 'ENABLE_CD', defaultValue: true,
            description: 'master 배포 활성화. master 푸시로 시작된 자동 빌드는 이 값과 무관하게 항상 배포하며, 수동 빌드(Build with Parameters)에서만 해제할 수 있다')
        string(name: 'IMAGE_REPOSITORY', defaultValue: 'docker.io/leejeongmin24/on-maum',
            description: 'Docker Hub Private repository: docker.io/account/repository (no tag)')
        string(name: 'FRONTEND_ENV_CREDENTIAL', defaultValue: 'frontend-build-env',
            description: 'Secret file credential for public Vite build settings. master 빌드에만 주입되며, 비우면 프론트가 공개 변수 없이 빌드된다')
    }
    stages {
        stage('CI') {
            agent { label 'ci' }
            steps {
                script {
                    // Automatic master builds (webhook push or branch scan) always deploy, even on the
                    // first run after a parameter default changes. Only a person starting the build
                    // via "Build with Parameters" can opt out by unchecking ENABLE_CD.
                    boolean manual = currentBuild.getBuildCauses().any { it._class?.endsWith('UserIdCause') }
                    env.DEPLOY = (env.BRANCH_NAME == 'master' && (params.ENABLE_CD || !manual)) ? 'true' : 'false'
                }
                checkout scm
                script {
                    env.IMAGE_TAG = sh(script: 'git rev-parse HEAD', returnStdout: true).trim()
                    env.GIT_AUTHOR_NAME = sh(script: 'git log -1 --pretty=%an', returnStdout: true).trim()
                    env.GIT_COMMIT_SUBJECT = sh(script: 'git log -1 --pretty=%s', returnStdout: true).trim()
                    env.RELEASE_ID = "${env.IMAGE_TAG}-b${env.BUILD_NUMBER}"
                    env.IMAGE_REPOSITORY = params.IMAGE_REPOSITORY?.trim() ?: 'docker.io/leejeongmin24/on-maum'
                    if (!(env.IMAGE_REPOSITORY ==~ /docker\.io\/[a-z0-9]+(?:[._-][a-z0-9]+)*\/[a-z0-9]+(?:[._-][a-z0-9]+)*/)) {
                        error('IMAGE_REPOSITORY must be docker.io/account/repository without a tag')
                    }
                    // Start notification: sent after checkout so commit/author/deploy mode are known.
                    // Runs on the current 'ci' node; must not allocate another executor here.
                    sendDiscord('started')
                }
                sh '''
                    RUN_COMPOSE_TESTS=1 python3 -m unittest discover -s infrastructure/scripts/tests -v
                    for target in local ec2-a ec2-b; do
                        docker compose --env-file infrastructure/$target/.env.example -f infrastructure/$target/compose.yaml --profile '*' config --quiet
                    done
                '''
                script {
                    // Feature/PR builds never receive production credentials.
                    if (env.BRANCH_NAME == 'master' && params.FRONTEND_ENV_CREDENTIAL.trim()) {
                        withCredentials([file(credentialsId: params.FRONTEND_ENV_CREDENTIAL.trim(), variable: 'FRONTEND_ENV')]) {
                            sh 'bash infrastructure/scripts/build.sh'
                        }
                    } else {
                        sh 'bash infrastructure/scripts/build.sh'
                    }
                }
                stash name: 'deploy-config', includes: 'infrastructure/ec2-*/compose.yaml,infrastructure/ec2-b/hdfs-init.sh,infrastructure/ec2-b/gold-daily.env.example,infrastructure/systemd/*,infrastructure/observability/**,infrastructure/scripts/**,infrastructure/nginx/*.template,infrastructure/keycloak/*.json,infrastructure/postgres/*.sql,infrastructure/mqtt/config/mosquitto.production.conf', excludes: '**/__pycache__/**'
                script {
                    if (env.DEPLOY == 'true') {
                        withCredentials([usernamePassword(credentialsId: 'registry-login',
                            usernameVariable: 'REGISTRY_USER', passwordVariable: 'REGISTRY_PASSWORD')]) {
                            sh 'bash infrastructure/scripts/with-registry.sh bash infrastructure/scripts/push.sh'
                        }
                        stash name: 'release-manifest', includes: 'release.json'
                        archiveArtifacts artifacts: 'release.json', fingerprint: true
                    }
                }
            }
        }
        stage('Pull A images') {
            when { beforeAgent true; allOf { branch 'master'; expression { env.DEPLOY == 'true' } } }
            agent { label 'ec2-a' }
            steps {
                unstash 'deploy-config'
                unstash 'release-manifest'
                withCredentials([usernamePassword(credentialsId: 'registry-login',
                    usernameVariable: 'REGISTRY_USER', passwordVariable: 'REGISTRY_PASSWORD')]) {
                    sh 'bash infrastructure/scripts/with-registry.sh python3 infrastructure/scripts/release.py pull --target a'
                }
            }
        }
        stage('Pull B images') {
            when { beforeAgent true; allOf { branch 'master'; expression { env.DEPLOY == 'true' } } }
            agent { label 'ec2-b' }
            steps {
                unstash 'deploy-config'
                unstash 'release-manifest'
                withCredentials([usernamePassword(credentialsId: 'registry-login',
                    usernameVariable: 'REGISTRY_USER', passwordVariable: 'REGISTRY_PASSWORD')]) {
                    sh 'bash infrastructure/scripts/with-registry.sh python3 infrastructure/scripts/release.py pull --target b'
                }
            }
        }
        stage('Prepare A') {
            when { beforeAgent true; allOf { branch 'master'; expression { env.DEPLOY == 'true' } } }
            agent { label 'ec2-a' }
            steps {
                unstash 'deploy-config'
                withCredentials([file(credentialsId: 'ec2-a-runtime-env', variable: 'RUNTIME_ENV')]) {
                    unstash 'release-manifest'
                    sh 'bash infrastructure/scripts/prepare.sh a'
                }
            }
        }
        stage('Prepare B') {
            when { beforeAgent true; allOf { branch 'master'; expression { env.DEPLOY == 'true' } } }
            agent { label 'ec2-b' }
            steps {
                unstash 'deploy-config'
                withCredentials([file(credentialsId: 'ec2-b-runtime-env', variable: 'RUNTIME_ENV')]) {
                    unstash 'release-manifest'
                    sh 'bash infrastructure/scripts/prepare.sh b'
                }
            }
        }
        stage('Deploy B base') {
            when { beforeAgent true; allOf { branch 'master'; expression { env.DEPLOY == 'true' } } }
            agent { label 'ec2-b' }
            steps {
                unstash 'deploy-config'
                withCredentials([usernamePassword(credentialsId: 'registry-login',
                    usernameVariable: 'REGISTRY_USER', passwordVariable: 'REGISTRY_PASSWORD')]) {
                    sh 'bash infrastructure/scripts/with-registry.sh bash infrastructure/scripts/deploy.sh b-base'
                }
            }
        }
        stage('Deploy A and verify HTTP') {
            when { beforeAgent true; allOf { branch 'master'; expression { env.DEPLOY == 'true' } } }
            agent { label 'ec2-a' }
            steps {
                unstash 'deploy-config'
                withCredentials([usernamePassword(credentialsId: 'registry-login',
                    usernameVariable: 'REGISTRY_USER', passwordVariable: 'REGISTRY_PASSWORD')]) {
                    sh 'bash infrastructure/scripts/with-registry.sh bash infrastructure/scripts/deploy.sh a'
                }
                sh 'python3 infrastructure/scripts/verify-http.py'
            }
        }
        stage('Deploy realtime analysis') {
            when { beforeAgent true; allOf { branch 'master'; expression { env.DEPLOY == 'true' } } }
            agent { label 'ec2-b' }
            steps {
                unstash 'deploy-config'
                sh 'bash infrastructure/scripts/deploy.sh b-analysis'
            }
        }
        stage('Deploy Bridge and verify pipeline') {
            when { beforeAgent true; allOf { branch 'master'; expression { env.DEPLOY == 'true' } } }
            agent { label 'ec2-b' }
            steps {
                unstash 'deploy-config'
                sh 'bash infrastructure/scripts/deploy.sh b-bridge'
            }
        }
        stage('Deploy bronze loader') {
            when { beforeAgent true; allOf { branch 'master'; expression { env.DEPLOY == 'true' } } }
            agent { label 'ec2-b' }
            steps {
                unstash 'deploy-config'
                sh 'bash infrastructure/scripts/deploy.sh b-loader'
            }
        }
        stage('Record success A') {
            when { beforeAgent true; allOf { branch 'master'; expression { env.DEPLOY == 'true' } } }
            agent { label 'ec2-a' }
            steps {
                unstash 'deploy-config'
                sh 'python3 infrastructure/scripts/release.py success --manifest /opt/nilm/release.json'
            }
        }
        stage('Record success B') {
            when { beforeAgent true; allOf { branch 'master'; expression { env.DEPLOY == 'true' } } }
            agent { label 'ec2-b' }
            steps {
                unstash 'deploy-config'
                sh 'python3 infrastructure/scripts/release.py success --manifest /opt/nilm/release.json'
            }
        }
    }
    post {
        success { script { notifyDiscord('success') } }
        failure { script { notifyDiscord('failure') } }
    }
}
