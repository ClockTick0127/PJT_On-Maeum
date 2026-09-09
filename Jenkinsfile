pipeline {
    agent none
    options {
        disableConcurrentBuilds()
        skipDefaultCheckout(true)
        timestamps()
        timeout(time: 60, unit: 'MINUTES')
    }
    parameters {
        booleanParam(name: 'ENABLE_CD', defaultValue: false,
            description: '초기 EC2/인증서/Credentials 준비 후 master 배포 활성화')
        string(name: 'REGISTRY', defaultValue: '',
            description: 'Registry namespace, e.g. registry.example.com/team/nilm')
        string(name: 'FRONTEND_ENV_CREDENTIAL', defaultValue: '',
            description: 'Optional Secret file credential for public Vite build settings')
    }
    stages {
        stage('CI') {
            agent { label 'ci' }
            steps {
                checkout scm
                script {
                    env.IMAGE_TAG = sh(script: 'git rev-parse HEAD', returnStdout: true).trim()
                    env.REGISTRY = params.REGISTRY.trim()
                    env.IMAGE_PREFIX = params.REGISTRY.trim() ?: 'nilm-ci'
                    if (params.ENABLE_CD && env.BRANCH_NAME == 'master' && !params.REGISTRY.trim()) {
                        error('REGISTRY is required for CD')
                    }
                }
                sh '''
                    for target in local ec2-a ec2-b; do
                        docker compose --env-file infrastructure/$target/.env.example -f infrastructure/$target/compose.yaml config --quiet
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
                stash name: 'deploy-config', includes: 'infrastructure/ec2-*/compose.yaml,infrastructure/scripts/**,infrastructure/nginx/*.template,infrastructure/keycloak/*.json,infrastructure/postgres/*.sql,infrastructure/mqtt/config/mosquitto.production.conf'
                script {
                    if (env.BRANCH_NAME == 'master' && params.ENABLE_CD) {
                        withCredentials([usernamePassword(credentialsId: 'registry-login',
                            usernameVariable: 'REGISTRY_USER', passwordVariable: 'REGISTRY_PASSWORD')]) {
                            sh 'bash infrastructure/scripts/with-registry.sh bash infrastructure/scripts/push.sh'
                        }
                    }
                }
            }
        }
        stage('Prepare A') {
            when { beforeAgent true; allOf { branch 'master'; expression { params.ENABLE_CD } } }
            agent { label 'ec2-a' }
            steps {
                unstash 'deploy-config'
                withCredentials([file(credentialsId: 'ec2-a-runtime-env', variable: 'RUNTIME_ENV')]) {
                    sh 'bash infrastructure/scripts/prepare.sh a'
                }
            }
        }
        stage('Prepare B') {
            when { beforeAgent true; allOf { branch 'master'; expression { params.ENABLE_CD } } }
            agent { label 'ec2-b' }
            steps {
                unstash 'deploy-config'
                withCredentials([file(credentialsId: 'ec2-b-runtime-env', variable: 'RUNTIME_ENV')]) {
                    sh 'bash infrastructure/scripts/prepare.sh b'
                }
            }
        }
        stage('Deploy B base') {
            when { beforeAgent true; allOf { branch 'master'; expression { params.ENABLE_CD } } }
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
            when { beforeAgent true; allOf { branch 'master'; expression { params.ENABLE_CD } } }
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
        stage('Deploy Bridge and verify pipeline') {
            when { beforeAgent true; allOf { branch 'master'; expression { params.ENABLE_CD } } }
            agent { label 'ec2-b' }
            steps {
                unstash 'deploy-config'
                sh 'bash infrastructure/scripts/deploy.sh b-bridge'
            }
        }
    }
}
