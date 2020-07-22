pipeline {
    agent any

    tools {
        jdk 'jdk-12'
    }

    options {
        timestamps()
        skipStagesAfterUnstable()
        buildDiscarder(logRotator(numToKeepStr: '30'))
    }

    stages {

        stage('Clean') {
            // Only clean when the last build failed
            when {
                expression {
                    currentBuild.previousBuild?.currentResult == 'FAILURE'
                }
            }
            steps {
                sh "./gradlew clean"
            }
        }

        stage('Compile') {
            steps {
                sh './gradlew -v' // Output gradle version for verification checks
                sh "./gradlew jenkinsClean compile"
            }
        }

        stage('Static Code Analysis') {
            steps {
                sh "./gradlew -PciRun=true staticCodeAnalysis"
            }
            post {
                always {
                    checkstyle canComputeNew: false, defaultEncoding: '', healthy: '0', pattern: '**/build/reports/checkstyle/*.xml', unHealthy: ''
                    findbugs canComputeNew: false, defaultEncoding: '', excludePattern: '', healthy: '0', includePattern: '', pattern: '**/build/reports/findbugs/*.xml', unHealthy: ''
                    pmd canComputeNew: false, defaultEncoding: '', healthy: '0', pattern: '**/build/reports/pmd/*.xml', unHealthy: ''
                    dry canComputeNew: false, defaultEncoding: '', healthy: '0', pattern: 'reports/cpd/*.xml', unHealthy: ''
                    archiveArtifacts allowEmptyArchive: true, artifacts: '**/build/reports/jdepend/*.txt' // No publisher available
                }
            }
        }

        stage('Deploy to Artifactory') {
            when {
                allOf {
                    branch 'master'
                    expression {
                        currentBuild.currentResult == 'SUCCESS'
                    }
                }

            }
            steps {
                script {
                    sh "./gradlew artifactoryPublish"
                }
            }
        }
    }

    post {
        always {
            archiveArtifacts allowEmptyArchive: true, artifacts: '**/*.log'
            slackNotification()
        }
    }
}
