pipeline {
    agent any

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

        stage('Unit Test') {
            steps {
                sh "./gradlew unitTest"
            }
            post {
                always {
                    publishHTML([
                            allowMissing         : true,
                            alwaysLinkToLastBuild: true,
                            keepAll              : true,
                            reportDir            : 'reports/tests/unitTest',
                            reportFiles          : 'index.html',
                            reportName           : 'Unit Test Report',
                            reportTitles         : 'Test'
                    ])
                    outputTestResults()
                }
            }
        }

        stage('Integration Test') {
            steps {
                script {
                    def outputTestFolder = uk.ac.ox.ndm.jenkins.Utils.generateRandomTestFolder()
                    def port = uk.ac.ox.ndm.jenkins.Utils.findFreeTcpPort()

                    sh "./gradlew " +
                       "-Dhibernate.search.default.indexBase=${outputTestFolder} " +
                       "-Dserver.port=${port} " +
                       "integrationTest"
                }
            }
            post {
                always {
                    publishHTML([
                            allowMissing         : true,
                            alwaysLinkToLastBuild: true,
                            keepAll              : true,
                            reportDir            : 'reports/tests/integrationTest',
                            reportFiles          : 'index.html',
                            reportName           : 'Integration Test Report',
                            reportTitles         : 'Test'
                    ])
                    junit allowEmptyResults: true, testResults: '**/build/test-results/**/*.xml'
                    outputTestResults()
                }
            }
        }

        stage('Jacoco Report') {
            steps {
                sh "./gradlew jacocoRootReport"
            }
            post {
                always {
                    jacoco execPattern: '**/build/jacoco/*.exec'
                    publishHTML([
                            allowMissing         : true,
                            alwaysLinkToLastBuild: true,
                            keepAll              : true,
                            reportDir            : 'reports/jacoco/jacocoRootReport/html',
                            reportFiles          : 'index.html',
                            reportName           : 'Coverage Report (Gradle)',
                            reportTitles         : 'Jacoco Coverage'
                    ])
                }
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
