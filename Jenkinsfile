#!groovy

// Global scope required for multi-stage persistence
def artifactoryStr = 'art-p-01'
artServer = Artifactory.server "${artifactoryStr}"
buildInfo = Artifactory.newBuildInfo()
def agentPython3Version = 'python_3.10'
def artifactVersion

// Define a function to push packaged code to Artifactory
def pushToPyPiArtifactoryRepo_temp(String projectName, String version, String sourceDistLocation = 'python/dist/*', String artifactoryHost = 'art-p-01') {
    withCredentials([usernamePassword(credentialsId: env.ARTIFACTORY_CREDS, usernameVariable: 'ARTIFACTORY_USER', passwordVariable: 'ARTIFACTORY_PASSWORD')]){
        sh "curl -u ${ARTIFACTORY_USER}:\${ARTIFACTORY_PASSWORD} -T ${sourceDistLocation} 'http://${artifactoryHost}/artifactory/${env.ARTIFACTORY_PYPI_REPO}/${projectName}/'"
    }
}

// This section defines the Jenkins pipeline
pipeline {
    libraries {
        lib('jenkins-pipeline-shared@feature/dap-ci-scripts')
    }

    environment {
        ARTIFACTORY_CREDS       = 's_jenkins_epds'
        ARTIFACTORY_PYPI_REPO   = 'LR_EPDS_pypi'
        PROJECT_NAME            = 'resdev'
        BUILD_BRANCH            = 'main'  // Any commits to this branch will create a build in artifactory
        BUILD_TAG               = '*-release'  // Any commits tagged with this pattern will create a build in artifactory
        MIN_COVERAGE_PC         = '0'
        GITLAB_CREDS            = 'rdsa_token'  // Credentials used for notifying GitLab of build status
    }

    options {
        skipDefaultCheckout true
    }

    agent any

    stages {
        stage('Checkout') {
            agent { label 'download.jenkins.slave' }
            steps {
                onStage()
                colourText('info', "Checking out code from source control.")

                checkout scm

                script {
                    buildInfo.name = "${PROJECT_NAME}"
                    buildInfo.number = "${BUILD_NUMBER}"
                    buildInfo.env.collect()
                }
                colourText('info', "BuildInfo: ${buildInfo.name}-${buildInfo.number}")
                stash name: 'Checkout', useDefaultExcludes: false
            }
        }

        stage('Preparing virtual environment') {
            agent { label "test.${agentPython3Version}" }
            steps {
                onStage()
                colourText('info', "Create venv and install dependencies")
                unstash name: 'Checkout'

                sh '''
                PATH=$WORKSPACE/venv/bin:/usr/local/bin:/root/.local/bin:$PATH

                python3 -m pip install -U pip
                pip3 install virtualenv

                if [ ! -d "venv" ]; then
                    virtualenv venv
                fi
                . venv/bin/activate
                export PIP_USER=false

                pip3 install pypandoc

                # Remove pydoop from requirements before it's installed.
                awk '!/pydoop.*/' requirements.txt > temp && mv temp requirements.txt

                pip3 install -r requirements.txt

                pip3 freeze

                '''
            stash name: 'venv', useDefaultExcludes: false
            }
        }


        stage('Unit Test and coverage') {
            agent { label "test.${agentPython3Version}" }
            steps {
                onStage()
                colourText('info', "Running unit tests and code coverage.")
                unstash name: 'Checkout'
                unstash name: 'venv'

                // Compatibility for PyArrow with Spark 2.4-legacy IPC format.
                sh 'export ARROW_PRE_0_15_IPC_FORMAT=1'

                // Running coverage first runs the tests
                sh '''
                . venv/bin/activate

                python3 -m pytest --junitxml "junit-report.xml" "./tests"
                '''

                junit testResults: 'junit-report.xml'

                /* coverage run --branch --source=./${PROJECT_NAME} --omit=src/utils/hdfs_mods.py,src/utils/wrappers.py,src/utils/runlog.py,src/_version.py,src/pipeline.py \
                    -m pytest -ra ./tests --ignore=tests/test_utils/test_hdfs_mods.py
                '''
                /*
                // Lines below create a coverage report for on Jenkins. Currently commented out
                // as it gives errors when no imports are used in unit tests. import src.main
                // causes pre-commit to complain. Easier to leave out for now.
                coverage xml -o python_coverage.xml && coverage report -m --fail-under=${MIN_COVERAGE_PC}


                cobertura autoUpdateHealth: false,
                        autoUpdateStability: false,
                        coberturaReportFile: 'python_coverage.xml',
                        conditionalCoverageTargets: '70, 0, 0',
                        failUnhealthy: false,
                        failUnstable: false,
                        lineCoverageTargets: '80, 0, 0',
                        maxNumberOfBuilds: 0,
                        methodCoverageTargets: '80, 0, 0',
                        onlyStable: false,
                        zoomCoverageChart: false */
            }
        }

        stage('Build and publish Python Package') {
            when {
                anyOf{
                    branch BUILD_BRANCH
                    tag BUILD_TAG
                }
                beforeAgent true
            }
            agent { label "test.${agentPython3Version}" }
            steps {
                onStage()
                colourText('info', "Building Python package.")
                unstash name: 'Checkout'
                unstash name: 'venv'

                sh '''
                . venv/bin/activate
                pip3 install setuptools
                pip3 install wheel
                python3 setup.py build bdist_wheel
                '''

                script {
                    pushToPyPiArtifactoryRepo_temp("${buildInfo.name}", "", "dist/*")
                }
            }
        }



    }

}
