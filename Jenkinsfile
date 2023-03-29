#!groovy

// Global scope required for multi-stage persistence
def artifactoryStr = 'art-p-01'
artServer = Artifactory.server "${artifactoryStr}"
buildInfo = Artifactory.newBuildInfo()
def agentPython3Version = 'python_3.6.1'
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
        BUILD_BRANCH            = '142_jenkinsFile_RAP'  // Any commits to this branch will create a build in artifactory
        BUILD_TAG               = 'v*'  // Any commits tagged with this pattern will create a build in artifactory
        MIN_COVERAGE_PC         = '0'
        GITLAB_CREDS            = 'epds_gitlab_token'  // Credentials used for notifying GitLab of build status
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
                    buildInfo.name = "resdev"
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
                PATH=$WORKSPACE/venv/bin:/usr/local/bin:$PATH

                python3 -m pip install -U pip
                pip install conda
                conda -V

                if [ ! -d "resdev36" ]; then
                    conda create -n resdev36 python=3.6.2
                fi

                source activate resdev36

                '''
            stash name: 'resdev36', useDefaultExcludes: false
            }
        }

    }

}
