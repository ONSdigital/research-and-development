#!groovy

// Global scope required for multi-stage persistence
def artifactoryStr = 'art-p-01'
artServer = Artifactory.server "${artifactoryStr}"
buildInfo = Artifactory.newBuildInfo()
def agentPython3Version = 'python_3.6.2'
def artifactVersion

// Define a function to push packaged code to Artifactory
def pushToPyPiArtifactoryRepo_temp(String projectName, String version, String sourceDistLocation = 'python/dist/*', String artifactoryHost = 'art-p-01') {
    withCredentials([usernamePassword(credentialsId: env.ARTIFACTORY_CREDS, usernameVariable: 'ARTIFACTORY_USER', passwordVariable: 'ARTIFACTORY_PASSWORD')]){
        sh "curl -u ${ARTIFACTORY_USER}:\${ARTIFACTORY_PASSWORD} -T ${sourceDistLocation} 'http://${artifactoryHost}/artifactory/${env.ARTIFACTORY_PYPI_REPO}/${projectName}/'"
    }
}

// Define a function to update the pipeline status on Gitlab
def updateGitlabStatus_temp(String stage, String state, String gitlabHost = 'https://gitlab-app-l-01.ons.statistics.gov.uk') {
    withCredentials([string(credentialsId: env.GITLAB_CREDS, variable: 'GITLAB_TOKEN')]) {
        println("Updating GitLab pipeline status")
        shortCommit = sh(returnStdout: true, script: "cd ${PROJECT_NAME} && git log -n 1 --pretty=format:'%h'").trim()
        sh "curl --request POST --header \"PRIVATE-TOKEN: ${GITLAB_TOKEN}\" \"${gitlabHost}/api/v4/projects/${GITLAB_PROJECT_ID}/statuses/${shortCommit}?state=${state}&name=${stage}&target_url=${BUILD_URL}\""
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
        GITLAB_PROJECT_ID       = 'gitlabid_placeholder'
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

                updateGitlabStatus_temp('Jenkins', 'pending')

                script {
                    buildInfo.name = "resdev"
                    buildInfo.number = "${BUILD_NUMBER}"
                    buildInfo.env.collect()
                }
                colourText('info', "BuildInfo: ${buildInfo.name}-${buildInfo.number}")
                stash name: 'Checkout', useDefaultExcludes: false
            }
        }

    }
}
