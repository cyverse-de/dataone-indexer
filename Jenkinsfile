!groovy

JENKINS_CREDS = [
    [
        $class: 'UsernamePasswordMultiBinding',
        credentialsId: 'jenkins-docker-credentials',
        passwordVariable: 'DOCKER_PASSWORD',
        usernameVariable: 'DOCKER_USERNAME'
    ]
]

def buildDockerImage(commit, version, repo) {
    sh """
        docker build --rm \\
            --build-arg git_commit=${commit} \\
            --build-arg version=${version} \\
            --build-arg descriptive_version=${version} \\
            -t ${repo} \\
            .
    """

    // Retrieve the image SHA.
    imageSha = sh(returnStdout: true, script: "docker inspect -f '{{ .Config.Image }}' ${repo}").trim()
    echo imageSha

    // Record the image SHA.
    shaFile = "${repo}.docker-image-sha"
    writeFile(file: "${shaFile}", text: "${imageSha}")
    fingerprint "${shaFile}"
}

def testGoService(containerName, repo, packagesToTest) {
    sh """
        docker run --rm \\
            --name ${containerName} \\
            --entrypoint 'go' \\
            ${repo} \\
            test ${packagesToTest}
    """
}

def retagDockerImage(oldTag, newTag) {
    sh "docker tag ${oldTag} ${newTag}"
}

def pushDockerImage(repo, containerName) {
    withCredentials(JENKINS_CREDS) {
        sh """
            docker run -e DOCKER_USERNAME -e DOCKER_PASSWORD \\
                       -v /var/run/docker.sock:/var/run/docker.sock \\
                       --rm --name ${containerName} \\
                       docker:\$(docker version --format '{{ .Server.Version }}') \\
                       sh -e -c \\
                'docker login -u \"\$DOCKER_USERNAME\" -p \"\$DOCKER_PASSWORD\" && \\
                 docker push ${repo} && \\
                 docker rmi ${repo} && \\
                 docker logout'
        """
    }
}

// Containers may not exit immediately if a build is aborted through Jenkins. Explicitly kill the container to
// make sure it stops. Also remove the container for good measure. Just because I'm paranoid, it doesn't mean
// that they're not out to get me.
def removeContainer(containerName) {
    sh returnStatus: true, script: "docker kill ${containerName}"
    sh returnStatus: true, script: "docker rm ${containerName}"
}

// Ensure that a Docker image is removed.
def removeImage(image) {
    sh returnStatus: true, script: "docker rmi ${image}"
}

// Records build information in the associated Jira issue if possible.
def recordBuild(label) {
    step(
        [
            $class: 'hudson.plugins.jira.JiraIssueUpdater',
            issueSelector: [$class: 'hudson.plugins.jira.selector.DefaultIssueSelector'],
            scm: scm,
            labels: [label]
        ]
    )
}

// Configure this job to clean up old builds.
properties(
    [
        buildDiscarder(
            logRotator(
                artifactDaysToKeepStr: '',
                artifactNumToKeepStr: '',
                daysToKeepStr: '7',
                numToKeepStr: '5'
            )
        )
    ]
)

// The packages we want to test.
packagesToTest = "github.com/cyverse-de/dataone-indexer/database github.com/cyverse-de/dataone-indexer/model"

node('docker') {
    slackJobDescription = "job '${env.JOB_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL})"

    try {
        checkout scm

        // Get the git commit hash and descriptive version.
        gitCommit = sh(returnStdout: true, script: "git rev-parse HEAD").trim()
        descriptiveVersion = sh(returnStdout: true, script: "git describe --long --tags --dirty --always").trim()

        // Load the service properties.
        service = readProperties file: 'service.properties'

        // Docker container names.
        dockerTestRunner = "test-${env.BUILD_TAG}"
        dockerPusher = "push-${env.BUILD_TAG}"

        // Docker image names.
        dockerRepo = "test-${env.BUILD_TAG}"
        dockerPushRepo = "${service.dockerUser}/${service.repo}:${env.BRANCH_NAME}"

        try {
            // Display the git commit and descriptive version.
            echo "git commit: ${gitCommit}"
            echo "descriptive version: ${descriptiveVersion}"

            // Build the Docker image.
            stage('Docker Build') {
                buildDockerImage(gitCommit, descriptiveVersion, dockerRepo)
            }

            // Test the service.
            stage('Test') {
                testGoService(dockerTestRunner, dockerRepo, packagesToTest)
            }

            // Push the docker image.
            milestone 100
            lock ("docker-push-${dockerPushRepo}") {
                milestone 101
                stage('Docker Push') {
                    retagDockerImage(dockerRepo, dockerPushRepo)
                    pushDockerImage(dockerPushRepo, dockerPusher)
                }
            }
        } finally {
            removeContainer(dockerTestRunner)
            removeContainer(dockerPusher)
            removeImage(dockerRepo)
            removeImage(dockerPushRepo)
        }
    } catch (InterruptedException e) {
        currentBuild.result = "ABORTED"
        slackSend color: 'warning', message: "ABORTED: ${slackJobDescription}"
        throw e
    } catch (e) {
        currentBuild.result = "FAILED"
        sh "echo ${e}"
        slackSend color: 'danger', message: "FAILED: ${slackJobDescription}"
        throw e
    }
}
