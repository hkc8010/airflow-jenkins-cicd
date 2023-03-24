pipeline {
    agent any
    stages {
        stage('Install astro cli') {
            when {
                expression {
                    return env.GIT_BRANCH == "origin/main"
                }
            }
            steps {
                checkout scm
                sh '''
                curl -LJO https://github.com/astronomer/astro-cli/releases/download/v1.11.0/astro_1.11.0_darwin_arm64.tar.gz
                tar -zxvf astro_1.11.0_darwin_arm64.tar.gz astro && rm astro_1.11.0_darwin_arm64.tar.gz
                ./astro version
                export PATH=$PATH:/usr/local/bin 
                echo $PATH
                '''
            }
        }
        stage("Test") {
            steps {
                sh '''
                export PATH=$PATH:/usr/local/bin 
                echo $PATH
                ls -altrh
                ./astro dev parse
                ./astro dev pytest
                '''
            }
        }
        }

    post {
        always {
            cleanWs()
        }
    }
}