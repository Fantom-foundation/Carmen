// Copyright (c) 2024 Fantom Foundation
//
// Use of this software is governed by the Business Source License included
// in the LICENSE file and at fantom.foundation/bsl11.
//
// Change Date: 2028-4-16
//
// On the date above, in accordance with the Business Source License, use of
// this software will be governed by the GNU Lesser General Public License v3.

pipeline {
    agent { label 'db-small-ssd' }

    options { 
        timestamps ()
        timeout(time: 2, unit: 'HOURS')
    }

    environment {
        GOROOT = '/usr/lib/go-1.21/'
        GOMEMLIMIT = '5GiB'
        CC = 'clang-14'
        CXX = 'clang++-14'
    }

    stages {
        stage('Check License headers') {
            steps {
                sh 'cd scripts/license && ./add_license_header.sh --check'
            }
        }

        stage('Check Go sources formatting') {
            steps {
                sh 'cd go && diff=`${GOROOT}/bin/gofmt -s -d .` && echo "$diff" && test -z "$diff"'
            }
        }

        stage('Build C++ libraries') {
            steps {
                sh 'git submodule update --init --recursive'
                sh 'cd go/lib && ./build_libcarmen.sh'
            }
        }

        stage('Build Go') {
            steps {
                sh 'cd go && go build -v ./...'
            }
        }

        stage('Run Go tests') {
            steps {
                sh 'cd go && go test ./... -parallel 1 -timeout 60m'
            }
        }

        stage('Check C++ sources formatting') {
            steps {
                sh 'find cpp/ -iname *.h -o -iname *.cc | xargs clang-format --dry-run -Werror '
            }
        }

        stage('Run C++ tests') {
            steps {
                sh 'cd cpp && bazel test --test_output=errors //...'
            }
        }
    }
}
