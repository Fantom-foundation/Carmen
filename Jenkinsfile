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
    }
}
