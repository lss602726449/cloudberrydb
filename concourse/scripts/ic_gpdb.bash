#!/bin/bash -l

set -eox pipefail

CWDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "${CWDIR}/common.bash"

function gen_env(){
  cat > /opt/run_test.sh <<-EOF
		trap look4diffs ERR

		function look4diffs() {

		    diff_files=\`find .. -name regression.diffs\`

		    for diff_file in \${diff_files}; do
			if [ -f "\${diff_file}" ]; then
			    cat <<-FEOF

						======================================================================
						DIFF FILE: \${diff_file}
						----------------------------------------------------------------------

						\$(cat "\${diff_file}")

					FEOF
			fi
		    done
		    exit 1
		}
		source $INSTALL_DIR/greenplum_path.sh
		cd "\${1}/gpdb_src"
		source gpAux/gpdemo/gpdemo-env.sh
		PG_TEST_EXTRA="kerberos ssl" make -s ${MAKE_TEST_COMMAND}
	EOF

	chmod a+x /opt/run_test.sh
}

function setup_gpadmin_user() {
    ./gpdb_src/concourse/scripts/setup_gpadmin_user.bash "$TEST_OS"
}

# release package doesn't configure with debug extension
# See option `enable_debug_extensions` in gpcontrib/Makefile
function install_debug_extension_for_release() {
    pushd ./gpdb_src/gpcontrib
    for i in gp_debug_numsegments \
        gp_inject_fault \
        gp_legacy_string_agg \
        gp_replica_check ; do
        pushd $i
        make install
        popd
    done
    popd
}

function _main() {
    if [ -z "${MAKE_TEST_COMMAND}" ]; then
        echo "FATAL: MAKE_TEST_COMMAND is not set"
        exit 1
    fi

    if [ -z "$TEST_OS" ]; then
        echo "FATAL: TEST_OS is not set"
        exit 1
    fi

    case "${TEST_OS}" in
    centos|ubuntu|sles) ;; #Valid
    *)
      echo "FATAL: TEST_OS is set to an invalid value: $TEST_OS"
      echo "Configure TEST_OS to be centos, or ubuntu"
      exit 1
      ;;
    esac

    time install_and_configure_gpdb
    time setup_gpadmin_user
    time make_cluster
    time gen_env
    #increasing sleep time 60s here for separated fts node feature compatibility, the cluster needs to wait for one fts probe procedure timewait.
    sleep 60s
    su gpadmin -c ./gpdb_src/concourse/scripts/ic_start_fts.bash
    if [ "$BUILD_TYPE" == "release" ]; then
        install_debug_extension_for_release
    fi
    time run_test

    su gpadmin -c ./gpdb_src/concourse/scripts/ic_recover_fts_dump.bash
    if [ "${TEST_BINARY_SWAP}" == "true" ]; then
        time ./gpdb_src/concourse/scripts/test_binary_swap_gpdb.bash
    fi

    if [ "${DUMP_DB}" == "true" ]; then
        chmod 777 sqldump
        su gpadmin -c ./gpdb_src/concourse/scripts/dumpdb.bash
    fi
}

_main "$@"
