#!/usr/bin/env bash

set -eo pipefail

: "${GOOGLE_CREDENTIALS:?GOOGLE_CREDENTIALS must be set}"
: "${GP_VER:?GP_VER must be set}"
: "${PXF_CERTIFICATION_FOLDER:?PXF_CERTIFICATION_FOLDER must be set}"

full_certification_dir="gs://${PXF_CERTIFICATION_FOLDER}/gp${GP_VER}"

function authenticate() {
    echo "Authenticating with Google service account..."
    gcloud auth activate-service-account --key-file=<(echo "${GOOGLE_CREDENTIALS}") >/dev/null 2>&1
}

function list_certifications() {
    echo
    echo "******************************************************************************"
    echo "*****************  Available certifications for Greenplum-${GP_VER}  *****************"
    echo "******************************************************************************"
    echo

    if certifications=($(gsutil list ${full_certification_dir} 2>/dev/null)); then
        printf "%s\n" "${certifications[@]##*/}"
    fi

    echo
    echo "******************************************************************************"

}

function upload_certification() {
    if [[ ! -f certification/certification.txt ]]; then
	    echo 'ERROR: certification.txt file is not found.'
	    exit 1
    fi

    certification=$(< certification/certification.txt)
    echo "Found certification: $certification"

    authenticate

    full_certification_path="${full_certification_dir}/${certification}"

    # find if the certification already exist to avoid error on duplicate publishing
    if existing_certification=$(gsutil list "${full_certification_path}"); then
	    echo "Found existing certification: ${existing_certification}"
	    echo "Skipping upload, exiting successfully"
	    exit 0
    fi

    echo $(date) > /tmp/now
    echo "Uploading certification to ${full_certification_path} ..."
    gsutil cp /tmp/now "${full_certification_path}"

    echo
    echo "*****************************************************************************************"
    echo "Successfully uploaded certification : ${certification}"
    echo "*****************************************************************************************"
    echo

}


if [[ -z "$1" ]]; then
    echo "ERROR: No argument provided, allowed arguments are 'list' and 'upload'"
    exit 2
fi

case $1 in
    'list')
        authenticate
        list_certifications
        ;;
    'upload')
        upload_certification
        ;;
    *)
        echo "ERROR: Invalid argument $1, allowed arguments are 'list' and 'upload'"
        exit 2
        ;;
esac

exit $?