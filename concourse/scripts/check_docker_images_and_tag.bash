#!/bin/bash

set -euo pipefail

: "${IMAGE_TAG:?IMAGE_TAG must be set}"
COMMIT_SHA=$(cat pxf_src/.git/ref)
echo "Checking images for PXF SHA-1: ${COMMIT_SHA}"

if [[ -z $IMAGE_LIST ]]; then
  echo "The IMAGE_LIST needs to be provided"
  exit 1
fi

gcloud config set project "$GOOGLE_PROJECT_ID"
gcloud auth activate-service-account --key-file=<(echo "$GOOGLE_CREDENTIALS")

# Don't quote $IMAGE_LIST to allow array
IMAGE_NAMES=(${IMAGE_LIST})

# wait 1 minute before checking
sleep 1m
# get the build id
BUILD_ID=$(gcloud builds list --ongoing --filter "images='gcr.io/${GOOGLE_PROJECT_ID}/gpdb-pxf-dev/${IMAGE_NAMES[0]}:${COMMIT_SHA}'" | awk 'FNR == 2 {print $1}')

# Wait ~ 40 minutes
for i in {0..40}
do
  if [ -n "$BUILD_ID" ]; then
    echo "Checking status for build with ID: ${BUILD_ID}"
    # if we have the $BUILD_ID, check the status using it
    STATUS=$(gcloud builds list --filter "status='SUCCESS' AND build_id='${BUILD_ID}'" 2>&1 | awk 'FNR == 2 {print $1}')
  else
    echo "Checking status with filter images='gcr.io/${GOOGLE_PROJECT_ID}/gpdb-pxf-dev/${IMAGE_NAMES[0]}:${COMMIT_SHA}'"
    # otherwise, let's use the image name
    STATUS=$(gcloud builds list --filter "status='SUCCESS' AND images='gcr.io/${GOOGLE_PROJECT_ID}/gpdb-pxf-dev/${IMAGE_NAMES[0]}:${COMMIT_SHA}'" 2>&1 | awk 'FNR == 2 {print $1}')
  fi

  if [ -n "$STATUS" ]; then
    echo "Cloud build completed with ID ${STATUS}"

    for image in "${IMAGE_NAMES[@]}"
    do
      # we need to untag ${IMAGE_TAG} first if it already exists
      # this returns an empty value if no tags are found
      tag_exists=$(gcloud container images list-tags gcr.io/${GOOGLE_PROJECT_ID}/gpdb-pxf-dev/${image} --filter="tags : ${IMAGE_TAG}")
      # if the call to gcloud returned values, make sure the tag is in the output
      if [[ -n $tag_exists && tag_exists =~ ${IMAGE_TAG} ]]; then
        gcloud container images untag --quiet "gcr.io/${GOOGLE_PROJECT_ID}/gpdb-pxf-dev/${image}:${IMAGE_TAG}" || true
      fi
      # tag image with ${IMAGE_TAG}
      echo "Tagging gcr.io/${GOOGLE_PROJECT_ID}/gpdb-pxf-dev/${image}:${COMMIT_SHA} with ${IMAGE_TAG}"
      gcloud container images add-tag --quiet \
        "gcr.io/${GOOGLE_PROJECT_ID}/gpdb-pxf-dev/${image}:${COMMIT_SHA}" \
        "gcr.io/${GOOGLE_PROJECT_ID}/gpdb-pxf-dev/${image}:${IMAGE_TAG}"
    done

    exit 0
  else
    echo "Attempt ${i}: Checking cloud build status"
  fi

  # sleep for 1 min
  sleep 1m
done

echo "Timeout exceeded while waiting for cloud build to complete"
# fail if we wait for too long
exit 1