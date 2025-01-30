#!/bin/bash

set -e

main() {
  # ANSI escape code variables
  BOLD=$(tput bold)
  NORMAL=$(tput sgr0)

  if ! command -v tar >/dev/null 2>&1; then
    echo "Error: 'tar' is required." 1>&2
    exit 1
  fi

  if ! command -v jq >/dev/null 2>&1; then
    echo "Error: 'jq' is required." 1>&2
    exit 1
  fi

  OS=$(uname -s)
  if [ "$OS" = "Windows_NT" ]; then
    echo "Error: Windows is not supported yet." 1>&2
    exit 1
  else
    UNAME_SM=$(uname -sm)
    case "$UNAME_SM" in
    "Darwin x86_64") target="darwin.amd64.tar.gz" ;;
    "Darwin arm64") target="darwin.arm64.tar.gz" ;;
    "Linux x86_64") target="linux.amd64.tar.gz" ;;
    "Linux aarch64") target="linux.arm64.tar.gz" ;;
    *) echo "Error: '$UNAME_SM' is not supported yet." 1>&2; exit 1 ;;
    esac
  fi

  # Check if version is provided as an argument
  if [ $# -eq 0 ] || [ -z "$1" ]; then
    printf "Enter the version (latest): "
    read version
    version=${version:-latest}
  else
    version=$1
  fi

  # Check if location is provided as an argument
  if [ $# -lt 2 ] || [ -z "$2" ]; then
    printf "Enter location (/usr/local/bin): "
    read location
    location=${location:-/usr/local/bin}
  else
    location=$2
  fi

  bin_dir=$location
  exe="$bin_dir/tailpipe"

  tmp_dir=$(mktemp -d)
  mkdir -p "${tmp_dir}"
  tmp_dir="${tmp_dir%/}"

  echo "Created temporary directory at $tmp_dir."
  cd "$tmp_dir" || exit

  # set a trap for a clean exit - even in failures
  trap 'rm -rf $tmp_dir' EXIT

  case $(uname -s) in
    "Darwin" | "Linux") zip_location="$tmp_dir/tailpipe.${target}" ;;
    *) echo "Error: tailpipe is not supported on '$(uname -s)' yet." 1>&2; exit 1 ;;
  esac

  # Generate the URI for the binary
  if [ "$version" = "latest" ]; then
    uri="https://api.github.com/repos/turbot/tailpipe/releases/latest"
    asset_name="tailpipe.${target}"
  else
    uri="https://api.github.com/repos/turbot/tailpipe/releases/tags/${version}"
    asset_name="tailpipe.${target}"
  fi

  # Read the GitHub Personal Access Token
  GITHUB_TOKEN=${GITHUB_TOKEN:-}

  if [ -z "$GITHUB_TOKEN" ]; then
    echo ""
    echo "Error: GITHUB_TOKEN is not set. Please set your GitHub Personal Access Token as an environment variable." 1>&2
    exit 1
  fi
  AUTH="Authorization: token $GITHUB_TOKEN"

  response=$(curl -sH "$AUTH" $uri)
  id=$(echo "$response" | jq --arg asset_name "$asset_name" '.assets[] | select(.name == $asset_name) | .id' | tr -d '"')
  GH_ASSET="$uri/releases/assets/$id"

  echo ""
  echo "Downloading ${BOLD}${asset_name}${NORMAL}..."
  curl -#SL -H "$AUTH" -H "Accept: application/octet-stream" \
     "https://api.github.com/repos/turbot/tailpipe/releases/assets/$id" \
     -o "$zip_location" -L --create-dirs

  file "$zip_location"

  echo "Deflating downloaded archive"
  tar -xvf "$zip_location" -C "$tmp_dir"

  echo "Installing"
  install -d "$bin_dir"
  install "$tmp_dir/tailpipe" "$bin_dir"

  echo "Applying necessary permissions"
  chmod +x $exe

  echo "Removing downloaded archive"
  rm "$zip_location"

  echo "tailpipe was installed successfully to $bin_dir"

  if ! command -v $bin_dir/tailpipe >/dev/null 2>&1; then
    echo "tailpipe was installed, but could not be executed. Are you sure '$bin_dir/tailpipe' has the necessary permissions?"
    exit 1
  fi
}

# Call the main function to run the script
main "$@"
