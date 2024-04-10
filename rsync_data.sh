#!/bin/bash

# Default variables
server="" # Server alias defined in ~/.ssh/config
file_path=""
auth_file=""
destination_path="data/ids/" # Default path for data syncing

# Function to display usage
usage() {
    echo "Usage: $0 --server SERVER --file FILE_PATH [--auth AUTH_FILE] [--destination_path DESTINATION_PATH]"
    exit 1
}

# Parse command line arguments
while [[ "$#" -gt 0 ]]; do
    case $1 in
        --server) server="$2"; shift ;;
        --file) file_path="$2"; shift ;;
        --auth) auth_file="$2"; shift ;;
        --destination_path) destination_path="$2"; shift ;;
        *) echo "Unknown parameter passed: $1"; usage; ;;
    esac
    shift
done

# Check required parameters
if [ -z "$server" ] || [ -z "$file_path" ]; then
    echo "Error: --server and --file are required."
    usage
fi

# Ensure the creation of the log/, data/ids/, and auth/ directories on the server
ssh "$server" << 'EOF'
mkdir -p ~/log ~/data/ids/ ~/auth
EOF

# Rsync the main file
rsync -avz -e ssh "$file_path" "$server:~/$destination_path"

# Rsync the auth file if specified
if [ ! -z "$auth_file" ]; then
    rsync -avz -e ssh "$auth_file" "$server:~/auth/AUTH.json"
fi

# Check if refresh_shift exists in the user's home directory and clone if not
ssh "$server" << 'EOF'
if [ -d "~/refresh_shift" ]; then
    if [ ! -d "~/refresh_shift/.git" ]; then
        echo "The directory '~/refresh_shift' exists but is not a Git repository. Consider removing it or turning it into a Git repository."
    else
        echo "'~/refresh_shift' already exists as a Git repository."
    fi
else
    git clone https://github.com/joaopn/refresh_shift.git ~/refresh_shift
fi
EOF
