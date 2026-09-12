#!/bin/bash

# Script for automatic GitHub repository update
# Usage: ./update.sh "Your commit message"

# Check for presence of commit message
if [ -z "$1" ]; then
    echo "Error: Please provide a commit message."
    echo "Example: ./update.sh \"Fixed log processing error\""
    exit 1
fi

COMMIT_MSG="$1"

echo "--- Starting GitHub update ---"

# 1. Adding all changes
echo "[1/3] Adding files..."
git add .

# 2. Creating commit
echo "[2/3] Creating commit: '$COMMIT_MSG'..."
git commit -m "$COMMIT_MSG" || echo "No changes to commit."

# 3. Pushing to current branch
CURRENT_BRANCH=$(git rev-parse --abbrev-ref HEAD)
echo "[3/3] Pushing data to GitHub (branch $CURRENT_BRANCH)..."
# Добавляем флаг --no-thin и предварительную очистку для стабильности на внешних дисках
git gc --auto > /dev/null 2>&1
git push origin "$CURRENT_BRANCH" --no-thin

if [ $? -eq 0 ]; then
    echo "--- Success: Repository updated! ---"
else
    echo "--- Error: Failed to update repository. ---"
    exit 1
fi
