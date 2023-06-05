#!/bin/bash

# Start the SSH agent and evaluate its output
eval "$(ssh-agent -s)"

# Add the SSH key to the agent
ssh-add ~/.ssh/githubwork/work_key