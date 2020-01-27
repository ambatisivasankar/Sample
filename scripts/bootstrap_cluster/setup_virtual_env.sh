###############################################################
## Setup the virtual environment

# Stop at any error, treat unset vars as errors and make pipelines exit
# with a non-zero exit code if any command in the pipeline exits with a
# non-zero exit code.
set -o nounset
set +e
set +x


printLineMsg before "Setting up the virtual environment..."
python35 -m venv virt

echo "Updating pip..."
"${PYTHON_VENV}"/bin/pip3 install --upgrade pip
"${PYTHON_VENV}"/bin/pip3 install --upgrade wheel  # py4jdbc install will fail without this
echo "Installing squark-classic requirements."
"${PYTHON_VENV}"/bin/pip3 install pytest-runner==2.9 --index-url=https://artifactory.awsmgmt.massmutual.com/artifactory/api/pypi/python-virtual/simple/
"${PYTHON_VENV}"/bin/pip3 install -r squark-classic/requirements.txt
echo "Installing squark requirements."
cd "${WORKSPACE}/squark" || die "Failed to cd into squark"
"${PYTHON_VENV}"/bin/pip3 install --no-cache-dir -r requirements.txt
echo "Installing squark"
"${PYTHON_VENV}"/bin/python3 setup.py develop
cd ..

# Adding a temporary fix to remove cryptography as it causes issues with older linux kernels.
"${PYTHON_VENV}"/bin/pip3 uninstall -y cryptography
printLineMsg after "Finished setting up the virtual environment..."
