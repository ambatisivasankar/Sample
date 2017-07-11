## Bootstrapping script for setting up the environment for the cluster

# Need to tell the script what branch we are going to be running
echo "--------------------------------------------------------"
#echo "Setting up submodules..."
GIT_BRANCH="master"
# Check for options dev or develop for develop branch or prod/production/master for master branch
for i in "$@"; do
    case $i in 
        dev|develop)
            GIT_BRANCH="develop"
        ;;
        prod|production|master)
            GIT_BRANCH="master"
        ;;
        *)
            # unknown option
        ;;
    esac
done

#echo "Pulling submodules branch: $GIT_BRANCH"
#git submodule init
#git submodule update
#cd squark
#git checkout $GIT_BRANCH
#git pull
#cd ../squark-classic
#git checkout $GIT_BRANCH
#git pull
##cd ../data_catalog-statscli
##git checkout master
##git pull
#cd ..
#
#echo "Finished pulling submodules..."
#echo "--------------------------------------------------------"

echo "Checking password variables are set..."
# Check to make sure that password variables are set:
if [ -z ${SQUARK_PASSWORD+x} ]; then
    echo "SQUARK_PASSWORD variable is not set. Cannot continue..."
    exit 1
fi
if [ -z ${VERTICA_PASSWORD+x} ]; then
    echo "VERTICA_PASSWORD variable is not set. Cannot continue..."
    exit 1
fi
echo "Finished checking password variables..."
echo "--------------------------------------------------------"

echo "Setting SQUARK_PASSWORD_FILE and setting to mode 600..."
echo $SQUARK_PASSWORD > $SQUARK_PASSWORD_FILE
chmod 600 $SQUARK_PASSWORD_FILE
echo "--------------------------------------------------------"

echo "Setting up the virtual environment..."
virtualenv -p /usr/local/bin/python3.5 virt
#${PYTHON_VENV}/bin/pip3 install -r data_catalog-statscli/requirements.txt
${PYTHON_VENV}/bin/pip3 install -r squark-classic/requirements.txt
cd squark
${PYTHON_VENV}/bin/pip3 install --no-cache-dir -r requirements.txt
${PYTHON_VENV}/bin/python3 setup.py develop
cd ..
# Adding a temporary fix to remove cryptography as it causes issues with older linux kernels.
${PYTHON_VENV}/bin/pip uninstall -y cryptography
echo "Finished setting up the virtual environment..."
echo "--------------------------------------------------------"

echo "FINISHED BOOTSTRAPPING THE ENVINROMENT -- GOOD TO GO!"
echo " -- ENVIRONMENT WAS SETUP USING THE GIT $GIT_BRANCH ENVIRONMENT..."
echo "--------------------------------------------------------"
