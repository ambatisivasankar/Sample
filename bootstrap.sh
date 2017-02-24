virtualenv -p /usr/bin/python3 virt
/usr/local/src/squark-advana/virt/bin/pip3 install -r squark-classic/requirements.txt
cd squark
/usr/local/src/squark-advana/virt/bin/pip3 install -r --no-cache-dir requirements.txt
/usr/local/src/squark-advana/virt/bin/python3 setup.py develop
cd ../data_catalog-statscli
/usr/local/src/squark-advana/virt/bin/pip3 install -r requirements.txt
