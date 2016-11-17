virtualenv -p /usr/bin/python3 virt
/usr/local/src/squark-advana/virt/bin/pip3 install -r squark-classic/requirements.txt
cd squark
/usr/local/src/squark-advana/virt/bin/pip3 install -r requirements.txt
/usr/local/src/squark-advana/virt/bin/python3 setup.py develop
