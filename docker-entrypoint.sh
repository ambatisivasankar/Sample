set -e

cd /usr/local/src/squark-advana/squark-classic
source env-container.sh
source jobs/$1.sh
./run.sh

./load_wh.sh $1
