Squark
#############


Setting up for local development
-------------------------------------

    $ mkvirtualenv -p $(which python3.5) squark 
    $ pip install -r requirements.txt 
    $ python setup.py develop 

Running with docker-compose 
----------------------------------

    $ docker network create advana
    $ make build 
    $ make up 
    $ make test 

