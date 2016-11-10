Advana's Custom Squark Config
##############################

This repo contains encrypted Squark configuration files and a couple of scripts to make installing squark easier in Jenkins jobs. 

What the
++++++++++

Our adventure with handling secrets like database connection parameters has arrived at envrypted config files as the least-unworkable solution for the time being, for a few reasons. A commmon practice in web development is to put secrets in environment variables as a means of preventing them from being checked into version control. This makes a certain amount of sense in the context of deploying web application, where apps are commonly segregated into distinct virtual machines in the deployment environment, and storage volumes may be dyniamically allocated, unencrypted, and possibly-not-wiped when the VM is terminated. In that setting some people favor keeping secrets out of the filesystem. 

Using environment variables for secret storage is a mistake in a multi-user Linux environment, where any user can dump the environment of a another user's process to the console with ps -ef. When secrets are instead stored in config files, at least you get the benefit of filesystem permissions. One option is to take this approach one step further and stronly encrypt the config files. This also enables you to simply check the files into a git repo and distribute them with the rest of the source code. 

There are other options too, such as using an encrypted secret storage system like Hashicorp's Vault server, which stores secrets as encypted binary objects in any of a number of different backends. Other job scheduling tools like Airflow include a database-backed encrypted storage facility. These options may very well be better than checking encrypted files into a git repo.  

How to Use
+++++++++++

To use this Squark config repo, you'll need the Squark password file. Contact me (tneale@massmutual.com) and I'll email it to you. Next, save that file as ~/.squark-password or choose a different location and edit ~/.env.sh with that location as the SQUARK_PASSWORD_FILE environment variable. Now when you run Squark, it will use this repo for its config. 

How to Edit
+++++++++++++

Currently only the .squark/secrets.cfg file is encrypted. To edit it, use ansible-vault:

    $ ansible-vault edit --vault-password-file ~/.squark-password .squark/secrets.cfg


RUNNING SQUARK-CLASSIC IN CONTAINER 
+++++++++++++++++++++++++++++++++++

Please follow the steps below to run the squark-classic container:

1. git submodule init; git submodule update
2. cd squark/
3. make build
4. make up
5. ./build-squark-classic.sh
6. ./run-squark-classic.sh  (This will put you in a bash shell in the container)
   a. ./docker-entrypoint.sh <JOB_NAME>

Here is an example of a typical workflow once the squark-container is run:
1. ./docker-entrypoint.sh test_psql

This will source the job file with that name. 

That should be it. Most environment variables are being set in the docker file, but can be overridden in the shell if needed.

To add new jars: Just copy them into the .jars directory.

RUNNING SQUARK-CLASSIC (No Docker)
++++++++++++++++++++++++++++++++++

To Run squark-classic locally - without containerization - then run:
1. `./bootstrap.sh` - This will setup the virtual environment.
2. Setup a environment file with the following values (see example for container env, env-container.sh):
   a. VERTICA_HOST
   b. VERTICA_USER
   c. VERTICA_PASSWORD
   d. VERTICA_DATABASE
   e. VERTICA_VSQL (This is the path to the vsql binary)
   f. HDFS_HOST
   g. HDFS_PORT
   h. HDFS_USER
3. Source that new environment file.
4. Source the job file that you are looking to run.
5. `./run.sh`
6. `./load_wh.sh <JOB_NAME>`


BELOW ARE THE SHASUMS OF THE JAR FILES:
+++
2ba2e5646d1d0fa6ca17e8b794a9e7b6b8607d18  .jars/postgresql-9.4.1211.jre6.jar
18330ff836547c60dbf75d440a62d80b87671b45  .jars/py4jdbc-assembly-latest.jar
de7e674823ec5010408859fa6b76961fa4fc49ac  .jars/vertica-jdbc-7.2.3-0.jar

