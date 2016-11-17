#Advana's Custom Squark Config

This repo contains encrypted Squark configuration files and a couple of scripts to make installing squark easier in Jenkins jobs. 

###What the
---

Our adventure with handling secrets like database connection parameters has arrived at envrypted config files as the least-unworkable solution for the time being, for a few reasons. A commmon practice in web development is to put secrets in environment variables as a means of preventing them from being checked into version control. This makes a certain amount of sense in the context of deploying web application, where apps are commonly segregated into distinct virtual machines in the deployment environment, and storage volumes may be dyniamically allocated, unencrypted, and possibly-not-wiped when the VM is terminated. In that setting some people favor keeping secrets out of the filesystem. 

Using environment variables for secret storage is a mistake in a multi-user Linux environment, where any user can dump the environment of a another user's process to the console with ps -ef. When secrets are instead stored in config files, at least you get the benefit of filesystem permissions. One option is to take this approach one step further and stronly encrypt the config files. This also enables you to simply check the files into a git repo and distribute them with the rest of the source code. 

There are other options too, such as using an encrypted secret storage system like Hashicorp's Vault server, which stores secrets as encypted binary objects in any of a number of different backends. Other job scheduling tools like Airflow include a database-backed encrypted storage facility. These options may very well be better than checking encrypted files into a git repo.  

###How to Use
---

To use this Squark config repo, you'll need the Squark password file. Contact me (tneale@massmutual.com) and I'll email it to you. Next, save that file as ~/.squark-password or choose a different location and edit ~/.env.sh with that location as the SQUARK_PASSWORD_FILE environment variable. Now when you run Squark, it will use this repo for its config. 

###How to Edit
---

Currently only the .squark/secrets.cfg file is encrypted. To edit it, use ansible-vault:
```
    $ ansible-vault edit --vault-password-file ~/.squark-password .squark/secrets.cfg
```

###RUNNING SQUARK-CLASSIC IN CONTAINER 
---

Please follow the steps below to run the squark-classic container:

1. git submodule init; git submodule update
2. cd squark/
3. make build
4. make up
5. ./build-squark-classic.sh
6. ./run-squark-classic.sh  (This will put you in a bash shell in the container)
  * ./docker-entrypoint.sh <JOB_NAME>

Here is an example of a typical workflow once the squark-container is run:

1. ./docker-entrypoint.sh test_psql

This will source the job file with that name. 

That should be it. Most environment variables are being set in the docker file, but can be overridden in the shell if needed.

To add new jars: Just copy them into the .jars directory.

###RUNNING SQUARK-CLASSIC (No Docker)
---

To Run squark-classic locally - without containerization - then run:

1. `source "./env-cluster.sh" - This will set the variables needed (related to the cluster setup).
2. `./bootstrap-cluster.sh <dev|prod>` - This will setup the virtual environment and setup the git submodules according to the dev or prod setup.
3. `/launch_squark_job.sh [--dev|--prod] <job_name> - This will set the standard variables for running the job and will source the proper job file and run the job.

###Notes on files:
---
*bootstrap-cluster.sh* - This is the file which will take care of the following steps:

1. Update the submodules and pull the selected branch.
2. Check to make sure that the correct password variables are set.
3. Create the .squark_password file.
4. Create the virtual environment and install the correct libraries.

bootstrap cluster takes a single argument, which can be any of the following:

* `dev` or `develop` - This tells the script to pull down the `develop` branch in each of the submodules.
* `prod` or `production` or `master` - This tells the script to pull down the `master` branch in the submodules.

**launch_squark_job.sh** - This is the file which will run the squark job. It controls how the jobs are run by setting the correct warehouse and vertica connection variables.

It has a few defaults that can be used:

`develop`: 

1. VERTICA_CONNECTION_ID='vertica_dev'
2. WAREHOUSE_DIR='/_wh_dev/'

`production`:

1. VERTICA_CONNECTION_ID='vertica_prod'
2. WAREHOUSE_DIR='/_wh/'

It takes the following arguments:

1. --dev | --develop : This tells the script to use default development variables.
2. --prod | --production : This tells the script to use the default production variables.
3. -w=* | --warehouse-dir=* : This tells the script to use the value after the equals sign as the warehouse-dir.
4. -v=* | --vertica-id=* : This tells the script to use the value after the equals sign as the vertica-connection-id.
5. <job_name> : This is a required arguement, and it tells the script which job to run.

###Example of running a job on the cluster:
---

Below is the execute shell for a jenkins job on the cluster:

```
source "./env-cluster.sh"
./bootstrap-cluster.sh dev
./launch_squark_job --dev teradata_cmn
```


###BELOW ARE THE SHASUMS OF THE JAR FILES:
---

*. 2ba2e5646d1d0fa6ca17e8b794a9e7b6b8607d18  .jars/postgresql-9.4.1211.jre6.jar
*. 18330ff836547c60dbf75d440a62d80b87671b45  .jars/py4jdbc-assembly-latest.jar
*. de7e674823ec5010408859fa6b76961fa4fc49ac  .jars/vertica-jdbc-7.2.3-0.jar

