# Running `squark` locally

Currently the code to run `squark` locally (in a container or otherwise) is no longer functional. If you get it working, submit a PR, and change this document.

***
For example, there are no longer git submodules so there is no need to init them, and `make build` fails on multiple steps. Good luck!
***

### Running `squark-classic` in a Container 


Please follow the steps below to run the `squark-classic` container:

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

### Running `squark-classic` (No Container)

---

To Run `squark-classic` without containerization do:

1. `source "./env-cluster.sh"` - This will set the variables needed (related to the cluster setup).
2. `./bootstrap-cluster.sh <dev|prod>` - This will setup the virtual environment and setup the git submodules according to the dev or prod setup.
3. `/launch_squark_job.sh [--dev|--prod] <job_name>` - This will set the standard variables for running the job and will source the proper job file and run the job.
