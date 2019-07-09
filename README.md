# squark-advana

![Image](docs/imgs/squark_birds_eye_view.png?raw=true)

***

## Description

Squark exports data from a variety of sources into S3 and then loads that data into Vertica.
Jobs are launched with Jenkins.

This repo contains:

 * encrypted Squark configuration files
 * scripts to make installing and running Squark possible in Jenkins jobs
 * the core `squark` package
 * job files
 * Dockerfiles to enable local usage and development (currently non-functional)
 
 Note: `squark-advana` combines the no longer active repos `squark` and `squark-classic`.

## Links to documentation
 * Usage guide: [./docs/usage_guide.md](docs/usage_guide.md)
 * Development guide: [./docs/development_guide.md](docs/development_guide.md)
 * Description of the job files used to process jobs: [./docs/job_files_description.md](docs/job_files_description.md)
 * Description of the environmental variables used throughout Squark: [./docs/ENV_VARIABLES.md](docs/ENV_VARIABLES.md)
 * Guide to running squark locally: [./docs/running_squark_locally.md](docs/running_squark_locally.md)
