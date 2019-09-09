# Development guide

## `squark` vs `squark-classic` 
Two `squark` directories exist in `squark-advana`, each with a different purpose.

`squark` 
 * Contains the core code that makes `squark` functional.
 
 `squark-advana`
 * Contains job files, scripts to process them, and associated files.

## New Branches
New feature branches should be branched off of the up to date version of `develop`.


### Branch naming guidelines
If a branch relates to a Jira ticket it should be named the same as the ticket number.
Additionally, branch names should follow one of the following formats:

|Name|Use case|
|----|--------|
|`feature/<ticket-number>`|New features|
|`bugfix/<ticket-number>`|Bugfixes|
|`source/<ticket-number>`|Changes to the `sources.cfg` file|
|`job/<ticket-number>`|Changes to the existing jobs, or addition of new jobs|
|`doc/<ticket-number>`|Changes to the documentation|
|`backup/<branch-name_YYYY_MM_DD>`|Backup branches|

The above is a guide, not all code changes are so self contained, please use your best judgement.

For example, a customer submitted a ticket `INGEST-12345` for new tables to be added to an existing job.
The branch for this work should be created like so:
```bash
$ git checkout develop
$ git pull
$ git checkout -b job/INGEST-12345
```

#### Branch Naming Warning!
Warning: Be careful with branch names! 
Making a branch named `something/something_else` will create file named `something_else` in a directory named `something`, inside the `.git/refs/` directory. 
This is fine. However, If _after_ that branch you then try to create a new branch named `something`, then there will be an error because git will try to create a file where a directotry with the same name already exists.

Vice versa is also true! 
Making a branch named `another_thing` will create file named `another_thing` inside the `.git/refs/` directory. 
This is fine. However, If _after_ that branch you then try to create a new branch named `another_thing/a_related_thing`, then there will be an error because git will try to create a directory where a fine with the same name already exists.

This may actually "work" in that you can create these branches, but cloning the repo could then cause an error.
If this happens you can fix the error using `git remote prune origin` and `git push --delete origin <bad_branch>`.

See here for more info [a-caution-about-git-branch-names-with-s](https://coderwall.com/p/qkofma/a-caution-about-git-branch-names-with-s) 

The moral of the story is that you _can_ have nested directories _if_ you plan it out ahead of time. 


## Commits
Use a good commit message.
 - Messages that begin with the ticket number will appear in Jira (optional)
 
Python files in `squark-classic` should be auto formatted using `black` before being committed with git:
```
$ black {source_file.py}
$ git add {source_file.py}
$ git commit -m "INGEST-12345 implemented xxxx"
```

## Branch Description
`master` - used in Production (jenkins-prod).

`develop`- used in QA (jenkins-qa) and develop (jenkins-dev). New jobs and features should branch off from here.

Other branches are either in active development, are temporary backups, or should be scheduled for deletion

## Encrypted Secrets
Our adventure with handling secrets like database connection parameters has arrived at encrypted config files as the least-unworkable solution for the time being, for a few reasons. A common practice in web development is to put secrets in environment variables as a means of preventing them from being checked into version control. This makes a certain amount of sense in the context of deploying web applications, where apps are commonly segregated into distinct virtual machines in the deployment environment, and storage volumes may be dyniamically allocated, unencrypted, and possibly-not-wiped when the VM is terminated. In that setting some people favor keeping secrets out of the filesystem. 

Using environment variables for secret storage is a mistake in a multi-user Linux environment, where any user can dump the environment of a another user's process to the console with `ps -ef`. When secrets are instead stored in config files, at least you get the benefit of filesystem permissions. One option is to take this approach one step further and strongly encrypt the config files. This also enables you to simply check the files into a git repo and distribute them with the rest of the source code. 

There are other options too, such as using an encrypted secret storage system like Hashicorp's Vault server, which stores secrets as encrypted binary objects in any of a number of different backends. Other job scheduling tools like Airflow include a database-backed encrypted storage facility. These options may very well be better than checking encrypted files into a git repo.  

### Reading and editing the encrypted files
__PLEASE DO NOT EDIT THE `sources.cfg` FILE__. Reach out to @Andrew-Sheridan.

To add (or edit) sources to Squark you'll need the Squark password file. This file is currently in the possession of the Data Management and Delivery [Ticket Squad](https://massmutual.atlassian.net/wiki/spaces/DMD/pages/854888007/Ticket+Squad). Save that file as `~/.squark-password`.
Currently only the `config/sources.cfg` file is encrypted. To edit it, use `ansible-vault`:
```
    $ ansible-vault edit --vault-password-file=.squark-password  config/sources.cfg
```

```
$ git checkout develop
$ git pull
$ git checkout -b source/INGEST-12345
$ ansible-vault edit --vault-password-file=.squark-password  config/sources.cfg
$ git add config/sources.cfg
$ git commit -m 'Added source for INGEST-12345`
$ git push -u origin source/INGEST-12345
```

After pushing, submit a Pull Request to merge into develop.

Again, *Please* make changes to the `sources.cfg` file in its own branch (`source/<ticket-number`), NOT the branch for a job.


## Environment variables
- see [ENV_VARIABLES.md](./ENV_VARIABLES.md)

## Job files
- see [job_files_description.md](./job_files_description.md)

## Python files in `squark-classic`
The Python script files in `squark-classic` are activated by shell scripts like `launch_squark_job.sh`

The list of Python files in `squark-classic` which are actually being used by Squark is:
* `all_tables.py` - Exports data from source into S3 as ORC files
* `copyddl.py` - Reads the source and generates a DDL
* `dirload.py` - Loads the ORC files from S3 into Vertica
* `new_utils.py` - New utilities generated during during refactoring, should be merged with `utils.py`
* `utils.py` - Various utility functions.

Note: `archived_functions.py` holds code found in Squark during tidying up, that isn't being used anywhere.

### Other Python files in`squark-classic`
* Are undocumented :(

### Environmental Variables in Python files in `squark-classic`
* Environmental variables are loaded in different ways. These variables can be broken down into five categories:  
    1. As-is (with KeyError) : Variables used exactly as they are set in the environment, but raise a KeyError if not set.
    2. As-is (with default) : Variables used exactly as they are set in the environment, but use a default value if not set.
    3. As-int : Variables converted to `int` after loading
    4. As-bool : Variables converted to `bool` after loading, based on their value falling into a pre-defined set
    5. Logic-applied : Variables loaded as above and then significantly changed
    
* Environmental variable keys are added to constant lists at the top of Python scripts. They are loaded into a dictionary `env_vars` at the start of `main()`.
* - see [ENV_VARIABLES.md](./ENV_VARIABLES.md) for a list of available variables

### Changes to the Python files in `squark-classic`
These files should be auto-formatted using `black` before being committed with git (see Commits section). This is done to both minimize diffs between commits, and to eliminate questions about formatting. 
Formatting can be disabled for a block using `# fmt: off` and re-enabled after the block with `# fmt: on`.

## Shell scripts in `squark-advana`

### `bootstrap-cluster.sh`
 - This is the file which will take care of the following steps:
 - For a list of available parameters see [usage_guide.md](./usage_guide.md)
 
### `launch_squark_job.sh`
 - This is the file which will run the Squark job. It controls how the jobs are run by setting the correct warehouse and Vertica connection variables.
 - For a list of available parameters see [usage_guide.md](./usage_guide.md)
 
### Other shell scripts
 - Are undocumented. :(
 
## Jar files:
Which Jar files? Where are they? How are they used? Find out and update this section.

SHASUMS:
* 2ba2e5646d1d0fa6ca17e8b794a9e7b6b8607d18  .jars/postgresql-9.4.1211.jre6.jar
* 18330ff836547c60dbf75d440a62d80b87671b45  .jars/py4jdbc-assembly-latest.jar
* de7e674823ec5010408859fa6b76961fa4fc49ac  .jars/vertica-jdbc-7.2.3-0.jar

