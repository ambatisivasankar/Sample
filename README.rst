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

Assuming you have squark set up locally and can run the squark CLI, next you source this repo's env.sh file to tell squark where to look for its config, then try accessing the squark config from python:

    $ python
    >>> from squark.config.environment import Environment
    >>> env = Environment()
    >>> env.sources.tpp
    JdbcSourceConfig([('type', 'jdbc'), ('user', 'moo'), ('password', '************'), ('schema', 'barn'),
    ('url', '"jdbc:sqlserver://somehost;databaseName=cow;UID=moo;PWD=************"')])

How to Edit
+++++++++++++

Currently only the .squark/secrets.cfg file is encrypted. To edit it, use ansible-vault:

    $ ansible-vault edit --vault-password-file ~/.squark-password .squark/secrets.cfg


