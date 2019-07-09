# Usage Guide

How to use Squark? Currently Squark is only usable in Jenkins.

## Jenkins

Setting a job in Jenkins

### Source Code Management
Make sure to grab the correct branch:

Set the Repository URL to https://github.com/massmutual/squark-advana.git

Set the Branch Specifier to
* `*/master` - used in Production (Jenkins-prod).
* `*/develop`- used in QA (Jenkins-qa) and develop (Jenkins-dev)
* `*/<some_other_branch>` - used in develop (Jenkins-dev)

### Build - Execute Shell

#### Jenkins Dev Settings
Build - Execute Shell:
```
# Overwrite the previous vertica password
export AWS_VERTICA_PASSWORD=$AWS_EON_VERTICA_PASSWORD

squark_name="blender"
source "./aws-env-cluster.sh"

./bootstrap-cluster.sh dev

export AWS_VERTICA_HOST=$AWS_VERTICA_DEV_EON
export AWS_VERTICA_PORT=5433
export SQUARK_NUM_RETRY=3
export SQUARK_BUCKET=nonprd-squark-dev

# Export new data to S3 but do not load to Vertica
./launch_squark_job.sh --dev --s3-id=s3_nprd --vertica-id=vertica_aws_nprd_dev --use-aws --load-from-aws $squark_name --skip-vertica-load

# Do not export new data to S3, just load exisiting data from S3 into Vertica
#./launch_squark_job.sh --dev --s3-id=s3_nprd --vertica-id=vertica_aws_nprd_dev --use-aws --load-from-aws $squark_name --skip-hdfs-load

# export from source to s3 and load into vertica
#./launch_squark_job.sh --dev --s3-id=s3_nprd --vertica-id=vertica_aws_nprd_dev --use-aws --load-from-aws $squark_name 
```

#### Jenkins QA Settings
Build - Execute Shell:
```
# Overwrite the previous vertica password
export AWS_VERTICA_PASSWORD=$AWS_EON_VERTICA_PASSWORD

squark_name="ddnb1"
source "./aws-env-cluster.sh"

./bootstrap-cluster.sh dev

export AWS_VERTICA_HOST=${AWS_VERTICA_QA}
export AWS_VERTICA_PORT=5433
export SQUARK_NUM_RETRY=3
export SQUARK_BUCKET=nonprd-squark-dev


# Uncomment the line you want to use:

# Export new data to S3 but do not load to Vertica
#./launch_squark_job.sh --qa --s3-id=s3_nprd --vertica-id=vertica_aws_nprd_qa --use-aws --load-from-aws $squark_name --skip-vertica-load

# Do not export new data to S3, just load exisiting data from S3 into Vertica
#./launch_squark_job.sh --qa --s3-id=s3_nprd --vertica-id=vertica_aws_nprd_qa --use-aws --load-from-aws $squark_name --skip-hdfs-load

# Export to S3 and load to Vertica
#./launch_squark_job.sh --qa --s3-id=s3_nprd --vertica-id=vertica_aws_nprd_qa --use-aws --load-from-aws ${squark_name} 
```

#### Jenkins Prod Settings
Build - Execute Shell:
```
# Overwrite the previous vertica password
export AWS_VERTICA_PASSWORD=$AWS_EON_VERTICA_PASSWORD

squark_name="ddnb1"
source "./aws-env-cluster.sh"

./bootstrap-cluster.sh prod

export AWS_VERTICA_HOST=${AWS_VERTICA_PROD}
export AWS_VERTICA_PORT=5433
export SQUARK_NUM_RETRY=3
export SQUARK_BUCKET=dsprd-squark-prd


# Uncomment the line you want to use:

# Export new data to S3 but do not load to Vertica
#./launch_squark_job.sh --prod --s3-id=s3_prd --vertica-id=vertica_aws_prd_prod --use-aws --load-from-aws $squark_name --skip-vertica-load

# Do not export new data to S3, just load exisiting data from S3 into Vertica
#./launch_squark_job.sh --prod --s3-id=s3_prd --vertica-id=vertica_aws_prd_prod --use-aws --load-from-aws $squark_name --skip-hdfs-load

# Export to S3 and load to Vertica
#./launch_squark_job.sh --prod --s3-id=s3_prd --vertica-id=vertica_aws_prd_prod --use-aws --load-from-aws ${squark_name} 
```

## Script Parameters

### `bootstrap-cluster.sh`
Note: This section is _absolutely_ out of date. 

`bootstrap-cluster.sh` takes a single argument, which can be any of the following:
* `dev` or `develop` - This tells the script to pull down the `develop` branch in each of the submodules.
* `prod` or `production` or `master` - This tells the script to pull down the `master` branch in the submodules.

### `launch_squark_job.sh`
Note:
 > This section is _absolutely_ out of date. <br>
Please use the patterns described above in the Jenkins Settings sections. <br>
Below section is out of date and needs to be fixed (along with the code to which it references) <br>
Below section is not comprehensive. Some parameters conflict with others. <br>
Usage outside of above prescribed patterns may lead to unexpected behavior. <br>

 Below was true at one point: <br>
`launch_squark_job.sh` has a few defaults that can be used.

| Argument | Description|
|--- | --- |
|`--dev` or  `--develop` | This tells the script to use default development variables.|
|`--prod` or `--production` | This tells the script to use the default production variables.|
|`--test` | This tells the script to use default test variables (use in a docker container).|
|`--s3-id=*` | This tells the script which key to use for the S3 bucket credentials to use from `sources.cfg`. (s3_prd for production, s3_nprd for qa, s3_nprd for dev)|
|`-w=*` or `--warehouse-dir=*` | This tells the script to use the value after the equals sign as the warehouse-dir.|
|`-v=*` or `--vertica-id=*` | This tells the script to use the value after the equals sign as the vertica-connection-id key for `sources.cfg`. (vertica_aws_prd_prod for production, vertica_aws_nprd_qa for qa, vertica_aws_nprd_dev for dev)  |
|`--skip-hdfs-load` | This tells squark to not load data into hdfs (or s3 if --use-aws option is passed).|
|`--skip-vertica-load` | This tell squark to not load data into vertica.|
|`--use-aws` | This tells squark to save the data into s3 (can be used with --use-hdfs to save into both).|
|`--use-hdfs` | This tells squark to save the data into hdfs (can be used with --use-aws to save into both).|
| `--load-from-aws` | This tells squark to load the data from s3 into aws-vertica.|
| `--force-cutover` | This will force the program to cutover the warehouse dirs. By default, cutover will happen if a full run happens.|
| `--skip-cutover` | This will skip the cutover, useful if you don't want a cutover to happen, even during a full run.|
| `job_name` | This is a required argument, and it tells the script which job to run. Just pass the name of the job here. this is also referenced as `squark_name` elsewhere in Squark|
