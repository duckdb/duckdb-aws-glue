#!/usr/bin/env sh
# Prints the environment for the tests that read a second Glue account (test_view_other_account.test):
# credentials of a role in account 222222222222 of the local moto (scripts/docker-compose.yml), which runs a request in
# the account of the role whose credentials signed it. Prints nothing if the moto container is not running, and the
# tests are skipped.
docker exec glue-moto python -c "
import boto3
sts = boto3.client('sts', endpoint_url='http://localhost:5000', region_name='us-east-1',
                   aws_access_key_id='testing', aws_secret_access_key='testing')
c = sts.assume_role(RoleArn='arn:aws:iam::222222222222:role/glue-test', RoleSessionName='glue-test')['Credentials']
print('GLUE_SECOND_CATALOG_ID=222222222222 GLUE_SECOND_KEY_ID=%s GLUE_SECOND_SECRET=%s GLUE_SECOND_SESSION_TOKEN=%s'
      % (c['AccessKeyId'], c['SecretAccessKey'], c['SessionToken']))
" 2>/dev/null
