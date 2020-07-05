import boto3
import json
from botocore.exceptions import ClientError
import configparser

config = configparser.ConfigParser()
config.read('dwh.cfg')

AWS_ACCESS_KEY_ID      = config.get('AWS','KEY')
AWS_SECRET_ACCESS_KEY  = config.get('AWS','SECRET')
DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
DWH_IAM_ROLE_NAME      = config.get('DWH', "DWH_IAM_ROLE_NAME")

def main():

    redshift = boto3.client('redshift',
                        region_name="us-west-2",
                        aws_access_key_id=AWS_ACCESS_KEY_ID,
                        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
                        )
    iam = boto3.client('iam',aws_access_key_id=AWS_ACCESS_KEY_ID,
                        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                        region_name='us-west-2'
                    )

    # DELETE CLUSTER
    redshift.delete_cluster( ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,  SkipFinalClusterSnapshot=True)
    print("Deleting cluster {}".format(DWH_CLUSTER_IDENTIFIER))
    while True:
        try:
            response = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)
            cluster_info = response1['Clusters'][0]
            time.sleep(10)
        except:

            print("Deleted!")
            break

    # DELETE IAM ROLE
    iam.detach_role_policy(RoleName=DWH_IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
    iam.delete_role(RoleName=DWH_IAM_ROLE_NAME)

if __name__ == "__main__":
    main()