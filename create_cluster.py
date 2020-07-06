import boto3
import json
import time
from botocore.exceptions import ClientError
import configparser

config = configparser.ConfigParser()
config.read('dwh.cfg')

AWS_KEY                = config.get('AWS','AWS_KEY')
AWS_SECRET             = config.get('AWS','AWS_SECRET')

DWH_CLUSTER_TYPE       = config.get("DWH","DWH_CLUSTER_TYPE")
DWH_NUM_NODES          = config.get("DWH","DWH_NUM_NODES")
DWH_NODE_TYPE          = config.get("DWH","DWH_NODE_TYPE")

DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
DWH_DB                 = config.get("DWH","DWH_DB")
DWH_DB_USER            = config.get("DWH","DWH_DB_USER")
DWH_DB_PASSWORD        = config.get("DWH","DWH_DB_PASSWORD")
DWH_PORT               = config.get("DWH","DWH_PORT")

DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")


def create_clients():

    iam = boto3.client('iam',aws_access_key_id=AWS_KEY,
                        aws_secret_access_key=AWS_SECRET,
                        region_name='us-west-2'
                    )

    redshift = boto3.client('redshift',
                        region_name="us-west-2",
                        aws_access_key_id=AWS_KEY,
                        aws_secret_access_key=AWS_SECRET
                        )

    ec2 = boto3.resource('ec2',
                       region_name="us-east-1",
                       aws_access_key_id=AWS_KEY,
                       aws_secret_access_key=AWS_SECRET
                        )

    print("Created Clients")
    return iam, redshift, ec2


def create_iam_role(iam):

    try:
        print("Creating a new IAM Role") 
        dwhRole = iam.create_role(
            Path='/',
            RoleName=DWH_IAM_ROLE_NAME,
            Description = "Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                'Effect': 'Allow',
                'Principal': {'Service': 'redshift.amazonaws.com'}}],
                'Version': '2012-10-17'})
        )    
    except Exception as e:
        print(e)
        
    print("Attaching Policy")
    iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                        PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                        )['ResponseMetadata']['HTTPStatusCode']

    return iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']


def create_redshift_cluster(iam, redshift, ec2):

    try:
        roleArn = create_iam_role(iam)
        response = redshift.create_cluster(        
            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            NumberOfNodes=int(DWH_NUM_NODES),
            #Identifiers & Credentials
            DBName=DWH_DB,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUsername=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,
            #Roles (for s3 access)
            IamRoles=[roleArn]  
        )

    except Exception as e:
        print(e)

    # Wait until the cluster is available for further operations
    while True:
        response = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)
        cluster_info = response['Clusters'][0]
        if cluster_info['ClusterStatus'] == 'available':
            print("The cluster is ready")
            break
        time.sleep(10)

    myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]

    # Open incoming TCP port
    try:
        vpc = ec2.Vpc(id=myClusterProps['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        print(defaultSg)
        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(DWH_PORT),
            ToPort=int(DWH_PORT)
        )
    except Exception as e:
        print(e)

    print("IAM Role ARN:  {}".format(myClusterProps['IamRoles'][0]['IamRoleArn']))
    print("Endpoint:  {}".format(myClusterProps['Endpoint']['Address']))

def main():

    iam, redshift, ec2 = create_clients()
    create_redshift_cluster(iam, redshift, ec2)

if __name__ == "__main__":
    main()