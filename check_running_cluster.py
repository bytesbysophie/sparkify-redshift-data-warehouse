import boto3
import configparser

config = configparser.ConfigParser()
config.read('dwh.cfg')

AWS_KEY      = config.get('AWS','AWS_KEY')
AWS_SECRET  = config.get('AWS','AWS_SECRET')


def main():

    redshift = boto3.client('redshift',
                        region_name="us-west-2",
                        aws_access_key_id=AWS_KEY,
                        aws_secret_access_key=AWS_SECRET,
                        )

    print(redshift.describe_clusters())



if __name__ == "__main__":
    main()