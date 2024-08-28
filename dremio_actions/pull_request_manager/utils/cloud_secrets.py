import json

import boto3
from botocore.exceptions import ClientError


class CloudSecrets:

    secret_string = 'SecretString'

    def __init__(self, region, secret):
        self.region_name = region
        self.secret_name = secret
        self.SECRETS_MANAGER = 'secretsmanager'

    def get_secret(self, key):
        # Create a Secrets Manager client
        session = boto3.session.Session()
        client = session.client(
            service_name=self.SECRETS_MANAGER,
            region_name=self.region_name
        )

        try:
            get_secret_value_response = client.get_secret_value(
                SecretId=self.secret_name
            )
        except ClientError as e:
            # For a list of exceptions thrown, see
            # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
            raise e

        # Decrypts secret using the associated KMS key.
        secret = json.loads(get_secret_value_response[self.secret_string])

        return secret[key]
