const config_js = {
    "vpc": "10.0.0.0/17",
    "vpc_comment": "cidr for the vpc. E.G. 10.0.0.0/17",
    "serverName": "139.180.221.11",
    "serverName_comment": "source db's ip address OR endpoints. E.G. cdk-datalake-sourcedb-1.ceh5weqzlis6.us-west-2.rds.amazonaws.com OR 10.71.0.1",
    "port": 3306,
    "port_comment": "source db's port E.G. 3306",
    "username": "steve",
    "username_comment": "username that used to login to source db",
    "password": "gyy1994",
    "password_comment": "password",
    "engineName": "mysql",
    "engineName_comment": "engine type of the source db. E.G. postgress, mysql",
    "databaseName": "pcr_gvg",
    "databaseName_comment": "database name that the source data stored. DMS will only migrate the data in this database.",
    "tableList": [
        {
            "schemaName": "pcr_gvg",
            "schemaName_comment": "name of the schema where the data stored",
            "tableName": "student",
            "tableName_comment": "name of the table where the data stored"
        },
        {
            "schemaName": "pcr_gvg",
            "tableName": "Member"
        },
        {
            "schemaName": "new_schema",
            "tableName": "%"
        }
    ],
    "tableList_comment": "The data in the defined schema and table name would be transfered to data lake",
    "s3LifecycleRule": [{
        "enabled": true,
        "expiration": 10,
        "prefix": "prefix_",
        "abortIncompleteMultipartUploadAfter": 3
    }],
    "s3LifecycleRule_comment": "Raw S3 bucket lifecycle rule. ",
    "emailSubscriptionList": [
        "steveguo1024@gmail.com",
        "steve.guo@pactera.com"
    ],
    "emailSubscriptionList_comment": "Email address that used to receive the notification when problem occur.",
    "smsSubscriptionList": [
        "+61421986234"
    ],
    "smsSubscriptionList_comment": "Phone number that used to receive the notification when problem occur.",
    "executiveArn": "arn:aws:iam::193793567275:user/steve.guo",
    "executiveArn_comment": "The arn of the IAM user who would create the CloudFormation stack, that is, the arn of the secret key and access token."
}