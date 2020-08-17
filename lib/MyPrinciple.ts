import * as DataLake from '@aws-cdk/aws-lakeformation';
import { Construct } from "@aws-cdk/core";

export class MyPrinciple implements DataLake.CfnDataLakeSettings.DataLakePrincipalProperty {
    public readonly dataLakePrincipalIdentifier: string;

    constructor(scope: Construct, id: string, props?: DataLake.CfnDataLakeSettings.DataLakePrincipalProperty) {
        this.dataLakePrincipalIdentifier = props?.dataLakePrincipalIdentifier ? props?.dataLakePrincipalIdentifier : '';
    }
}