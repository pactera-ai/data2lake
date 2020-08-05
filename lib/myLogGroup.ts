import { ILogGroup, MetricFilter } from "@aws-cdk/aws-logs";

export default class MyLogGroup implements ILogGroup {
    public readonly logGroupArn: string;
    public readonly logGroupName: string;
    public stack: any;
    public node: any;

    constructor(logGroupArn: string, logGroupName: string) {
        this.logGroupArn = logGroupArn;
        this.logGroupName = logGroupName;
    }
    
    addMetricFilter(): any {}

    addStream(): any {}
    
    addSubscriptionFilter(): any {}

    extractMetric(): any {}

    grant(): any {}

    grantWrite(): any {}
}