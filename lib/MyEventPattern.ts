import { Construct } from "@aws-cdk/core";
import { EventPattern } from '@aws-cdk/aws-events';

export interface EventPatternProps {
    detailType: string[],
    detail: {
        [key: string]: any;
    },
    source: string[]
}

export default class MyEventPattern implements EventPattern{
    readonly version?: string[];
    readonly id?: string[];
    readonly detailType?: string[];
    readonly source?: string[];
    readonly account?: string[];
    readonly time?: string[];
    readonly region?: string[];
    readonly resources?: string[];
    readonly detail?: {
        [key: string]: any;
    };

    constructor(scope: Construct, id: string, props: EventPatternProps) {
        this.detail = props.detail;
        this.source = props.source;
        this.detailType = props.detailType;
    }
}