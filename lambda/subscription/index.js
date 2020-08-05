var AWS = require('aws-sdk');
var zlib = require('zlib');
var sns = new AWS.SNS();

exports.handler = function(input, context) {
    var payload = Buffer.from(input.awslogs.data, 'base64');
    zlib.gunzip(payload, function(e, result) {
        if (e) { 
            context.fail(e);
        } else {
            result = JSON.parse(result.toString('ascii'));
            var msg = "Event Data:" + JSON.stringify(result, null, 2);
            console.log(msg);
            var sns_params = {
                Message: '[ERROR] - ' + msg,
                Subject: 'ERROR DETECTED',
                // TargetArn: 'STRING_VALUE',
                TopicArn: process.env.TOPIC_ARN
            };
            sns.publish(sns_params, function(err, data) {
                if (err) console.log(err, err.stack); // an error occurred
                else     console.log(data);           // successful response
                context.succeed();
            });
        }
    });
};