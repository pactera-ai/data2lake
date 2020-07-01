var AWS = require('aws-sdk');
exports.onEvent = async function(event, context) {
    console.log("request:", JSON.stringify(event, undefined, 2));
    var requestType = event['RequestType'];
    
    if (requestType === "Delete") {
        return {IsComplete: true};
    } else {
        return await checkProgress(event);
    }
}

function checkProgress(event) {
    return new Promise((resolve, reject) => {
        var buildId = event['PhysicalResourceId'];
        var props = event['ResourceProperties'];
        var projectName = props['ProjectName']
        var codebuild = new AWS.CodeBuild({apiVersion: '2016-10-06'});
        var params = {
            ids: [
                buildId
            ]
           };
        codebuild.batchGetBuilds(params, function(err, data) {
            if (err) {
                return reject(err);
            } else {
                var status = data.builds[0].buildStatus;
                if (status === "SUCCEEDED" || status === "FAILED") {
                    resolve({"IsComplete": true})
                } else {
                    resolve({"IsComplete": false})
                }
            }
        })
    });
}