var AWS = require('aws-sdk');
exports.onEvent = async function(event, context) {
    console.log("request:", JSON.stringify(event, undefined, 2));
    var requestType = event['RequestType']
    if (requestType === 'Create' || requestType === 'Update') {
        return await startCodeBuild(event)
    } else if (requestType == 'Delete') {
        var physicalResourceId = event['PhysicalResourceId']
        return { 'PhysicalResourceId': physicalResourceId }
    }
    throw "Invalid request type " + requestType;
}
function startCodeBuild(event) {
    var props = event['ResourceProperties'];
    var projectName = props['ProjectName']
    console.log("project name ", projectName)
    var codebuild = new AWS.CodeBuild({apiVersion: '2016-10-06'});
    var params = {
        projectName: projectName
    };
    return new Promise((resolve, reject) => {
        codebuild.startBuild(params, function (err, data) {
            if (err) {
                console.log(err, err.stack);
                reject(err);
            } else {
                console.log('start build response: ', JSON.stringify(data));
                var id = data.build.id;
                resolve({'PhysicalResourceId': id});
            } 
        })
    });

}