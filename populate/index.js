const {DynamoDB} = require("@aws-sdk/client-dynamodb");
const dynamoDb =    new DynamoDB ({ region: "us-east-1"});
const { convertToAttr, convertToNative } = require("@aws-sdk/util-dynamodb");

const updateDB = (songid,userid,tableName)=>{
    console.log(convertToAttr(userid),userid,tableName);
    return {
      PutRequest: {
        Item: {
          userID: convertToAttr(userid),
          songID: convertToAttr(songid),
          ttl: convertToAttr(new Date().getTime())
        },
        TableName: tableName,
      }
    };
  }

  const run = async (event) => {
    console.log(event)
    const userid = event.queryStringParameters.userid
    const songid = event.queryStringParameters.songid
    const response = {
      statusCode: 200,
      body: "",
    };
    try {
      const tableName = process.env.TABLE_NAME || "popularSongsDB";
      console.log(tableName);
  
      const results = await dynamoDb.query({
        TableName: tableName,
        ProjectionExpression: "userID, songID",
        KeyConditionExpression: `userID=:userID`,
        FilterExpression: 'songID = :songID',
        ExpressionAttributeValues: {
            ':userID': convertToAttr(userid),
            ':songID': convertToAttr(songid)
        }
      });
      const { Items, Count } = results;
      console.log(Items)
      if (Items.length > 0) {
        return response
      }

      try{
        const result = updateDB(songid,userid,tableName)
        const promise = await dynamoDb.batchWriteItem({
          RequestItems: {
            [tableName]: [result]
          }
        });
        console.log(promise);

      } catch (err) {
        console.log(err.toString())
        if (err.toString().includes("throughput")) {
        } else {
          throw err
        }
      }
    
    } catch (e) {
      response.statusCode = 400;
      response.body = JSON.stringify(e.message);
      throw e
    }
    console.log(response);
    return response;
  };

  exports.handler = async (event) => {
    console.log(event)
    return await run(event);
  };

//   const event = {
//     data: {
//         songid: '123',
//         userid: '23'
//     }
//   }

//   run(event)