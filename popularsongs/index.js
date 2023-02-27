const s3 = require("@aws-sdk/client-s3");
const { DynamoDB } = require("@aws-sdk/client-dynamodb");
const { convertToAttr, convertToNative } = require("@aws-sdk/util-dynamodb");
const dynamoDb = new DynamoDB({ region: "us-east-1" });

const updateDB = (song, tableName)=>{
    console.log(convertToAttr(song));
  return {
    PutRequest: {
      Item: {
        id:convertToAttr(1),
        popularSongs: convertToAttr(song),
      
      },
      TableName: tableName,
    }
  };
}



const run = async (event) => {
  const response = {
    statusCode: 200,
    body: "",
  };
  try {
    const tableName = process.env.TABLE_NAME || "sorted-songs";
    const dataTable = process.env.DATA_TABLE_NAME || "songs-count";

    const results = await dynamoDb.scan({
      TableName: dataTable,
      ProjectionExpression: 'songID, songsItems',
    });
    const { Items, Count } = results;
    if (Items && Count) {      
            let i = 0;
            let arr = [];
            // console.log(Items);
            Items.forEach((item) => {
               const count = Number(convertToNative(item.songsItems))
                if (i<count) {
                    i=count;
                }
            });
            for (let j = 0; j <= i; j++) {
                Items.forEach((item) => {
                const count = Number(convertToNative(item.songsItems))
                if (count === j) {
                    const songID = convertToNative(item.songID)
                  arr.unshift(songID);
                }
              });
            }    
            console.log(arr); 
            const result = updateDB(arr,tableName)
            dynamoDb.batchWriteItem({
                RequestItems: {
                  [tableName]: [result]
                }
              });
        };
    
  } catch (e) {
    response.statusCode = 400;
    response.body = JSON.stringify(e.message);
  }
  console.log(response);
  return response;
};

exports.handler = async (event) => {
  return await run(event);
};
