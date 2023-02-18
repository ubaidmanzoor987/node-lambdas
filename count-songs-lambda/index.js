const s3 = require("@aws-sdk/client-s3");
const { DynamoDB } = require("@aws-sdk/client-dynamodb");
const { convertToAttr, convertToNative } = require("@aws-sdk/util-dynamodb");
const dynamoDb = new DynamoDB({ region: "us-east-1" });

const waiter = (waitTime) => {
  return new Promise((res) => {
    setTimeout(() => {
      res()
    }, waitTime)
  });
}

const  batchWriteChunked = async (data, tableName) => {
  const chunkSize = 24;
  for (let i = 0; i < data.length; i += chunkSize) {
    const chunk = data.slice(i, i + chunkSize);
    let writeDone = false
    do {
      await waiter(500);
      try{
        await dynamoDb.batchWriteItem({
          RequestItems: {
            [tableName]: chunk
          }
        });
        writeDone = true
      } catch (err) {
        console.log(err.toString())
        if (err.toString().includes("throughput")) {
        } else {
          throw err
        }
      }
    } while(writeDone == false)
  }
}


const updateDB = (songid, count,tableName)=>{
    console.log(songid, count);
  return {
    PutRequest: {
      Item: {
        songID: convertToAttr(songid),
        songsItems: convertToAttr(count),
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
    const tableName = process.env.TABLE_NAME || "songs-count";
    const dataTable = process.env.DATA_TABLE_NAME || "popularSongsDB";

    const results = await dynamoDb.scan({
      TableName: dataTable,
      ProjectionExpression: "userID, songID",
    });
    const { Items, Count } = results;
    let songIds = []
    let songList = {}
    if (Items && Count) {      
      Items.forEach((item)=>{
        const songID = convertToNative(item.songID)
        if(songIds.indexOf(songID) === -1){
          songIds.push(songID)
          songList[songID] = 1;
        }
        else{
            songList[songID] +=1;
        }
      })
    }
    const result = Object.keys(songList).map((songItem)=>{
        const count = String(songList[songItem])
        return updateDB(songItem,count,tableName)
    })
    await batchWriteChunked(result,tableName)
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
