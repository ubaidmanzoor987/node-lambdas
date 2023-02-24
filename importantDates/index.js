const s3 = require("@aws-sdk/client-s3");
const { DynamoDB } = require("@aws-sdk/client-dynamodb");
const { convertToAttr, convertToNative } = require("@aws-sdk/util-dynamodb");
const dynamoDb = new DynamoDB({ region: "us-east-1" });
const fs = require("fs");

const streamToString = (stream) =>
  new Promise((resolve, reject) => {
    const chunks = [];
    stream.on("data", (chunk) => chunks.push(chunk));
    stream.on("error", reject);
    stream.on("end", () => resolve(Buffer.concat(chunks).toString("utf8")));
  });

const retrieveSongsFromS3 = async () => {
  try {
    const BUCKET_NAME = process.env.BUCKET_NAME || "songs-list";
    const FILE_NAME = process.env.FILE_NAME || "dates.json";

    if (BUCKET_NAME) {
      const getParams = {
        Bucket: BUCKET_NAME,
        Key: FILE_NAME,
      };

      const s3Client = new s3.S3Client({
        region: "us-east-1",
      });

      const command = new s3.GetObjectCommand(getParams);
      const response = await s3Client.send(command);
      const { Body } = response;
     return  JSON.parse(await streamToString(Body))
    
    }
  } catch (error) {
    console.log("error retrieveFileFromS3", error.message);
  }
};


const waiter = (waitTime) => {
  return new Promise((res) => {
    setTimeout(() => {
      res();
    }, waitTime);
  });
};

const batchWriteChunked = async (data, tableName) => {
  const chunkSize = 24;
  for (let i = 0; i < data.length; i += chunkSize) {
    const chunk = data.slice(i, i + chunkSize);
    let writeDone = false;
    do {
      await waiter(500);
      try {
        await dynamoDb.batchWriteItem({
          RequestItems: {
            [tableName]: chunk,
          },
        });
        writeDone = true;
      } catch (err) {
        console.log(err.toString());
        if (err.toString().includes("throughput")) {
        } else {
          throw err;
        }
      }
    } while (writeDone == false);
  }
};

const putDB = (data, tableName) => {
  return {
    PutRequest: {
      Item: {
        id: convertToAttr(data.id),
        individual: convertToAttr(data.individual),
        artist: convertToAttr(data.artist),
        dateTo: convertToAttr(data.date),
        gender: convertToAttr(data.gender),
        typeOf: convertToAttr(data.type),
      },
      TableName: tableName,
    },
  };
};

const deleteDB = (data, tableName) => {
  return {
    DeleteRequest: {
      Key: {
        id: convertToAttr(data),
      },
      TableName: tableName,
    },
  };
};

const retrieveDataFromDynamoDB = async (tableName) => {
  const results = await dynamoDb.scan({
    TableName: tableName,
    ProjectionExpression: "id,individual,artist,dateTo,gender,typeOf",
  });

  return results;
};

const run = async (event) => {
  const response = {
    statusCode: 200,
    body: "success",
  };
  try {
    const dynamoTable = "importantDates";
    
    const { dates } = await retrieveSongsFromS3();
    const arr = [];
    const s3IDs = [];
    const dynamoIDs = [];
    const { Items, Count } = await retrieveDataFromDynamoDB(dynamoTable);

    dates.forEach((item) => {
      for (let i = 0; i < Items.length; i++) {
        const dbItem = Items[i];
        const id = convertToNative(dbItem.id);
        if (item.id === id) {
            const individual = convertToNative(dbItem.individual);
            const artist = convertToNative(dbItem.artist);
            const dateTo = convertToNative(dbItem.dateTo);
            const gender = convertToNative(dbItem.gender);
            const typeOf = convertToNative(dbItem.typeOf);
    
          if (
            individual === item.individual &&
            dateTo === item.date &&
            gender === item.gender &&
            typeOf === item.type &&
            JSON.stringify(item.artist) === JSON.stringify(artist)
          ) {
          } else {
            arr.push(putDB(item, dynamoTable));
          }
          break;
        }
      }
      s3IDs.push(item.id);
    });
    Items.forEach((item) => {
      dynamoIDs.push(convertToNative(item.id));
    });

    const addItems = s3IDs.filter(function (e) {
      return dynamoIDs.indexOf(e) < 0;
    });
    const delItems = dynamoIDs.filter(function (e) {
      return s3IDs.indexOf(e) < 0;
    });

    addItems.forEach((i) => {
      const item = dates[i - 1];
      arr.push(putDB(item, dynamoTable));
    });

    delItems.forEach((i) => {
      arr.push(deleteDB(i - 1, dynamoTable));
    });

    if (arr.length !== 0) {
      await batchWriteChunked(arr, dynamoTable);
    }
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
