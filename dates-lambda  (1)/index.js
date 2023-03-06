const s3 = require("@aws-sdk/client-s3");
const { DynamoDB } = require("@aws-sdk/client-dynamodb");
const { convertToAttr, convertToNative } = require("@aws-sdk/util-dynamodb");
const dynamoDb = new DynamoDB({ region: "us-east-1" });

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
      return JSON.parse(await streamToString(Body));
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

function ltrim(str) {
  str = str.toLowerCase()
  if(!str) return str;
  return str.replace(/^\s+/g, '');
}


const putDB = (data, tableName) => {
  const date = new Date(data.date).getDate();
  const month = new Date(data.date).getMonth()+1;
  const Bdate = Number(`${month}${date}`)
  const artists = data.artist.map((str)=>{
    return ltrim(str)
  })
  return {
    PutRequest: {
      Item: {
        id: convertToAttr(data.id),
        individual: convertToAttr(data.individual),
        artist: convertToAttr(artists),
        dateTo: convertToAttr(data.date),
        gender: convertToAttr(data.gender),
        typeOf: convertToAttr(data.type),
        Bdate: convertToAttr(Bdate),
        gsi1pk: convertToAttr('Song')
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
    ProjectionExpression: "id,individual,artist,dateTo,gender,typeOf"
  });

  return results;
};

const run = async (event) => {
  const response = {
    statusCode: 200,
    body: "success",
  };
  try {
    const TABLE_NAME = process.env.TABLE_NAME || "importantDates";

    const { dates } = await retrieveSongsFromS3();
    const arr = [];
    const s3IDs = [];
    const dynamoIDs = [];
    const { Items, Count } = await retrieveDataFromDynamoDB(TABLE_NAME);

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
          let artArr = item.artist.filter((e) => {
            e = ltrim(e)
            return artist.indexOf(e) < 0;
          });

          if (
            individual === item.individual &&
            dateTo === item.date &&
            gender === item.gender &&
            typeOf === item.type &&
            artArr.length===0
          ) {
          } else {
            arr.push(putDB(item, TABLE_NAME));
          }
          break;
        }
      }
      s3IDs.push(item.id);
    });

    Items.forEach((item) => {
      dynamoIDs.push(convertToNative(item.id));
    });

    const addItems = s3IDs.filter((e) => {
      return dynamoIDs.indexOf(e) < 0;
    });

    const delItems = dynamoIDs.filter((e) => {
      return s3IDs.indexOf(e) < 0;
    });

    addItems.forEach((i) => {
      const item = dates.filter((date) => date.id === i);
      if (item.length > 0) {
        arr.push(putDB(item[0], TABLE_NAME));
      }
    });

    delItems.forEach((i) => {
      const item = Items.filter((item) => convertToNative(item.id) === i);
      if (item.length > 0) {
        const id = convertToNative(item[0].id);
        arr.push(deleteDB(id, TABLE_NAME));
      }
    });

    if (arr.length !== 0) {
      await batchWriteChunked(arr, TABLE_NAME);
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

// run()