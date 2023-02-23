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
  let songs = [];

  try {
    const BUCKET_NAME = process.env.BUCKET_NAME || "songs-list";
    const FILE_NAME = process.env.FILE_NAME || "songbook.json"

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
      if (Body) {
        songs = await streamToString(Body);
        if (songs && songs.length > 0) {
          songs = JSON.parse(songs);
          if ("Songs" in songs) {
            songs = songs["Songs"];
          }
        }
      }
    }
  } catch (error) {
    console.log("error retrieveFileFromS3", error.message);
  }
  return songs;
};

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

const run = async (event) => {
  const response = {
    statusCode: 200,
    body: "",
  };
  try {
    const songs = await retrieveSongsFromS3();
    if (songs.length === 0) {
      response.message = "Songs List is empty";
      return response;
    }
    const tableName = process.env.TABLE_NAME || "songs-data";

    const results = await dynamoDb.scan({
      TableName: tableName,
      ProjectionExpression: "playlistid, songid",
    });
    const { Items, Count } = results;
    if (Items && Count && Count > 0) {
      const processedSongs = [];
      for (let i = 0; i < songs.length; i++) {
        const song = songs[i];
        for (let j = 0; j < Items.length; j++) {
          const item = Items[j];
          if (
            song.playlistid === convertToNative(item.playlistid) &&
            song.songid !== convertToNative(item.songid)
          ) {
            processedSongs.push(song);
          }
        }
      }

      console.log({tableName})
      const songsResusts1 = processedSongs.map((song) => {
        return {
          PutRequest: {
            Item: {
              playlistid: convertToAttr(song.playlistid),
              songid: convertToAttr(song.songid ? song.songid : ""),
            },
            TableName: tableName,
          },
        };
      })
      
      await batchWriteChunked(songsResusts1, tableName);

      const unprocessedSongs = songs.filter((x) => {
        return !processedSongs.includes(x)
      });

      const songsResusts = unprocessedSongs.map((song) => {
        return {
          PutRequest: {
            Item: {
              playlistid: convertToAttr(song.playlistid),
              songid: convertToAttr(song.songid ? song.songid : ""),
            },
            TableName: tableName,
          },
        };
      })

      await batchWriteChunked(songsResusts, tableName);

    } else {

      const songsResusts = songs.map((song) => {
        return {
          PutRequest: {
            Item: {
              playlistid: convertToAttr(song.playlistid),
              songid: convertToAttr(song.songid ? song.songid : ""),
            },
            TableName: tableName,
          }
        };
      })

      await batchWriteChunked(songsResusts, tableName);
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


exports.handler();
