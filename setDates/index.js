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
  let playlists = [];

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
        let songsData = await streamToString(Body);
        if (songsData && songsData.length > 0) {
          songsData = JSON.parse(songsData);
          if ("Songs" in songsData && "Playlists" in songsData) {
            songs = songsData["Songs"];
            playlists = songsData["Playlists"];
          }
        }
      }

    }
  } catch (error) {
    console.log("error retrieveFileFromS3", error.message);
  }
  return {songs,playlists};
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


const updateDB = (song,playlists,tableName)=>{
  let playlistname = ''
  let visible = false,dateto = "",datefrom= "";

  playlists.forEach(element => {
    if(element.playlistid === song.playlistid){
        playlistname = element.playlistname
        visible = element.visible;
        dateto = element.dateto;
        datefrom = element.datefrom;
    }
  });
  return {
    PutRequest: {
      Item: {
        playlistid: convertToAttr(song.playlistid),
        songid: convertToAttr(song.songid ? song.songid : ""),
        playlistname: convertToAttr(playlistname),
        visible : convertToAttr(visible ? visible : false),
        dateto: convertToAttr(dateto ? dateto : ""),
        datefrom: convertToAttr(datefrom ? datefrom : "")

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
    const {songs, playlists} = await retrieveSongsFromS3();
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
      
      const songsResusts1 = processedSongs.map((song) => {
        const returnObj = updateDB(song,playlists, tableName)
        return returnObj
      })
      
      await batchWriteChunked(songsResusts1, tableName);

      const unprocessedSongs = songs.filter((x) => {
        return !processedSongs.includes(x)
      });

      const songsResusts = unprocessedSongs.map((song) => {
        const returnObj = updateDB(song,playlists, tableName)
        return returnObj
      })

      await batchWriteChunked(songsResusts, tableName);

    } else {

      const songsResusts = songs.map((song) => {
        const returnObj = updateDB(song,playlists, tableName)
        return returnObj
        
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
