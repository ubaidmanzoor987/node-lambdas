const { DynamoDB } = require("@aws-sdk/client-dynamodb");
const { convertToNative } = require("@aws-sdk/util-dynamodb");
const dynamoDb = new DynamoDB({ region: "us-east-1" });

const searchFunction = (searchid, items) => {
  let arr = [];
  items.forEach((item) => {
    const playlistid = convertToNative(item.playlistid);
    const songid = convertToNative(item.songid);
    if (songid && songid.length > 0) {
      songid.forEach((x) => {
        if (x === searchid) {
          arr.push(playlistid);
        }
      });
    }
  });
  return arr;
};

const searchPlaylists = (searchIds, items) => {
  let arr = [];
  searchIds.forEach((id) => {
    arr.push(searchFunction(id, items));
  });
  return arr;
};

const flattenArray = (nestedarray) => {
  const playListMap = {};
  let plylist = nestedarray.flatMap((e) => {
    return e;
  });

  plylist.forEach((x) => {
    if (x in playListMap) {
      playListMap[x] += 1;
    } else {
      playListMap[x] = 1;
    }
  });

  console.log({playListMap})
  return playListMap;
};

const sortArray = (obj) => {
  let i = 0;
  let arr = [];
  Object.keys(obj).map((item) => {
    if (obj[item] > i) {
      i = obj[item];
    }
  });
  for (let j = 0; j <= i; j++) {
    Object.keys(obj).map((item) => {
      if (obj[item] === j) {
        arr.unshift(item);
      }
    });
  }
  return arr;
};

const run = async (event) => {
  let sortedPlaylists = [];
  try {
    const searchIds = event.searchIds;
    const tableName = process.env.TABLE_NAME || "songs-data";
   
    const results = await dynamoDb.scan({
      TableName: tableName,
      ProjectionExpression: "playlistid, songid",
    });

    const { Items, Count } = results;

    if (Count && Count > 0) {
      const playlist = searchPlaylists(searchIds, Items);
      const playlistObj = flattenArray(playlist);
      sortedPlaylists = sortArray(playlistObj);
    }

    console.log({ sortedPlaylists });

  } catch (e) {
    console.log("exception in handler", e.message);
  }
  
  return sortedPlaylists;
};

exports.handler = async (event) => {
  return await run(event);
};
