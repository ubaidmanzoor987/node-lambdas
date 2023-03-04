const { DynamoDB } = require("@aws-sdk/client-dynamodb");
const { convertToAttr, convertToNative } = require("@aws-sdk/util-dynamodb");
const dynamoDb = new DynamoDB({ region: "us-east-1" });

function ltrim(str) {
  str = str.toLowerCase();
  if (!str) return str;
  return str.replace(/^\s+/g, "");
}

const retrieveDataFromDynamoDB = async (tableName, artists) => {
  const date = new Date().getDate();
  const month = new Date().getMonth() + 1;

  const ExpressionAttributeValues = {
    ":gsi1pk": convertToAttr("Song"),
    ":date1": convertToAttr(Number(`${month}${date - 1}`)),
    ":date2": convertToAttr(Number(`${month}${date + 1}`)),
  };

  const filters = artists.map((element, ind) => {
    element = ltrim(element);
    ExpressionAttributeValues[":artist" + ind.toString()] =
      convertToAttr(element);
    return `contains(artist, :artist${ind})`;
  });

  const q = {
    TableName: tableName,
    FilterExpression: filters.join(" or "),
    IndexName: "gsi1pk-Bdate-index",
    KeyConditionExpression:
      "gsi1pk = :gsi1pk and Bdate BETWEEN :date1 and :date2",
    ExpressionAttributeValues,
  };

  const results = await dynamoDb.query(q);
  return results;
};

const run = async (event) => {
  const response = {
    statusCode: 200,
    body: "success",
  };
  try {
    if (Object.keys(event).length === 0 || !event.queryStringParameters) {
      response.body = "Missing queryStringParameters";
      console.log(response);
      return response;
    }

    const queryStringParameters = event.queryStringParameters;

    if (!queryStringParameters.artists) {
      response.body = "Invalid input missing artists ";
      console.log(response);
      return response;
    }

    const artists = queryStringParameters.artists;
    if (artists.length === 0) {
      response.body = "Invalid artists ";
      console.log(response);
      return response;
    }

    const TABLE_NAME = process.env.TABLE_NAME || "importantDates";
    const arr = [];
    const { Items, Count } = await retrieveDataFromDynamoDB(
      TABLE_NAME,
      artists
    );
    Items.forEach((item) => {
      arr.push(convertToNative(item.artist));
    });
    response.body = arr;
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

// Sample Event

// const event = {
//   queryStringParameters: {
//     artists: ["One direction", "Metallica Khanam"],
//   },
// };
// run(event);
