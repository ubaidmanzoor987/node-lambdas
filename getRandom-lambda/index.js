const { DynamoDB } = require("@aws-sdk/client-dynamodb");
const { convertToAttr, convertToNative } = require("@aws-sdk/util-dynamodb");
const seedRandom = require("seedrandom");
const dynamoDb = new DynamoDB({ region: "us-east-1" });

async function query(queryParam) {
  return new Promise((res, rej) => {
    dynamoDb.query(queryParam).then((resp) => {
      res(resp)
    }).catch((err) => {
      rej(err)
    });
  })
}

const run = async (event) => {
  const response = {
    statusCode: 200,
    body: "",
  };
  try {
    const tableName = process.env.TABLE_NAME;
    const cacheTableName = process.env.CACHE_TABLE_NAME;

    var seed = new Date().toDateString();

    if (cacheTableName) {
      const item = await query({
        KeyConditions: {
          id: {
            "ComparisonOperator": "EQ",
            AttributeValueList: [{S: seed}]
          }
        },
        TableName: cacheTableName
      });

      if (item && item.Count && item.Count > 0 && item.Items) {
        response.body = JSON.stringify({
          ids: convertToNative(item.Items[0].items),
        });
        return response
      }
    }

    const params = {
      TableName: tableName,
      ProjectionExpression: "id",
    };
    const results = await dynamoDb.scan(params);
    if (results) {
      const { Items, Count } = results;

      if (Items && Count) {
        const random1 = Math.ceil(seedRandom(seed)() * Count);
        const random2 = Math.ceil(seedRandom(seed + "a")() * Count);
        const random3 = Math.ceil(seedRandom(seed + "b")() * Count);
        const resp = [ convertToNative(Items[random1].id), convertToNative(Items[random2].id), convertToNative(Items[random3].id)];
        if (cacheTableName) {
          await dynamoDb.putItem({
            TableName: cacheTableName,
            Item: {
              id: convertToAttr(seed),
              items: convertToAttr(resp)
            }
          })
        }
        response.body = JSON.stringify({
          ids: resp,
        });
      }
    }
  } catch (e) {
    response.statusCode = 400;
    response.body = JSON.stringify(e.message);
  }
  return response;
};

exports.handler = async (event) => {
  return await run(event);
};
